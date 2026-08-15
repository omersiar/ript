package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
)

type KafkaClient interface {
	ListTopicsWithPartitions(ctx context.Context) (map[string][]int32, error)
	GetHighWatermarksBatch(ctx context.Context, topicPartitions map[string][]int32) (map[string]map[int32]int64, error)
	GetEarliestWatermarksBatch(ctx context.Context, topicPartitions map[string][]int32) (map[string]map[int32]int64, error)
	GetTopicConfigsBatch(ctx context.Context, topicNames []string) (map[string]string, error)
}

type StateManager interface {
	EnsureTrackerTopic(ctx context.Context) error
	SaveSnapshot(ctx context.Context, snapshot *models.ClusterSnapshot) error
	SaveTopicState(ctx context.Context, topicName string, topicStatus *models.TopicStatus) error
	DeleteTopicState(ctx context.Context, topicName string) error
	SaveInstanceHeartbeat(ctx context.Context, record *kafka.HeartbeatRecord) error
	DeregisterInstance(ctx context.Context, instanceID string) error
	LoadLatestSnapshot(ctx context.Context) (*kafka.StateSnapshot, *kafka.StateLoadStats, error)
	SubscribeGlobalUpdates(ctx context.Context, startOffsets map[int32]int64, onRecord func(key string, value []byte))
}

type WorkloadBalancer interface {
	Start(ctx context.Context) error
	Stop()
	WaitForAssignments(ctx context.Context, timeout time.Duration) bool
	WaitForStableAssignments(ctx context.Context, timeout time.Duration) bool
	AssignmentEpoch() uint64
	AssignedShardCount() int
	OwnsTopic(topicName string) bool
}

type assignmentHookSetter interface {
	SetAssignmentChangeHook(hook func())
}

type TopicTracker struct {
	kafkaClient        KafkaClient
	stateManager       StateManager
	workloadBalancer   WorkloadBalancer
	scanManager        *ScanManager
	heartbeatManager   *HeartbeatManager
	consumerManager    *ConsumerManager
	scanInterval       time.Duration
	instanceID         string
	consumerGroupID    string
	heartbeatInterval  time.Duration
	configCacheTTLDays int
	activeInstances    atomic.Pointer[map[string]models.InstanceInfo]
	stopChan           chan struct{}
	wg                 sync.WaitGroup
	scanMu             sync.Mutex
	// globalMu guards globalTopics. globalSnapshot is rebuilt atomically after
	// every merge so readers never need to hold globalMu.
	globalMu        sync.RWMutex
	globalTopics    map[string]*models.TopicStatus
	globalSnapshot  atomic.Pointer[models.ClusterSnapshot]
	globalCancel    context.CancelFunc
	assignmentEpoch uint64
}

type Options struct {
	InstanceID               string
	ConsumerGroupID          string
	InstanceHeartbeatSeconds int
	TopicConfigCacheTTLDays  int
}

func NewWithOptions(kafkaClient KafkaClient, stateManager StateManager, workloadBalancer WorkloadBalancer, scanIntervalMinutes int, opts Options) *TopicTracker {
	heartbeatInterval := time.Duration(opts.InstanceHeartbeatSeconds) * time.Second
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}

	cacheTTLDays := opts.TopicConfigCacheTTLDays
	if cacheTTLDays <= 0 {
		cacheTTLDays = 30
	}

	tt := &TopicTracker{
		kafkaClient:        kafkaClient,
		stateManager:       stateManager,
		workloadBalancer:   workloadBalancer,
		scanInterval:       time.Duration(scanIntervalMinutes) * time.Minute,
		instanceID:         opts.InstanceID,
		consumerGroupID:    opts.ConsumerGroupID,
		heartbeatInterval:  heartbeatInterval,
		configCacheTTLDays: cacheTTLDays,
		stopChan:           make(chan struct{}),
		globalTopics:       make(map[string]*models.TopicStatus),
	}
	emptySnapshot := &models.ClusterSnapshot{
		Topics:          make(map[string]*models.TopicStatus),
		Timestamp:       time.Now().UTC().Unix(),
		Version:         1,
		IsGlobal:        true,
		LocalInstanceID: opts.InstanceID,
	}
	tt.globalSnapshot.Store(emptySnapshot)
	emptyInstances := make(map[string]models.InstanceInfo)
	tt.activeInstances.Store(&emptyInstances)
	tt.scanManager = NewScanManager(tt)
	tt.heartbeatManager = NewHeartbeatManager(tt)
	tt.consumerManager = NewConsumerManager(tt)
	return tt
}

func (t *TopicTracker) Start(ctx context.Context) error {
	logging.Info("Starting topic tracker with scan interval: %v", t.scanInterval)

	if err := t.stateManager.EnsureTrackerTopic(ctx); err != nil {
		logging.Warn("Could not ensure tracker topic: %v", err)
	}

	snapshot, loadStats, err := t.stateManager.LoadLatestSnapshot(ctx)
	if err == nil && snapshot != nil {
		logging.Info("Loaded previous snapshot from %v", time.Unix(snapshot.Timestamp, 0).UTC())
		t.syncGlobalFromState(snapshot)
		t.heartbeatManager.syncInstancesFromState(snapshot)
		if loadStats != nil && loadStats.TopicExists {
			status := "complete"
			if loadStats.TimedOut {
				status = "partial_timeout"
			}
			logging.Info("State replay stats: total_messages=%d duplicate_keys=%d discarded=%d tombstones=%d unique_keys=%d malformed=%d partitions_with_data=%d duration_ms=%d final_topics=%d final_instances=%d status=%s",
				loadStats.TotalRecords,
				loadStats.DuplicateKeyRecords,
				loadStats.DiscardedRecords,
				loadStats.TombstoneRecords,
				loadStats.UniqueKeysSeen,
				loadStats.MalformedRecords,
				loadStats.PartitionsScanned,
				loadStats.LoadDuration.Milliseconds(),
				loadStats.FinalTopicCount,
				loadStats.FinalInstanceCount,
				status,
			)
		}
	} else {
		logging.Info("No previous snapshot found, starting fresh")
	}

	// Resume offsets ensure the global consumer starts exactly where the
	// offline state replay ended — no gap and no double-read.
	var resumeOffsets map[int32]int64
	if loadStats != nil {
		resumeOffsets = loadStats.ResumeOffsets
	}

	if t.workloadBalancer != nil {
		if hookable, ok := t.workloadBalancer.(assignmentHookSetter); ok {
			hookable.SetAssignmentChangeHook(t.heartbeatManager.signalAssignmentChanged)
		}
		if err := t.workloadBalancer.Start(ctx); err != nil {
			return err
		}
		if !t.workloadBalancer.WaitForAssignments(ctx, 10*time.Second) {
			logging.Warn("No workload assignment received within startup wait window; tracker will wait for rebalance updates")
		}
		t.assignmentEpoch = t.workloadBalancer.AssignmentEpoch()
	}

	// Write an initial heartbeat before starting the periodic loop so that
	// the instance is immediately visible to peers.
	if err := t.heartbeatManager.writeLocalHeartbeat(ctx); err != nil {
		logging.Warn("Failed to write initial heartbeat: %v", err)
	}

	t.consumerManager.startGlobalConsumerLoop(ctx, resumeOffsets)
	t.scanManager.startLoop(ctx)
	t.heartbeatManager.startLoop(ctx)

	return nil
}

func (t *TopicTracker) Stop() {
	t.heartbeatManager.deregisterOnShutdown()

	close(t.stopChan)
	t.consumerManager.stopGlobalConsumerLoop()
	// Stop the workload balancer (sends LeaveGroup) before waiting for in-flight
	// scans to complete. Heartbeating in franz-go is driven by PollFetches; once
	// the poll loop exits its heartbeats stop. If we waited for t.wg.Wait() first
	// (which blocks for any active scan), the broker would reach the session
	// timeout (~30s) and evict the member via heartbeat expiration instead of a
	// clean LeaveGroup.
	if t.workloadBalancer != nil {
		t.workloadBalancer.Stop()
	}
	t.wg.Wait()
	logging.Info("Topic tracker stopped")
}

// syncGlobalFromState populates globalTopics from the offline state replay and
// stores the initial globalSnapshot. Called once during Start() after the state
// load completes, before the continuous consumer loop begins.
func (t *TopicTracker) syncGlobalFromState(snapshot *kafka.StateSnapshot) {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()
	t.globalTopics = make(map[string]*models.TopicStatus, len(snapshot.Topics))
	for topicName, topicState := range snapshot.Topics {
		if topicState != nil {
			t.globalTopics[topicName] = buildTopicStatusFromState(topicState)
		}
	}
	t.globalSnapshot.Store(t.buildGlobalSnapshotLocked())
}

// applyGlobalRecord merges one record from the continuous tracker topic
// consumer into globalTopics or activeInstances, then atomically publishes
// an updated snapshot.
func (t *TopicTracker) applyGlobalRecord(key string, value []byte) {
	t.consumerManager.applyGlobalRecord(key, value)
}

// buildGlobalSnapshotLocked constructs a read-only ClusterSnapshot from the
// current globalTopics map. Must be called with globalMu held.
func (t *TopicTracker) buildGlobalSnapshotLocked() *models.ClusterSnapshot {
	topics := make(map[string]*models.TopicStatus, len(t.globalTopics))
	for k, v := range t.globalTopics {
		topics[k] = v
	}
	return &models.ClusterSnapshot{
		Topics:          topics,
		Timestamp:       time.Now().UTC().Unix(),
		Version:         1,
		IsGlobal:        true,
		LocalInstanceID: t.instanceID,
	}
}

// applyHeartbeatRecord updates activeInstances with a single instance heartbeat
// record received from the tracker topic. A nil value tombstones the instance.
// Uses copy-on-write so readers on activeInstances never see a torn update.
func (t *TopicTracker) applyHeartbeatRecord(instanceID string, value []byte) {
	prev := t.activeInstances.Load()
	next := make(map[string]models.InstanceInfo, len(*prev))
	for k, v := range *prev {
		next[k] = v
	}

	if value == nil {
		delete(next, instanceID)
	} else {
		var hb kafka.HeartbeatRecord
		if err := json.Unmarshal(value, &hb); err != nil {
			logging.Warn("applyHeartbeatRecord: failed to unmarshal heartbeat for instance %s: %v", instanceID, err)
			return
		}
		now := time.Now().UTC()
		next[instanceID] = models.InstanceInfo{
			InstanceID:           hb.InstanceID,
			LastHeartbeatAt:      hb.LastHeartbeatAt,
			HeartbeatIntervalSec: hb.HeartbeatIntervalSec,
			ScanIntervalSec:      hb.ScanIntervalSec,
			GroupID:              hb.GroupID,
			AssignedShards:       hb.AssignedShards,
			IsActive:             hb.IsActive(now),
		}
	}

	t.activeInstances.Store(&next)
}

// buildTopicStatusFromState converts a TopicState into a TopicStatus with
// computed Age and aggregate timestamps, preserving the Ignored flag.
// Used both for the initial state load and for incremental global consumer updates.
func buildTopicStatusFromState(state *kafka.TopicState) *models.TopicStatus {
	topicStatus := &models.TopicStatus{
		Name:          state.Topic,
		Partitions:    make(map[int32]*models.PartitionInfo, len(state.Partitions)),
		LastUpdate:    state.Timestamp,
		DiscoveryTime: state.DiscoveryTime,
		Ignored:       state.Ignored,
		IgnoredAt:     state.IgnoredAt,
	}

	if state.RetentionPolicy != nil {
		topicStatus.RetentionPolicy = &models.RetentionPolicy{
			CleanupPolicy: state.RetentionPolicy.CleanupPolicy,
			FetchedAt:     state.RetentionPolicy.FetchedAt,
		}
	}

	var oldestTimestamp int64
	var newestTimestamp int64

	for partID, part := range state.Partitions {
		age := models.CalculateDuration(time.Unix(part.Timestamp, 0).UTC())
		topicStatus.Partitions[partID] = &models.PartitionInfo{
			Partition:    partID,
			Offset:       part.Offset,
			Timestamp:    part.Timestamp,
			Age:          age,
			IsEmpty:      part.IsEmpty,
			ScannedAt:    part.ScannedAt,
			MessageCount: part.MessageCount,
		}
		if oldestTimestamp == 0 || part.Timestamp < oldestTimestamp {
			oldestTimestamp = part.Timestamp
		}
		if newestTimestamp == 0 || part.Timestamp > newestTimestamp {
			newestTimestamp = part.Timestamp
		}
		if !topicStatus.RetentionPolicy.IsCompacted() {
			topicStatus.TotalMessageCount += part.MessageCount
		}
	}

	topicStatus.PartitionCount = int32(len(state.Partitions))
	if oldestTimestamp > 0 {
		topicStatus.OldestPartitionAge = models.CalculateDuration(time.Unix(oldestTimestamp, 0).UTC())
	}
	if newestTimestamp > 0 {
		topicStatus.NewestPartitionAge = models.CalculateDuration(time.Unix(newestTimestamp, 0).UTC())
	}

	// For compacted topics message counts are meaningless; use -1 so the
	// presentation layer can render N/A.
	if topicStatus.RetentionPolicy.IsCompacted() {
		topicStatus.TotalMessageCount = -1
		for _, p := range topicStatus.Partitions {
			p.MessageCount = -1
		}
	}

	topicStatus.IsEmpty = len(topicStatus.Partitions) > 0
	for _, p := range topicStatus.Partitions {
		if !p.IsEmpty {
			topicStatus.IsEmpty = false
			break
		}
	}

	return topicStatus
}

// mergeGlobalTopicRecord returns a TopicStatus that contains all partitions from
// incoming, plus any partitions from existing that are absent in the incoming
// record. The incoming record takes precedence for every partition it covers.
// Aggregate fields (PartitionCount, oldest/newest ages) are recomputed from the
// full merged set so the result is always self-consistent. The Ignored flag from
// incoming takes precedence (ensures cluster-wide ignore state is applied).
//
// This prevents a write from one instance — which only covers its owned
// partitions — from inadvertently discarding state for partitions held by other
// instances that was already present in the global snapshot.
func mergeGlobalTopicRecord(existing, incoming *models.TopicStatus) *models.TopicStatus {
	if existing == nil {
		return incoming
	}

	merged := &models.TopicStatus{
		Name:            incoming.Name,
		LastUpdate:      incoming.LastUpdate,
		DiscoveryTime:   mergeTopicDiscoveryTime(existing.DiscoveryTime, incoming.DiscoveryTime),
		Ignored:         incoming.Ignored,
		IgnoredAt:       incoming.IgnoredAt,
		Partitions:      make(map[int32]*models.PartitionInfo, len(incoming.Partitions)+len(existing.Partitions)),
		RetentionPolicy: mergeRetentionPolicy(existing.RetentionPolicy, incoming.RetentionPolicy),
	}

	for partID, incomingPart := range incoming.Partitions {
		existingPart, exists := existing.Partitions[partID]
		// Keep whichever partition was more recently scanned. A higher ScannedAt
		// means a fresher observation from the owning instance. This prevents a
		// concurrent scan from overwriting a valid IsEmpty (or offset) with a
		// stale carried-over value: the instance that actually owns a partition
		// always writes it with ScannedAt=now, which beats any carried-over copy
		// that retains the previous scan's ScannedAt.
		// When ScannedAt values are equal (e.g. both zero in old state records),
		// incoming takes precedence to preserve the existing write-ordering behaviour.
		if !exists || incomingPart.ScannedAt >= existingPart.ScannedAt {
			merged.Partitions[partID] = incomingPart
		} else {
			merged.Partitions[partID] = existingPart
		}
	}
	for partID, existingPart := range existing.Partitions {
		if _, present := merged.Partitions[partID]; !present {
			merged.Partitions[partID] = existingPart
		}
	}

	merged.PartitionCount = int32(len(merged.Partitions))
	var oldest, newest int64
	for _, part := range merged.Partitions {
		if oldest == 0 || part.Timestamp < oldest {
			oldest = part.Timestamp
		}
		if newest == 0 || part.Timestamp > newest {
			newest = part.Timestamp
		}
	}

	// Recompute total message count. For compacted topics use -1 (N/A).
	if merged.RetentionPolicy.IsCompacted() {
		merged.TotalMessageCount = -1
		for _, p := range merged.Partitions {
			p.MessageCount = -1
		}
	} else {
		var totalMessages int64
		for _, part := range merged.Partitions {
			totalMessages += part.MessageCount
		}
		merged.TotalMessageCount = totalMessages
	}

	if oldest > 0 {
		merged.OldestPartitionAge = models.CalculateDuration(time.Unix(oldest, 0).UTC())
	}
	if newest > 0 {
		merged.NewestPartitionAge = models.CalculateDuration(time.Unix(newest, 0).UTC())
	}

	merged.IsEmpty = len(merged.Partitions) > 0
	for _, p := range merged.Partitions {
		if !p.IsEmpty {
			merged.IsEmpty = false
			break
		}
	}

	return merged
}

func mergeTopicDiscoveryTime(existing, incoming int64) int64 {
	switch {
	case existing == 0:
		return incoming
	case incoming == 0:
		return existing
	case incoming < existing:
		return incoming
	default:
		return existing
	}
}

// mergeRetentionPolicy returns the more recently fetched policy. When only one
// side is non-nil it wins. When both are non-nil the one with the higher
// FetchedAt wins (fresher data). Returns nil only if both are nil.
func mergeRetentionPolicy(existing, incoming *models.RetentionPolicy) *models.RetentionPolicy {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}
	if incoming.FetchedAt >= existing.FetchedAt {
		return incoming
	}
	return existing
}

func (t *TopicTracker) syncInstancesFromState(snapshot *kafka.StateSnapshot) {
	t.heartbeatManager.syncInstancesFromState(snapshot)
}

func (t *TopicTracker) GetSnapshot() *models.ClusterSnapshot {
	return t.globalSnapshot.Load()
}

// BuildTopicStatusesFromSnapshot converts a raw StateSnapshot into TopicStatus
// values with computed ages, preserving the Ignored flag. Reuses the same
// conversion performed during daemon startup. Intended for CLI commands that load
// state without starting the daemon.
func BuildTopicStatusesFromSnapshot(snapshot *kafka.StateSnapshot) map[string]*models.TopicStatus {
	result := make(map[string]*models.TopicStatus, len(snapshot.Topics))
	for topicName, topicState := range snapshot.Topics {
		if topicState != nil {
			result[topicName] = buildTopicStatusFromState(topicState)
		}
	}
	return result
}

// RunOnceScan performs exactly one scan cycle without starting daemon loops.
// It ensures the tracker topic exists, replays previous state for timestamp
// continuity, fetches current metadata and offsets for all topics, persists the
// updated snapshot to the tracker topic, and emits tombstones for deleted topics.
// Safe to call on a tracker that has not been Start()ed.
func (t *TopicTracker) RunOnceScan(ctx context.Context) error {
	return t.scanManager.runOnceScan(ctx)
}

func previousPartitionInfo(snapshot *models.ClusterSnapshot, topicName string, partID int32) *models.PartitionInfo {
	if snapshot == nil {
		return nil
	}

	previousTopic, exists := snapshot.Topics[topicName]
	if !exists {
		return nil
	}

	return previousTopic.Partitions[partID]
}

func buildPartitionInfo(partitionID int32, currentOffset int64, earliestOffset int64, previous *models.PartitionInfo, now int64) *models.PartitionInfo {
	timestamp := resolvePartitionTimestamp(previous, currentOffset, now)
	age := models.CalculateDuration(time.Unix(timestamp, 0).UTC())

	messageCount := currentOffset - earliestOffset
	if messageCount < 0 {
		messageCount = 0
	}

	return &models.PartitionInfo{
		Partition:    partitionID,
		Offset:       currentOffset,
		Timestamp:    timestamp,
		Age:          age,
		IsEmpty:      earliestOffset == currentOffset,
		ScannedAt:    now,
		MessageCount: messageCount,
	}
}

func resolvePartitionTimestamp(previous *models.PartitionInfo, currentOffset int64, now int64) int64 {
	if previous == nil || previous.Timestamp == 0 {
		return now
	}
	// Reset timestamp only on forward offset movement (new data). If offset is
	// unchanged or moves backwards (transient metadata/leader effects), preserve
	// the previous timestamp to keep age monotonic.
	if currentOffset > previous.Offset {
		return now
	}
	return previous.Timestamp
}

func (t *TopicTracker) GetTopic(name string) *models.TopicStatus {
	snapshot := t.globalSnapshot.Load()
	if topic, exists := snapshot.Topics[name]; exists {
		return topic
	}
	return nil
}

// UpdateTopicIgnored updates the ignored flag for a topic and persists the
// change to the tracker topic so all instances see the updated state.
func (t *TopicTracker) UpdateTopicIgnored(ctx context.Context, topicName string, ignored bool) error {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	topic, exists := t.globalTopics[topicName]
	if !exists {
		return fmt.Errorf("topic not found: %s", topicName)
	}

	// Update the ignored flag
	topic.Ignored = ignored
	if ignored {
		now := time.Now().UTC().Unix()
		topic.IgnoredAt = &now
	} else {
		topic.IgnoredAt = nil
	}

	// Persist the updated state to the tracker topic
	if err := t.stateManager.SaveTopicState(ctx, topicName, topic); err != nil {
		// Revert the change if save failed
		topic.Ignored = !ignored
		if !ignored {
			topic.IgnoredAt = nil
		}
		return fmt.Errorf("failed to persist ignore state: %w", err)
	}

	// Rebuild and publish the updated snapshot
	t.globalSnapshot.Store(t.buildGlobalSnapshotLocked())

	return nil
}

func (t *TopicTracker) GetUnusedTopics(unusedDays int) []*models.TopicStatus {
	snapshot := t.globalSnapshot.Load()

	var unused []*models.TopicStatus
	for _, topic := range snapshot.Topics {
		if topic.NewestPartitionAge.Days >= unusedDays {
			unused = append(unused, topic)
		}
	}
	return unused
}

func (t *TopicTracker) GetEmptyTopics() []*models.TopicStatus {
	snapshot := t.globalSnapshot.Load()

	var empty []*models.TopicStatus
	for _, topic := range snapshot.Topics {
		if topic.IsEmpty {
			empty = append(empty, topic)
		}
	}
	return empty
}

func (t *TopicTracker) GetInstances() []models.InstanceInfo {
	instancesPtr := t.activeInstances.Load()
	now := time.Now().UTC()
	instances := make([]models.InstanceInfo, 0, len(*instancesPtr))
	for _, instance := range *instancesPtr {
		interval := time.Duration(instance.HeartbeatIntervalSec) * time.Second
		if interval <= 0 {
			interval = 30 * time.Second
		}
		lastHeartbeatAt := time.Unix(instance.LastHeartbeatAt, 0).UTC()
		instance.IsActive = !now.After(lastHeartbeatAt.Add(3 * interval))
		instances = append(instances, instance)
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].InstanceID < instances[j].InstanceID
	})
	return instances
}
