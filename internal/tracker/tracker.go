package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
)

type TopicTracker struct {
	kafkaClient       *kafka.Client
	stateManager      *kafka.StateManager
	workloadBalancer  *kafka.WorkloadBalancer
	scanInterval      time.Duration
	instanceID        string
	consumerGroupID   string
	heartbeatInterval time.Duration
	currentSnapshot   atomic.Pointer[models.ClusterSnapshot]
	activeInstances   atomic.Pointer[map[string]models.InstanceInfo]
	stopChan          chan struct{}
	wg                sync.WaitGroup
	scanMu            sync.Mutex
	// globalMu guards globalTopics. globalSnapshot is rebuilt atomically after
	// every merge so readers never need to hold globalMu.
	globalMu       sync.RWMutex
	globalTopics   map[string]*models.TopicStatus
	globalSnapshot atomic.Pointer[models.ClusterSnapshot]
	globalCancel   context.CancelFunc
}

type Options struct {
	InstanceID               string
	ConsumerGroupID          string
	InstanceHeartbeatSeconds int
}

type scanTopicData struct {
	name       string
	partitions []int32
	offsets    map[int32]int64
}

func NewWithOptions(kafkaClient *kafka.Client, stateManager *kafka.StateManager, workloadBalancer *kafka.WorkloadBalancer, scanIntervalMinutes int, opts Options) *TopicTracker {
	heartbeatInterval := time.Duration(opts.InstanceHeartbeatSeconds) * time.Second
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}

	tt := &TopicTracker{
		kafkaClient:       kafkaClient,
		stateManager:      stateManager,
		workloadBalancer:  workloadBalancer,
		scanInterval:      time.Duration(scanIntervalMinutes) * time.Minute,
		instanceID:        opts.InstanceID,
		consumerGroupID:   opts.ConsumerGroupID,
		heartbeatInterval: heartbeatInterval,
		stopChan:          make(chan struct{}),
		globalTopics:      make(map[string]*models.TopicStatus),
	}
	emptySnapshot := &models.ClusterSnapshot{
		Topics:          make(map[string]*models.TopicStatus),
		Timestamp:       time.Now().UTC().Unix(),
		Version:         1,
		IsGlobal:        true,
		LocalInstanceID: opts.InstanceID,
	}
	tt.currentSnapshot.Store(emptySnapshot)
	tt.globalSnapshot.Store(emptySnapshot)
	emptyInstances := make(map[string]models.InstanceInfo)
	tt.activeInstances.Store(&emptyInstances)
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
		t.syncSnapshotToModel(snapshot)
		t.syncGlobalFromState(snapshot)
		t.syncInstancesFromState(snapshot)
		if loadStats != nil && loadStats.TopicExists {
			status := "complete"
			if loadStats.TimedOut {
				status = "partial_timeout"
			}
			logging.Info("State replay stats: total_messages=%d duplicate_keys=%d discarded=%d tombstones=%d unique_keys=%d malformed=%d partitions=%d duration_ms=%d final_topics=%d final_instances=%d status=%s",
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
		if err := t.workloadBalancer.Start(ctx); err != nil {
			return err
		}
		if !t.workloadBalancer.WaitForAssignments(ctx, 10*time.Second) {
			logging.Warn("No workload assignment received within startup wait window; tracker will wait for rebalance updates")
		}
	}

	// Write an initial heartbeat before starting the periodic loop so that
	// the instance is immediately visible to peers.
	if err := t.writeLocalHeartbeat(ctx); err != nil {
		logging.Warn("Failed to write initial heartbeat: %v", err)
	}

	t.startGlobalConsumerLoop(ctx, resumeOffsets)

	t.wg.Add(2)
	go t.scanLoop(ctx)
	go t.heartbeatLoop(ctx)

	return nil
}

func (t *TopicTracker) Stop() {
	// Write a tombstone for this instance before shutting down so other
	// instances (and the next restart) immediately remove it from the
	// active-instances list.
	if t.stateManager != nil && t.instanceID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := t.stateManager.DeregisterInstance(ctx, t.instanceID); err != nil {
			logging.Warn("Failed to write instance tombstone on shutdown: %v", err)
		}
		cancel()
	}

	close(t.stopChan)
	if t.globalCancel != nil {
		t.globalCancel()
	}
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

func (t *TopicTracker) scanLoop(ctx context.Context) {
	defer t.wg.Done()

	ticker := time.NewTicker(t.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopChan:
			return
		case <-ticker.C:
			if !t.scanMu.TryLock() {
				logging.Warn("Skipping scan cycle: previous scan still in progress")
				continue
			}
			err := t.scanTopics(ctx)
			t.scanMu.Unlock()
			if err != nil {
				logging.Error("Error during scan: %v", err)
			}
		}
	}
}

// heartbeatLoop periodically writes the local instance heartbeat to the
// tracker topic on a wall-clock interval, independent of scan cycles.
func (t *TopicTracker) heartbeatLoop(ctx context.Context) {
	defer t.wg.Done()

	ticker := time.NewTicker(t.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopChan:
			return
		case <-ticker.C:
			if err := t.writeLocalHeartbeat(ctx); err != nil {
				logging.Warn("Heartbeat write failed: %v", err)
			}
		}
	}
}

func (t *TopicTracker) scanTopics(ctx context.Context) error {
	scanStartedAt := time.Now()

	// Single MetadataRequest returns all topics with their full partition lists,
	// replacing the previous pattern of 1 ListTopics + N GetTopicPartitions calls.
	allTopics, err := t.kafkaClient.ListTopicsWithPartitions(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	previousSnapshot := t.currentSnapshot.Load()

	// Build a set of all topics currently in Kafka (before ownership filtering)
	// so we can detect topics that have been deleted since the last scan.
	kafkaTopicSet := make(map[string]struct{}, len(allTopics))
	for topicName := range allTopics {
		kafkaTopicSet[topicName] = struct{}{}
	}

	scanTime := time.Now().UTC().Unix()

	snapshot := &models.ClusterSnapshot{
		Topics:    make(map[string]*models.TopicStatus),
		Timestamp: scanTime,
		Version:   1,
	}

	assignedTopics := 0
	processedTopics := 0
	processedPartitions := 0

	// Apply ownership filter: only track partitions assigned to this instance.
	ownedTopicPartitions := make(map[string][]int32, len(allTopics))
	for topicName, partitions := range allTopics {
		owned := make([]int32, 0, len(partitions))
		for _, partID := range partitions {
			if t.workloadBalancer == nil || t.workloadBalancer.OwnsTopicPartition(topicName, partID) {
				owned = append(owned, partID)
			}
		}
		if len(owned) == 0 {
			continue
		}
		assignedTopics++
		ownedTopicPartitions[topicName] = owned
	}

	// During transient rebalance/loss windows (for example after host sleep),
	// this instance can briefly own zero shards. Do not replace local snapshot
	// with an empty/partial view in that window, otherwise unchanged partitions
	// lose their previous timestamps and ages reset on the next scan.
	if len(ownedTopicPartitions) == 0 {
		logging.Warn("Skipping scan cycle: no owned topic partitions assigned (likely rebalance in progress)")
		return nil
	}

	// Single sharded ListOffsetsRequest covers all topics/partitions at once,
	// replacing N per-topic GetHighWatermarks calls.
	allOffsets, err := t.kafkaClient.GetHighWatermarksBatch(ctx, ownedTopicPartitions)
	if err != nil {
		return fmt.Errorf("failed to get high watermarks: %w", err)
	}

	topicData := make([]scanTopicData, 0, len(ownedTopicPartitions))
	for topicName, ownedPartitions := range ownedTopicPartitions {
		offsets, ok := allOffsets[topicName]
		if !ok {
			logging.Warn("No offsets returned for topic %s, skipping", topicName)
			continue
		}
		topicData = append(topicData, scanTopicData{
			name:       topicName,
			partitions: ownedPartitions,
			offsets:    offsets,
		})
	}

	for _, meta := range topicData {
		topicStatus := &models.TopicStatus{
			Name:           meta.name,
			PartitionCount: int32(len(meta.partitions)),
			Partitions:     make(map[int32]*models.PartitionInfo),
			LastUpdate:     scanTime,
		}

		var oldestTimestamp int64
		var newestTimestamp int64

		for _, partID := range meta.partitions {
			offset, ok := meta.offsets[partID]
			if !ok {
				logging.Warn("Missing offset for %s partition %d", meta.name, partID)
				continue
			}
			processedPartitions++

			previous := previousPartitionInfo(previousSnapshot, meta.name, partID)
			partInfo := buildPartitionInfo(partID, offset, previous, scanTime)

			topicStatus.Partitions[partID] = partInfo

			if oldestTimestamp == 0 || partInfo.Timestamp < oldestTimestamp {
				oldestTimestamp = partInfo.Timestamp
			}
			if newestTimestamp == 0 || partInfo.Timestamp > newestTimestamp {
				newestTimestamp = partInfo.Timestamp
			}
		}

		if oldestTimestamp > 0 {
			topicStatus.OldestPartitionAge = models.CalculateDuration(time.Unix(oldestTimestamp, 0).UTC())
		}
		if newestTimestamp > 0 {
			topicStatus.NewestPartitionAge = models.CalculateDuration(time.Unix(newestTimestamp, 0).UTC())
		}

		snapshot.Topics[meta.name] = topicStatus
		processedTopics++
	}

	t.currentSnapshot.Store(snapshot)

	if err := t.stateManager.SaveSnapshot(ctx, snapshot); err != nil {
		logging.Warn("Failed to save snapshot: %v", err)
	}

	// Emit tombstones for topics that existed in the previous snapshot but are
	// no longer present in Kafka. This keeps the compacted tracker topic clean
	// so that deleted topics do not reappear when the tracker restarts.
	for topicName := range previousSnapshot.Topics {
		if _, exists := kafkaTopicSet[topicName]; !exists {
			logging.Info("Topic %q no longer exists in Kafka, emitting tombstone", topicName)
			if err := t.stateManager.DeleteTopicState(ctx, topicName); err != nil {
				logging.Warn("Failed to emit tombstone for deleted topic %q: %v", topicName, err)
			}
		}
	}

	logging.Info("Scan cycle completed in %s: listed_topics=%d assigned_topics=%d processed_topics=%d processed_partitions=%d",
		time.Since(scanStartedAt), len(allTopics), assignedTopics, processedTopics, processedPartitions)
	logging.Debug("Scan completed. Found %d topics", len(snapshot.Topics))
	return nil
}

func (t *TopicTracker) syncSnapshotToModel(snapshot *kafka.StateSnapshot) {
	cs := &models.ClusterSnapshot{
		Topics:    make(map[string]*models.TopicStatus),
		Timestamp: snapshot.Timestamp,
		Version:   snapshot.Version,
	}
	for topicName, partitions := range snapshot.Topics {
		cs.Topics[topicName] = buildTopicStatusFromStatePartitions(topicName, snapshot.Timestamp, partitions)
	}
	t.currentSnapshot.Store(cs)
}

// syncGlobalFromState populates globalTopics from the offline state replay and
// stores the initial globalSnapshot. Called once during Start() after the state
// load completes, before the continuous consumer loop begins.
func (t *TopicTracker) syncGlobalFromState(snapshot *kafka.StateSnapshot) {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()
	for topicName, partitions := range snapshot.Topics {
		t.globalTopics[topicName] = buildTopicStatusFromStatePartitions(topicName, snapshot.Timestamp, partitions)
	}
	t.globalSnapshot.Store(t.buildGlobalSnapshotLocked())
}

// applyGlobalRecord merges one record from the continuous tracker topic
// consumer into globalTopics or activeInstances, then atomically publishes
// an updated snapshot.
func (t *TopicTracker) applyGlobalRecord(key string, value []byte) {
	if instanceID, ok := strings.CutPrefix(key, "tracker-instance:"); ok {
		t.applyHeartbeatRecord(instanceID, value)
		return
	}

	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	if value == nil {
		delete(t.globalTopics, key)
	} else {
		var state kafka.TopicState
		if err := json.Unmarshal(value, &state); err != nil {
			logging.Warn("applyGlobalRecord: failed to unmarshal topic state for key %s: %v", key, err)
			return
		}
		t.globalTopics[state.Topic] = buildTopicStatusFromStatePartitions(state.Topic, state.Timestamp, state.Partitions)
	}

	t.globalSnapshot.Store(t.buildGlobalSnapshotLocked())
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

// startGlobalConsumerLoop tails the tracker topic from resumeOffsets and
// merges every incoming record into the global snapshot via applyGlobalRecord.
// The loop runs until Stop() cancels its context.
func (t *TopicTracker) startGlobalConsumerLoop(ctx context.Context, resumeOffsets map[int32]int64) {
	loopCtx, cancel := context.WithCancel(ctx)
	t.globalCancel = cancel

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		logging.Info("Global consumer loop started (combined multi-instance view)")
		t.stateManager.SubscribeGlobalUpdates(loopCtx, resumeOffsets, t.applyGlobalRecord)
		logging.Info("Global consumer loop stopped")
	}()
}

// buildTopicStatusFromStatePartitions converts the raw persisted partition map
// into a TopicStatus with computed Age and aggregate timestamps. Used both for
// the initial state load and for incremental global consumer updates.
func buildTopicStatusFromStatePartitions(topicName string, ts int64, partitions map[int32]kafka.PartitionState) *models.TopicStatus {
	topicStatus := &models.TopicStatus{
		Name:       topicName,
		Partitions: make(map[int32]*models.PartitionInfo, len(partitions)),
		LastUpdate: ts,
	}

	var oldestTimestamp int64
	var newestTimestamp int64

	for partID, part := range partitions {
		age := models.CalculateDuration(time.Unix(part.Timestamp, 0).UTC())
		topicStatus.Partitions[partID] = &models.PartitionInfo{
			Partition: partID,
			Offset:    part.Offset,
			Timestamp: part.Timestamp,
			Age:       age,
		}
		if oldestTimestamp == 0 || part.Timestamp < oldestTimestamp {
			oldestTimestamp = part.Timestamp
		}
		if newestTimestamp == 0 || part.Timestamp > newestTimestamp {
			newestTimestamp = part.Timestamp
		}
	}

	topicStatus.PartitionCount = int32(len(partitions))
	if oldestTimestamp > 0 {
		topicStatus.OldestPartitionAge = models.CalculateDuration(time.Unix(oldestTimestamp, 0).UTC())
	}
	if newestTimestamp > 0 {
		topicStatus.NewestPartitionAge = models.CalculateDuration(time.Unix(newestTimestamp, 0).UTC())
	}
	return topicStatus
}

func (t *TopicTracker) syncInstancesFromState(snapshot *kafka.StateSnapshot) {
	now := time.Now().UTC()
	instances := make(map[string]models.InstanceInfo, len(snapshot.Instances))

	var expiredIDs []string

	for instanceID, hb := range snapshot.Instances {
		if !hb.IsActive(now) {
			expiredIDs = append(expiredIDs, instanceID)
			continue
		}

		instances[instanceID] = models.InstanceInfo{
			InstanceID:           instanceID,
			LastHeartbeatAt:      hb.LastHeartbeatAt,
			HeartbeatIntervalSec: hb.HeartbeatIntervalSec,
			ScanIntervalSec:      hb.ScanIntervalSec,
			GroupID:              hb.GroupID,
			AssignedShards:       hb.AssignedShards,
			IsActive:             true,
		}
	}

	t.activeInstances.Store(&instances)

	// Write tombstones for expired instances so they don't reappear on
	// subsequent restarts. Fire-and-forget with a short timeout; failure
	// is harmless — the instances will simply be pruned again next time.
	if len(expiredIDs) > 0 && t.stateManager != nil {
		logging.Info("Pruning %d expired instance(s) from state: %v", len(expiredIDs), expiredIDs)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range expiredIDs {
			if err := t.stateManager.DeregisterInstance(ctx, id); err != nil {
				logging.Warn("Failed to write tombstone for expired instance %s: %v", id, err)
			}
		}
	}
}

// writeLocalHeartbeat produces the local instance's HeartbeatRecord to the
// tracker topic. Other instances compute IsActive by comparing the heartbeat
// timestamp against 3× the declared heartbeat interval.
func (t *TopicTracker) writeLocalHeartbeat(ctx context.Context) error {
	if t.stateManager == nil {
		return nil
	}
	now := time.Now().UTC().Unix()
	heartbeatIntervalSec := int(t.heartbeatInterval / time.Second)

	var assignedShards int
	if t.workloadBalancer != nil {
		assignedShards = t.workloadBalancer.AssignedShardCount()
	}

	record := &kafka.HeartbeatRecord{
		Version:              1,
		InstanceID:           t.instanceID,
		LastHeartbeatAt:      now,
		HeartbeatIntervalSec: heartbeatIntervalSec,
		ScanIntervalSec:      int(t.scanInterval / time.Second),
		GroupID:              t.consumerGroupID,
		AssignedShards:       assignedShards,
	}

	if err := t.stateManager.SaveInstanceHeartbeat(ctx, record); err != nil {
		return fmt.Errorf("failed to write local heartbeat: %w", err)
	}
	return nil
}

func (t *TopicTracker) GetSnapshot() *models.ClusterSnapshot {
	return t.globalSnapshot.Load()
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

func buildPartitionInfo(partitionID int32, currentOffset int64, previous *models.PartitionInfo, now int64) *models.PartitionInfo {
	timestamp := resolvePartitionTimestamp(previous, currentOffset, now)
	age := models.CalculateDuration(time.Unix(timestamp, 0).UTC())

	return &models.PartitionInfo{
		Partition: partitionID,
		Offset:    currentOffset,
		Timestamp: timestamp,
		Age:       age,
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
