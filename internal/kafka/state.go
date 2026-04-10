package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
	"github.com/twmb/franz-go/pkg/kgo"
)

const ownershipRecordPrefix = "tracker-instance:"

// HeartbeatRecord is the persisted heartbeat record for a tracker instance.
type HeartbeatRecord struct {
	Version              int    `json:"version"`
	InstanceID           string `json:"instance_id"`
	LastHeartbeatAt      int64  `json:"last_heartbeat_at"`
	HeartbeatIntervalSec int    `json:"heartbeat_interval_sec"`
	ScanIntervalSec      int    `json:"scan_interval_sec,omitempty"`
	GroupID              string `json:"group_id,omitempty"`
	AssignedShards       int    `json:"assigned_shards,omitempty"`
}

// IsActive reports whether the instance should be considered alive based on
// wall-clock comparison: active if the last heartbeat is within 3× the
// instance's declared heartbeat interval.
func (r *HeartbeatRecord) IsActive(now time.Time) bool {
	if r == nil || r.LastHeartbeatAt <= 0 {
		return false
	}
	interval := time.Duration(r.HeartbeatIntervalSec) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	lastHeartbeatAt := time.Unix(r.LastHeartbeatAt, 0).UTC()
	return !now.After(lastHeartbeatAt.Add(3 * interval))
}

func parseInstanceIDFromHeartbeatKey(rawKey []byte) (string, bool) {
	key := string(rawKey)
	id, ok := strings.CutPrefix(key, ownershipRecordPrefix)
	if !ok {
		return "", false
	}
	if strings.TrimSpace(id) == "" {
		return "", false
	}
	return id, true
}

func cleanupPolicyHasCompact(value string) bool {
	for _, token := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(token), "compact") {
			return true
		}
	}
	return false
}

func validateTrackerTopicConfigs(actual map[string]string, expected map[string]string) (warnings []string, err error) {
	cleanupPolicy, ok := actual["cleanup.policy"]
	if !ok || !cleanupPolicyHasCompact(cleanupPolicy) {
		if !ok {
			return nil, fmt.Errorf("tracker topic is missing required config cleanup.policy=compact")
		}
		return nil, fmt.Errorf("tracker topic must be compacted; cleanup.policy is %q", cleanupPolicy)
	}

	warnings = make([]string, 0)
	for key, want := range expected {
		if key == "cleanup.policy" {
			continue
		}
		got, exists := actual[key]
		if !exists {
			warnings = append(warnings, fmt.Sprintf("tracker topic config %s is not set (recommended: %s)", key, want))
			continue
		}
		if got != want {
			warnings = append(warnings, fmt.Sprintf("tracker topic config %s=%s (recommended: %s)", key, got, want))
		}
	}

	return warnings, nil
}

// StateSnapshot is the raw loaded state from the tracker topic, keyed by topic
// name then partition ID. It is a lightweight intermediate type used to
// bootstrap the in-memory ClusterSnapshot on startup.
type StateSnapshot struct {
	Timestamp int64
	Version   int
	Topics    map[string]map[int32]PartitionState
	Instances map[string]HeartbeatRecord
}

// TopicState is the persisted representation of a single topic. Each instance
// is written as one Kafka message (key = topic name) to the compacted tracker
// topic, allowing Kafka log compaction to retain only the latest state per topic.
type TopicState struct {
	Version    int                      `json:"version"`
	Topic      string                   `json:"topic"`
	Timestamp  int64                    `json:"timestamp"`
	Partitions map[int32]PartitionState `json:"partitions"`
}

// PartitionState is the persisted offset and timestamp for a single partition.
type PartitionState struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
	Timestamp int64 `json:"timestamp"`
}

// StateLoadStats summarizes replay details while rebuilding state from the
// tracker topic on startup.
type StateLoadStats struct {
	TopicExists         bool
	Completed           bool
	TimedOut            bool
	LoadDuration        time.Duration
	PartitionsScanned   int
	TotalRecords        int64
	DuplicateKeyRecords int64
	TombstoneRecords    int64
	TopicStateRecords   int64
	HeartbeatRecords    int64
	MalformedRecords    int64
	UniqueKeysSeen      int64
	DiscardedRecords    int64
	FinalTopicCount     int64
	FinalInstanceCount  int64
	// ResumeOffsets holds the per-partition HWM from this load pass. Pass these
	// to SubscribeGlobalUpdates to start the continuous consumer exactly where
	// the offline replay left off — no gap, no double-read.
	ResumeOffsets map[int32]int64
}

type StateManager struct {
	client       *Client
	trackerTopic string
	partitions   int32
	replFactor   int16
	segmentMS    int
	minCleanable float64
	loadTimeout  time.Duration
	authOpts     []kgo.Opt
}

func NewStateManager(client *Client, trackerTopic string, partitions int32, replFactor int16, segmentMS int, minCleanable float64, loadTimeout time.Duration) *StateManager {
	return &StateManager{
		client:       client,
		trackerTopic: trackerTopic,
		partitions:   partitions,
		replFactor:   replFactor,
		segmentMS:    segmentMS,
		minCleanable: minCleanable,
		loadTimeout:  loadTimeout,
		authOpts:     slices.Clone(client.config.AuthOpts),
	}
}

// EnsureTrackerTopic creates the compacted state topic if it does not exist.
// Topic configs are tuned for a high-churn compacted log: aggressive compaction
// ratios, no time/size-based retention, and tombstone retention of 24 hours.
func (sm *StateManager) EnsureTrackerTopic(ctx context.Context) error {
	compactionConfigs := map[string]string{
		"cleanup.policy":            "compact",
		"segment.ms":                strconv.Itoa(sm.segmentMS),
		"min.cleanable.dirty.ratio": strconv.FormatFloat(sm.minCleanable, 'f', -1, 64),
	}

	err := sm.client.CreateTopicsIfNotExist(ctx, []string{sm.trackerTopic}, sm.partitions, sm.replFactor, compactionConfigs)
	if err != nil {
		logging.Warn("Could not auto-create tracker topic (may require manual creation or higher permissions): %v", err)
		logging.Warn("Tracker will continue and attempt to write state if topic already exists")
	}

	// Validate the topic actually exists and is usable, with retries.
	deadline := time.Now().Add(30 * time.Second)
	pollInterval := 500 * time.Millisecond
	for {
		// Check for cancellation before issuing broker calls so SIGTERM during
		// startup unblocks immediately without waiting for a network round-trip.
		if err := ctx.Err(); err != nil {
			return err
		}

		partitions, partErr := sm.client.GetTopicPartitions(ctx, sm.trackerTopic)
		if partErr == nil && len(partitions) > 0 {
			topicConfigs, cfgErr := sm.client.GetTopicConfigs(ctx, sm.trackerTopic, []string{
				"cleanup.policy",
				"segment.ms",
				"min.cleanable.dirty.ratio",
			})
			if cfgErr != nil {
				return fmt.Errorf("failed to verify tracker topic configs for %s: %w", sm.trackerTopic, cfgErr)
			}

			warnings, validationErr := validateTrackerTopicConfigs(topicConfigs, compactionConfigs)
			if validationErr != nil {
				return fmt.Errorf("tracker topic %s has invalid required config: %w", sm.trackerTopic, validationErr)
			}
			for _, warning := range warnings {
				logging.Warn("Tracker topic config warning for %s: %s", sm.trackerTopic, warning)
			}

			logging.Info("Tracker topic verified: %s (partitions=%d, replication=%d)", sm.trackerTopic, len(partitions), sm.replFactor)
			return nil
		}

		if time.Now().After(deadline) {
			if partErr != nil {
				return fmt.Errorf("tracker topic %s not available after creation: %w", sm.trackerTopic, partErr)
			}
			return fmt.Errorf("tracker topic %s has no partitions after creation", sm.trackerTopic)
		}

		logging.Info("Waiting for tracker topic %s to become available...", sm.trackerTopic)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			if pollInterval < 3*time.Second {
				pollInterval = pollInterval * 2
			}
		}
	}
}

// SaveSnapshot persists the full cluster snapshot in a single batch produce
// request — one message per topic using the topic name as the Kafka key.
// Batching all records in one ProduceSync call is significantly more efficient
// than per-topic calls for large clusters.
func (sm *StateManager) SaveSnapshot(ctx context.Context, snapshot *models.ClusterSnapshot) error {
	records := make([]*kgo.Record, 0, len(snapshot.Topics))

	for topicName, topicStatus := range snapshot.Topics {
		state := &TopicState{
			Version:    1,
			Topic:      topicName,
			Timestamp:  time.Now().UTC().Unix(),
			Partitions: make(map[int32]PartitionState, len(topicStatus.Partitions)),
		}
		for partID, partInfo := range topicStatus.Partitions {
			state.Partitions[partID] = PartitionState{
				Partition: partID,
				Offset:    partInfo.Offset,
				Timestamp: partInfo.Timestamp,
			}
		}

		data, err := json.Marshal(state)
		if err != nil {
			logging.Warn("Failed to marshal state for topic %s: %v", topicName, err)
			continue
		}
		records = append(records, &kgo.Record{
			Topic: sm.trackerTopic,
			Key:   []byte(topicName),
			Value: data,
		})
	}

	if len(records) == 0 {
		return nil
	}

	results := sm.client.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce snapshot batch: %w", err)
	}

	logging.Debug("Saved snapshot: %d topics in a single batch", len(records))
	return nil
}

// SaveTopicState persists the state for a single topic. Use SaveSnapshot for
// bulk updates; this is available for incremental single-topic saves.
func (sm *StateManager) SaveTopicState(ctx context.Context, topicName string, topicStatus *models.TopicStatus) error {
	state := &TopicState{
		Version:    1,
		Topic:      topicName,
		Timestamp:  time.Now().UTC().Unix(),
		Partitions: make(map[int32]PartitionState, len(topicStatus.Partitions)),
	}
	for partID, partInfo := range topicStatus.Partitions {
		state.Partitions[partID] = PartitionState{
			Partition: partID,
			Offset:    partInfo.Offset,
			Timestamp: partInfo.Timestamp,
		}
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal topic state: %w", err)
	}

	results := sm.client.client.ProduceSync(ctx, &kgo.Record{
		Topic: sm.trackerTopic,
		Key:   []byte(topicName),
		Value: data,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce topic state: %w", err)
	}
	return nil
}

// DeleteTopicState emits a tombstone (nil value) for the given topic key to the
// compacted tracker topic. On the next LoadLatestSnapshot replay the tombstone
// causes the topic to be removed from the restored snapshot, so deleted Kafka
// topics do not resurface after a restart.
func (sm *StateManager) DeleteTopicState(ctx context.Context, topicName string) error {
	results := sm.client.client.ProduceSync(ctx, &kgo.Record{
		Topic: sm.trackerTopic,
		Key:   []byte(topicName),
		Value: nil,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce tombstone for topic %s: %w", topicName, err)
	}
	return nil
}

// SaveInstanceHeartbeat writes the local instance's HeartbeatRecord to the
// tracker topic using the standard tracker-instance:<instanceID> key format.
// The global consumer on every instance (including this one) will pick it up
// and update activeInstances accordingly.
func (sm *StateManager) SaveInstanceHeartbeat(ctx context.Context, record *HeartbeatRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal instance heartbeat: %w", err)
	}
	key := ownershipRecordPrefix + record.InstanceID
	results := sm.client.client.ProduceSync(ctx, &kgo.Record{
		Topic: sm.trackerTopic,
		Key:   []byte(key),
		Value: data,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce instance heartbeat: %w", err)
	}
	logging.Debug("Instance heartbeat written: instance_id=%s interval=%ds", record.InstanceID, record.HeartbeatIntervalSec)
	return nil
}

// DeregisterInstance emits a tombstone (nil value) for the given instance ID to
// the compacted tracker topic so the instance is removed on next state load.
func (sm *StateManager) DeregisterInstance(ctx context.Context, instanceID string) error {
	key := ownershipRecordPrefix + instanceID
	results := sm.client.client.ProduceSync(ctx, &kgo.Record{
		Topic: sm.trackerTopic,
		Key:   []byte(key),
		Value: nil,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce tombstone for instance %s: %w", instanceID, err)
	}
	logging.Info("Instance tombstone written: instance_id=%s", instanceID)
	return nil
}

// LoadLatestSnapshot reads the compacted tracker topic from the beginning on
// all partitions using a short-lived dedicated consumer client, then stops as
// soon as the high-watermark for every partition has been reached. A dedicated
// client avoids polluting the shared kafka.Client's consumer assignment.
//
// For large clusters the topic may have many records; the load timeout
// (default 30 s, configurable via STATE_LOAD_TIMEOUT_SECONDS) is a safety net.
func (sm *StateManager) LoadLatestSnapshot(ctx context.Context) (*StateSnapshot, *StateLoadStats, error) {
	start := time.Now()
	stats := &StateLoadStats{}

	partitions, err := sm.client.GetTopicPartitions(ctx, sm.trackerTopic)
	if err != nil || len(partitions) == 0 {
		logging.Info("Tracker topic not found or empty — starting fresh")
		stats.LoadDuration = time.Since(start)
		return nil, stats, nil
	}
	stats.TopicExists = true

	// Fetch current high-watermarks in one request so we know when to stop.
	hwMap, err := sm.client.GetHighWatermarks(ctx, sm.trackerTopic, partitions)
	if err != nil {
		stats.LoadDuration = time.Since(start)
		return nil, stats, fmt.Errorf("failed to get high watermarks for tracker topic: %w", err)
	}

	// Capture HWMs as resume offsets before any early-return path so the
	// caller can hand them to SubscribeGlobalUpdates without re-reading history.
	stats.ResumeOffsets = hwMap

	// Only consume partitions that actually have data.
	activePartitions := make([]int32, 0, len(partitions))
	for _, p := range partitions {
		if hwMap[p] > 0 {
			activePartitions = append(activePartitions, p)
		}
	}
	stats.PartitionsScanned = len(activePartitions)

	snapshot := &StateSnapshot{
		Timestamp: time.Now().UTC().Unix(),
		Version:   1,
		Topics:    make(map[string]map[int32]PartitionState),
		Instances: make(map[string]HeartbeatRecord),
	}

	if len(activePartitions) == 0 {
		logging.Info("Tracker topic is empty — starting fresh")
		stats.Completed = true
		stats.LoadDuration = time.Since(start)
		return snapshot, stats, nil
	}

	// Build consume assignments: read every active partition from the start.
	consumeMap := map[string]map[int32]kgo.Offset{
		sm.trackerTopic: {},
	}
	for _, p := range activePartitions {
		consumeMap[sm.trackerTopic][p] = kgo.NewOffset().AtStart()
	}

	// Dedicated consumer so we never mutate the shared client's consumer state.
	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(sm.client.config.Brokers...),
		kgo.ClientID(sm.client.NextClientID("state-load")),
		kgo.ConsumePartitions(consumeMap),
		kgo.FetchMaxBytes(10 * 1024 * 1024),      // 10 MB per fetch for throughput
		kgo.FetchMaxWait(500 * time.Millisecond), // don't stall long on sparse partitions
	}
	consumerOpts = append(consumerOpts, sm.authOpts...)
	consumerClient, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		stats.LoadDuration = time.Since(start)
		return nil, stats, fmt.Errorf("failed to create consumer client for state load: %w", err)
	}
	defer consumerClient.Close()

	// Track the highest record offset seen per active partition.
	lastSeen := make(map[int32]int64, len(activePartitions))
	for _, p := range activePartitions {
		lastSeen[p] = -1
	}

	pollCtx, cancel := context.WithTimeout(ctx, sm.loadTimeout)
	defer cancel()
	seenKeys := make(map[string]struct{})

	for {
		fetches := consumerClient.PollFetches(pollCtx)
		if fetches.IsClientClosed() {
			break
		}

		for _, fe := range fetches.Errors() {
			if fe.Err != context.DeadlineExceeded && fe.Err != context.Canceled {
				logging.Warn("Error reading state partition %d: %v", fe.Partition, fe.Err)
			}
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			p.EachRecord(func(record *kgo.Record) {
				applyRecordToSnapshot(snapshot, stats, seenKeys, record)

				if record.Offset > lastSeen[record.Partition] {
					lastSeen[record.Partition] = record.Offset
				}
			})
		})

		// Stop as soon as every active partition is fully consumed.
		// The last record in a Kafka partition always has offset == HWM-1,
		// so lastSeen[p]+1 == hwMap[p] is a precise EOF condition.
		allDone := true
		for _, p := range activePartitions {
			if lastSeen[p]+1 < hwMap[p] {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}

		select {
		case <-pollCtx.Done():
			finalizeStateLoadStats(snapshot, stats, seenKeys, start)
			stats.Completed = false
			stats.TimedOut = true
			logging.Warn("State load timed out after %v — loaded %d topics so far (increase STATE_LOAD_TIMEOUT_SECONDS for very large clusters)", sm.loadTimeout, len(snapshot.Topics))
			return snapshot, stats, nil
		default:
		}
	}

	finalizeStateLoadStats(snapshot, stats, seenKeys, start)
	stats.Completed = true

	logging.Info("Loaded state: %d topics from %d partition(s)", len(snapshot.Topics), len(activePartitions))
	return snapshot, stats, nil
}

func applyRecordToSnapshot(snapshot *StateSnapshot, stats *StateLoadStats, seenKeys map[string]struct{}, record *kgo.Record) {
	stats.TotalRecords++

	key := string(record.Key)
	if _, ok := seenKeys[key]; ok {
		stats.DuplicateKeyRecords++
	} else {
		seenKeys[key] = struct{}{}
	}

	if record.Value == nil {
		stats.TombstoneRecords++
	}

	if instanceID, isInstanceHeartbeat := parseInstanceIDFromHeartbeatKey(record.Key); isInstanceHeartbeat {
		stats.HeartbeatRecords++
		if record.Value == nil {
			delete(snapshot.Instances, instanceID)
			return
		}

		var hb HeartbeatRecord
		if err := json.Unmarshal(record.Value, &hb); err != nil {
			stats.MalformedRecords++
			logging.Warn("Failed to unmarshal heartbeat for key %s: %v", key, err)
			return
		}
		snapshot.Instances[instanceID] = hb
		return
	}

	stats.TopicStateRecords++
	if record.Value == nil {
		// Tombstone: topic was deleted, remove from snapshot.
		delete(snapshot.Topics, key)
		return
	}

	var state TopicState
	if err := json.Unmarshal(record.Value, &state); err != nil {
		stats.MalformedRecords++
		logging.Warn("Failed to unmarshal state for key %s: %v", key, err)
		return
	}
	snapshot.Topics[state.Topic] = state.Partitions
}

func finalizeStateLoadStats(snapshot *StateSnapshot, stats *StateLoadStats, seenKeys map[string]struct{}, start time.Time) {
	stats.LoadDuration = time.Since(start)
	stats.UniqueKeysSeen = int64(len(seenKeys))
	stats.FinalTopicCount = int64(len(snapshot.Topics))
	stats.FinalInstanceCount = int64(len(snapshot.Instances))

	surviving := stats.FinalTopicCount + stats.FinalInstanceCount
	discarded := stats.TotalRecords - surviving
	if discarded < 0 {
		discarded = 0
	}
	stats.DiscardedRecords = discarded
}

// SubscribeGlobalUpdates continuously tails the tracker topic from startOffsets
// and calls onRecord for every incoming record until ctx is cancelled. A nil
// value in onRecord indicates a tombstone (topic was deleted). startOffsets
// controls the per-partition resume point — use StateLoadStats.ResumeOffsets
// from LoadLatestSnapshot to avoid re-reading already-replayed history.
// Partitions absent from startOffsets default to reading from the latest offset.
func (sm *StateManager) SubscribeGlobalUpdates(ctx context.Context, startOffsets map[int32]int64, onRecord func(key string, value []byte)) {
	partitions, err := sm.client.GetTopicPartitions(ctx, sm.trackerTopic)
	if err != nil || len(partitions) == 0 {
		logging.Warn("SubscribeGlobalUpdates: tracker topic not available: %v", err)
		return
	}

	consumeMap := map[string]map[int32]kgo.Offset{
		sm.trackerTopic: {},
	}
	for _, p := range partitions {
		if offset, ok := startOffsets[p]; ok {
			consumeMap[sm.trackerTopic][p] = kgo.NewOffset().At(offset)
		} else {
			consumeMap[sm.trackerTopic][p] = kgo.NewOffset().AtEnd()
		}
	}

	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(sm.client.config.Brokers...),
		kgo.ClientID(sm.client.NextClientID("global-view")),
		kgo.ConsumePartitions(consumeMap),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.FetchMaxBytes(10 * 1024 * 1024),
	}
	consumerOpts = append(consumerOpts, sm.authOpts...)
	consumerClient, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		logging.Warn("SubscribeGlobalUpdates: failed to create consumer client: %v", err)
		return
	}
	defer consumerClient.Close()

	logging.Debug("SubscribeGlobalUpdates: tailing tracker topic from %d partition(s)", len(partitions))

	for {
		fetches := consumerClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		for _, fe := range fetches.Errors() {
			if fe.Err != context.DeadlineExceeded && fe.Err != context.Canceled {
				logging.Warn("SubscribeGlobalUpdates: error on partition %d: %v", fe.Partition, fe.Err)
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			onRecord(string(record.Key), record.Value)
		})

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
