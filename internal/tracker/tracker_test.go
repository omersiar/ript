package tracker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/models"
)

func TestResolvePartitionTimestampWithoutPreviousState(t *testing.T) {
	now := time.Date(2026, time.March, 28, 12, 0, 0, 0, time.UTC)

	resolved := resolvePartitionTimestamp(nil, 100, now.Unix())

	if resolved != now.Unix() {
		t.Fatalf("expected timestamp %v, got %v", now, resolved)
	}
}

func TestResolvePartitionTimestampPreservesTimestampForUnchangedOffset(t *testing.T) {
	now := time.Date(2026, time.March, 28, 12, 0, 0, 0, time.UTC)
	previousTimestamp := now.Add(-6 * time.Hour)
	previous := &models.PartitionInfo{Offset: 100, Timestamp: previousTimestamp.Unix()}

	resolved := resolvePartitionTimestamp(previous, 100, now.Unix())

	if resolved != previousTimestamp.Unix() {
		t.Fatalf("expected previous timestamp %v, got %v", previousTimestamp, resolved)
	}
}

func TestResolvePartitionTimestampResetsTimestampForChangedOffset(t *testing.T) {
	now := time.Date(2026, time.March, 28, 12, 0, 0, 0, time.UTC)
	previousTimestamp := now.Add(-48 * time.Hour)
	previous := &models.PartitionInfo{Offset: 100, Timestamp: previousTimestamp.Unix()}

	resolved := resolvePartitionTimestamp(previous, 101, now.Unix())

	if resolved != now.Unix() {
		t.Fatalf("expected timestamp to reset to %v, got %v", now, resolved)
	}
}

func TestResolvePartitionTimestampPreservesTimestampWhenOffsetDecreases(t *testing.T) {
	now := time.Date(2026, time.March, 28, 12, 0, 0, 0, time.UTC)
	previousTimestamp := now.Add(-72 * time.Hour)
	previous := &models.PartitionInfo{Offset: 100, Timestamp: previousTimestamp.Unix()}

	resolved := resolvePartitionTimestamp(previous, 95, now.Unix())

	if resolved != previousTimestamp.Unix() {
		t.Fatalf("expected previous timestamp %v, got %v", previousTimestamp, resolved)
	}
}

// newTestTracker builds a minimal TopicTracker suitable for unit-testing
// methods that do not require live Kafka connectivity.
func newTestTracker(instanceID string) *TopicTracker {
	return NewWithOptions(nil, nil, nil, 5, Options{InstanceID: instanceID})
}

func mustMarshalTopicState(t *testing.T, topic string, ts time.Time, partitions map[int32]kafka.PartitionState) []byte {
	t.Helper()
	state := kafka.TopicState{
		Version:    1,
		Topic:      topic,
		Timestamp:  ts.Unix(),
		Partitions: partitions,
	}
	b, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal TopicState: %v", err)
	}
	return b
}

func TestApplyGlobalRecordAddsOrUpdatesTopic(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)

	value := mustMarshalTopicState(t, "events", now, map[int32]kafka.PartitionState{
		0: {Partition: 0, Offset: 42, Timestamp: now.Unix()},
	})

	tt.applyGlobalRecord("events", value)

	snapshot := tt.globalSnapshot.Load()
	topic, ok := snapshot.Topics["events"]
	if !ok {
		t.Fatal("expected topic 'events' in global snapshot after applyGlobalRecord")
	}
	if topic.PartitionCount != 1 {
		t.Errorf("expected PartitionCount=1, got %d", topic.PartitionCount)
	}
	if topic.Partitions[0].Offset != 42 {
		t.Errorf("expected partition 0 offset=42, got %d", topic.Partitions[0].Offset)
	}
}

func TestApplyGlobalRecordTombstoneRemovesTopic(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)

	// Seed the topic first.
	value := mustMarshalTopicState(t, "orders", now, map[int32]kafka.PartitionState{
		0: {Partition: 0, Offset: 10, Timestamp: now.Unix()},
	})
	tt.applyGlobalRecord("orders", value)

	if _, ok := tt.globalSnapshot.Load().Topics["orders"]; !ok {
		t.Fatal("precondition: topic should be present before tombstone")
	}

	// Apply tombstone (nil value).
	tt.applyGlobalRecord("orders", nil)

	if _, ok := tt.globalSnapshot.Load().Topics["orders"]; ok {
		t.Error("expected topic 'orders' to be removed from global snapshot after tombstone")
	}
}

func TestApplyGlobalRecordSkipsHeartbeatKey(t *testing.T) {
	tt := newTestTracker("inst-1")
	before := tt.globalSnapshot.Load().Timestamp

	// Heartbeat key — should be ignored without modifying globalTopics.
	tt.applyGlobalRecord("tracker-instance:inst-2", []byte(`{"instance_id":"inst-2"}`))

	if len(tt.globalTopics) != 0 {
		t.Errorf("expected globalTopics to remain empty, got %d entries", len(tt.globalTopics))
	}
	// Snapshot timestamp should not advance (no rebuild triggered).
	if tt.globalSnapshot.Load().Timestamp > before+1 {
		t.Error("globalSnapshot should not be rebuilt for heartbeat key records")
	}
}

func TestGetSnapshotReturnsGlobalSnapshot(t *testing.T) {
	tt := newTestTracker("inst-1")

	snapshot := tt.GetSnapshot()
	if !snapshot.IsGlobal {
		t.Error("GetSnapshot() should return the global snapshot (IsGlobal=true)")
	}
	if snapshot.LocalInstanceID != "inst-1" {
		t.Errorf("expected LocalInstanceID='inst-1', got %q", snapshot.LocalInstanceID)
	}
}

func TestSyncGlobalFromStatePopulatesSnapshot(t *testing.T) {
	tt := newTestTracker("inst-2")
	now := time.Date(2026, time.March, 30, 12, 0, 0, 0, time.UTC)

	stateSnapshot := &kafka.StateSnapshot{
		Timestamp: now.Unix(),
		Version:   1,
		Topics: map[string]map[int32]kafka.PartitionState{
			"topic-a": {0: {Partition: 0, Offset: 100, Timestamp: now.Unix()}},
			"topic-b": {0: {Partition: 0, Offset: 200, Timestamp: now.Unix()}},
		},
		Instances: map[string]kafka.HeartbeatRecord{},
	}

	tt.syncGlobalFromState(stateSnapshot)

	snapshot := tt.globalSnapshot.Load()
	if !snapshot.IsGlobal {
		t.Error("expected IsGlobal=true after syncGlobalFromState")
	}
	if len(snapshot.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(snapshot.Topics))
	}
	if _, ok := snapshot.Topics["topic-a"]; !ok {
		t.Error("expected 'topic-a' in global snapshot")
	}
	if _, ok := snapshot.Topics["topic-b"]; !ok {
		t.Error("expected 'topic-b' in global snapshot")
	}
}

func TestSyncGlobalFromStateReplacesPreviousTopics(t *testing.T) {
	tt := newTestTracker("inst-2")
	now := time.Date(2026, time.March, 30, 12, 0, 0, 0, time.UTC)

	initial := &kafka.StateSnapshot{
		Timestamp: now.Unix(),
		Version:   1,
		Topics: map[string]map[int32]kafka.PartitionState{
			"topic-old": {0: {Partition: 0, Offset: 10, Timestamp: now.Unix()}},
		},
		Instances: map[string]kafka.HeartbeatRecord{},
	}
	tt.syncGlobalFromState(initial)

	replayed := &kafka.StateSnapshot{
		Timestamp: now.Add(time.Minute).Unix(),
		Version:   1,
		Topics: map[string]map[int32]kafka.PartitionState{
			"topic-new": {0: {Partition: 0, Offset: 20, Timestamp: now.Add(time.Minute).Unix()}},
		},
		Instances: map[string]kafka.HeartbeatRecord{},
	}
	tt.syncGlobalFromState(replayed)

	snapshot := tt.globalSnapshot.Load()
	if _, ok := snapshot.Topics["topic-old"]; ok {
		t.Fatal("expected stale topic to be removed after replay")
	}
	if _, ok := snapshot.Topics["topic-new"]; !ok {
		t.Fatal("expected new topic to exist after replay")
	}
}

func mustMarshalHeartbeatRecord(t *testing.T, record kafka.HeartbeatRecord) []byte {
	t.Helper()
	b, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal HeartbeatRecord: %v", err)
	}
	return b
}

func TestApplyGlobalRecordUpdatesActiveInstances(t *testing.T) {
	tt := newTestTracker("inst-1")

	now := time.Now().UTC() // must be current time for IsActive check
	heartbeat := kafka.HeartbeatRecord{
		Version:              1,
		InstanceID:           "inst-2",
		LastHeartbeatAt:      now.Unix(),
		HeartbeatIntervalSec: 30,
		ScanIntervalSec:      60,
		GroupID:              "tracker-scan",
		AssignedShards:       3,
	}

	tt.applyGlobalRecord("tracker-instance:inst-2", mustMarshalHeartbeatRecord(t, heartbeat))

	instances := tt.GetInstances()
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance in activeInstances, got %d", len(instances))
	}
	inst := instances[0]
	if inst.InstanceID != "inst-2" {
		t.Errorf("expected InstanceID='inst-2', got %q", inst.InstanceID)
	}
	if inst.AssignedShards != 3 {
		t.Errorf("expected AssignedShards=3, got %d", inst.AssignedShards)
	}
	if inst.GroupID != "tracker-scan" {
		t.Errorf("expected GroupID='tracker-scan', got %q", inst.GroupID)
	}
	if !inst.IsActive {
		t.Error("expected IsActive=true for a fresh heartbeat")
	}
}

func TestApplyGlobalRecordTombstoneRemovesInstance(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Now().UTC()

	heartbeat := kafka.HeartbeatRecord{
		Version:              1,
		InstanceID:           "inst-2",
		LastHeartbeatAt:      now.Unix(),
		HeartbeatIntervalSec: 30,
	}
	tt.applyGlobalRecord("tracker-instance:inst-2", mustMarshalHeartbeatRecord(t, heartbeat))

	if len(tt.GetInstances()) != 1 {
		t.Fatal("precondition: instance should be present before tombstone")
	}

	tt.applyGlobalRecord("tracker-instance:inst-2", nil)

	if len(tt.GetInstances()) != 0 {
		t.Error("expected instance to be removed after tombstone")
	}
}

func TestApplyGlobalRecordMultiplePeers(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Now().UTC()

	for _, id := range []string{"inst-2", "inst-3"} {
		heartbeat := kafka.HeartbeatRecord{
			Version:              1,
			InstanceID:           id,
			LastHeartbeatAt:      now.Unix(),
			HeartbeatIntervalSec: 30,
		}
		tt.applyGlobalRecord("tracker-instance:"+id, mustMarshalHeartbeatRecord(t, heartbeat))
	}

	// Self heartbeat merges in without wiping peers.
	selfHeartbeat := kafka.HeartbeatRecord{
		Version:              1,
		InstanceID:           "inst-1",
		LastHeartbeatAt:      now.Unix(),
		HeartbeatIntervalSec: 30,
	}
	tt.applyGlobalRecord("tracker-instance:inst-1", mustMarshalHeartbeatRecord(t, selfHeartbeat))

	instances := tt.GetInstances()
	if len(instances) != 3 {
		t.Errorf("expected 3 instances, got %d", len(instances))
	}
}

func TestSyncInstancesFromStateLoadsGroupIDAndShards(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Now().UTC()

	stateSnapshot := &kafka.StateSnapshot{
		Timestamp: now.Unix(),
		Version:   1,
		Topics:    map[string]map[int32]kafka.PartitionState{},
		Instances: map[string]kafka.HeartbeatRecord{
			"inst-2": {
				Version:              1,
				InstanceID:           "inst-2",
				LastHeartbeatAt:      now.Unix(),
				HeartbeatIntervalSec: 30,
				GroupID:              "tracker-scan",
				AssignedShards:       2,
			},
		},
	}

	tt.syncInstancesFromState(stateSnapshot)

	instances := tt.GetInstances()
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].GroupID != "tracker-scan" {
		t.Errorf("expected GroupID='tracker-scan', got %q", instances[0].GroupID)
	}
	if instances[0].AssignedShards != 2 {
		t.Errorf("expected AssignedShards=2, got %d", instances[0].AssignedShards)
	}
}

// ---------------------------------------------------------------------------
// mergeGlobalTopicRecord
// ---------------------------------------------------------------------------

func TestMergeGlobalTopicRecordNilExistingReturnsIncoming(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)
	incoming := &models.TopicStatus{
		Name:       "topic-x",
		LastUpdate: now.Unix(),
		Partitions: map[int32]*models.PartitionInfo{
			0: {Partition: 0, Offset: 10, Timestamp: now.Unix()},
		},
		PartitionCount: 1,
	}

	merged := mergeGlobalTopicRecord(nil, incoming)
	if merged != incoming {
		t.Fatal("expected nil existing to return incoming unchanged")
	}
}

func TestMergeGlobalTopicRecordKeepsExistingPartitionsAbsentFromIncoming(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)
	existing := &models.TopicStatus{
		Name:       "topic-x",
		LastUpdate: now.Unix(),
		Partitions: map[int32]*models.PartitionInfo{
			0: {Partition: 0, Offset: 50, Timestamp: now.Add(-24 * time.Hour).Unix()},
			1: {Partition: 1, Offset: 60, Timestamp: now.Add(-12 * time.Hour).Unix()},
		},
		PartitionCount: 2,
	}
	incoming := &models.TopicStatus{
		Name:       "topic-x",
		LastUpdate: now.Unix(),
		Partitions: map[int32]*models.PartitionInfo{
			2: {Partition: 2, Offset: 100, Timestamp: now.Unix()},
			3: {Partition: 3, Offset: 200, Timestamp: now.Unix()},
		},
		PartitionCount: 2,
	}

	merged := mergeGlobalTopicRecord(existing, incoming)

	if merged.PartitionCount != 4 {
		t.Errorf("expected PartitionCount=4, got %d", merged.PartitionCount)
	}
	if len(merged.Partitions) != 4 {
		t.Errorf("expected 4 partitions, got %d", len(merged.Partitions))
	}
	for _, id := range []int32{0, 1, 2, 3} {
		if _, ok := merged.Partitions[id]; !ok {
			t.Errorf("expected partition %d in merged result", id)
		}
	}
}

func TestMergeGlobalTopicRecordIncomingPartitionsOverrideExisting(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)
	existing := &models.TopicStatus{
		Name:       "topic-x",
		LastUpdate: now.Add(-time.Hour).Unix(),
		Partitions: map[int32]*models.PartitionInfo{
			0: {Partition: 0, Offset: 50, Timestamp: now.Add(-time.Hour).Unix()},
		},
		PartitionCount: 1,
	}
	incoming := &models.TopicStatus{
		Name:       "topic-x",
		LastUpdate: now.Unix(),
		Partitions: map[int32]*models.PartitionInfo{
			0: {Partition: 0, Offset: 100, Timestamp: now.Unix()},
		},
		PartitionCount: 1,
	}

	merged := mergeGlobalTopicRecord(existing, incoming)

	if merged.Partitions[0].Offset != 100 {
		t.Errorf("expected incoming offset=100 to win, got %d", merged.Partitions[0].Offset)
	}
}

func TestMergeGlobalTopicRecordRecalculatesPartitionCount(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)
	existing := &models.TopicStatus{
		Name:       "topic-x",
		Partitions: map[int32]*models.PartitionInfo{
			1: {Partition: 1, Offset: 10, Timestamp: now.Unix()},
		},
		PartitionCount: 1,
	}
	incoming := &models.TopicStatus{
		Name:       "topic-x",
		LastUpdate: now.Unix(),
		Partitions: map[int32]*models.PartitionInfo{
			0: {Partition: 0, Offset: 20, Timestamp: now.Unix()},
		},
		PartitionCount: 1,
	}

	merged := mergeGlobalTopicRecord(existing, incoming)

	if int(merged.PartitionCount) != len(merged.Partitions) {
		t.Errorf("PartitionCount=%d but len(Partitions)=%d; should match", merged.PartitionCount, len(merged.Partitions))
	}
}

// ---------------------------------------------------------------------------
// applyGlobalRecord — multi-instance partition accumulation
// ---------------------------------------------------------------------------

// TestApplyGlobalRecordMergesPartitionsAcrossInstances verifies that successive
// writes from two instances covering different partitions of the same topic are
// accumulated in the global snapshot rather than the later write discarding the
// earlier one.
func TestApplyGlobalRecordMergesPartitionsAcrossInstances(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)

	// Instance A owns partitions 0 and 1.
	valueA := mustMarshalTopicState(t, "events", now, map[int32]kafka.PartitionState{
		0: {Partition: 0, Offset: 10, Timestamp: now.Unix()},
		1: {Partition: 1, Offset: 20, Timestamp: now.Unix()},
	})
	tt.applyGlobalRecord("events", valueA)

	// Instance B owns partitions 2 and 3 — its write must not erase 0 and 1.
	valueB := mustMarshalTopicState(t, "events", now, map[int32]kafka.PartitionState{
		2: {Partition: 2, Offset: 30, Timestamp: now.Unix()},
		3: {Partition: 3, Offset: 40, Timestamp: now.Unix()},
	})
	tt.applyGlobalRecord("events", valueB)

	topic := tt.globalSnapshot.Load().Topics["events"]
	if topic == nil {
		t.Fatal("topic 'events' missing from global snapshot")
	}
	if topic.PartitionCount != 4 {
		t.Errorf("expected PartitionCount=4, got %d", topic.PartitionCount)
	}
	for _, id := range []int32{0, 1, 2, 3} {
		if _, ok := topic.Partitions[id]; !ok {
			t.Errorf("expected partition %d in global snapshot after multi-instance writes", id)
		}
	}
}

// TestApplyGlobalRecordSubsequentUpdateForSamePartitionOverwrites verifies that
// a later write for a partition that already exists in the global snapshot
// takes precedence (incoming wins for owned partitions).
func TestApplyGlobalRecordSubsequentUpdateForSamePartitionOverwrites(t *testing.T) {
	tt := newTestTracker("inst-1")
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)

	first := mustMarshalTopicState(t, "orders", now, map[int32]kafka.PartitionState{
		0: {Partition: 0, Offset: 50, Timestamp: now.Unix()},
	})
	tt.applyGlobalRecord("orders", first)

	later := mustMarshalTopicState(t, "orders", now.Add(time.Minute), map[int32]kafka.PartitionState{
		0: {Partition: 0, Offset: 100, Timestamp: now.Add(time.Minute).Unix()},
	})
	tt.applyGlobalRecord("orders", later)

	topic := tt.globalSnapshot.Load().Topics["orders"]
	if topic == nil {
		t.Fatal("topic 'orders' missing from global snapshot")
	}
	if topic.Partitions[0].Offset != 100 {
		t.Errorf("expected updated offset=100, got %d", topic.Partitions[0].Offset)
	}
}

