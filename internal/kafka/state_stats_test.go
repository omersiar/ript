package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestApplyRecordToSnapshotCountsDuplicateAndTombstone(t *testing.T) {
	snapshot := &StateSnapshot{
		Topics:    make(map[string]map[int32]PartitionState),
		Instances: make(map[string]HeartbeatRecord),
	}
	stats := &StateLoadStats{}
	seenKeys := make(map[string]struct{})
	start := time.Now()

	state := TopicState{
		Version:    1,
		Topic:      "orders",
		Timestamp:  time.Now().UTC(),
		Partitions: map[int32]PartitionState{0: {Partition: 0, Offset: 10, Timestamp: time.Now().UTC()}},
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal topic state: %v", err)
	}

	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte("orders"), Value: stateBytes})
	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte("orders"), Value: stateBytes})
	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte("orders"), Value: nil})
	finalizeStateLoadStats(snapshot, stats, seenKeys, start)

	if got, want := stats.TotalRecords, int64(3); got != want {
		t.Fatalf("TotalRecords=%d, want %d", got, want)
	}
	if got, want := stats.DuplicateKeyRecords, int64(2); got != want {
		t.Fatalf("DuplicateKeyRecords=%d, want %d", got, want)
	}
	if got, want := stats.TombstoneRecords, int64(1); got != want {
		t.Fatalf("TombstoneRecords=%d, want %d", got, want)
	}
	if got, want := stats.FinalTopicCount, int64(0); got != want {
		t.Fatalf("FinalTopicCount=%d, want %d", got, want)
	}
	if got, want := stats.DiscardedRecords, int64(3); got != want {
		t.Fatalf("DiscardedRecords=%d, want %d", got, want)
	}
}

func TestApplyRecordToSnapshotCountsMalformedAndHeartbeats(t *testing.T) {
	snapshot := &StateSnapshot{
		Topics:    make(map[string]map[int32]PartitionState),
		Instances: make(map[string]HeartbeatRecord),
	}
	stats := &StateLoadStats{}
	seenKeys := make(map[string]struct{})
	start := time.Now()

	hb := HeartbeatRecord{
		Version:              1,
		InstanceID:           "i-1",
		LastHeartbeatAt:      time.Now().UTC(),
		HeartbeatIntervalSec: 30,
	}
	hbBytes, err := json.Marshal(hb)
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}

	state := TopicState{
		Version:    1,
		Topic:      "payments",
		Timestamp:  time.Now().UTC(),
		Partitions: map[int32]PartitionState{0: {Partition: 0, Offset: 20, Timestamp: time.Now().UTC()}},
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal topic state: %v", err)
	}

	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte(ownershipRecordPrefix + "i-1"), Value: hbBytes})
	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte(ownershipRecordPrefix + "i-1"), Value: []byte("{not-json")})
	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte("broken-topic"), Value: []byte("{not-json")})
	applyRecordToSnapshot(snapshot, stats, seenKeys, &kgo.Record{Key: []byte("payments"), Value: stateBytes})
	finalizeStateLoadStats(snapshot, stats, seenKeys, start)

	if got, want := stats.TotalRecords, int64(4); got != want {
		t.Fatalf("TotalRecords=%d, want %d", got, want)
	}
	if got, want := stats.DuplicateKeyRecords, int64(1); got != want {
		t.Fatalf("DuplicateKeyRecords=%d, want %d", got, want)
	}
	if got, want := stats.HeartbeatRecords, int64(2); got != want {
		t.Fatalf("HeartbeatRecords=%d, want %d", got, want)
	}
	if got, want := stats.MalformedRecords, int64(2); got != want {
		t.Fatalf("MalformedRecords=%d, want %d", got, want)
	}
	if got, want := stats.UniqueKeysSeen, int64(3); got != want {
		t.Fatalf("UniqueKeysSeen=%d, want %d", got, want)
	}
	if got, want := stats.FinalTopicCount, int64(1); got != want {
		t.Fatalf("FinalTopicCount=%d, want %d", got, want)
	}
	if got, want := stats.FinalInstanceCount, int64(1); got != want {
		t.Fatalf("FinalInstanceCount=%d, want %d", got, want)
	}
	if got, want := stats.DiscardedRecords, int64(2); got != want {
		t.Fatalf("DiscardedRecords=%d, want %d", got, want)
	}
}
