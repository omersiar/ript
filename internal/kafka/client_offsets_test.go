package kafka

import "testing"

func TestNewLatestOffsetPartitionRequestPreservesDefaultLeaderEpoch(t *testing.T) {
	req := newLatestOffsetPartitionRequest(7)

	if req.Partition != 7 {
		t.Fatalf("unexpected partition: got %d want %d", req.Partition, int32(7))
	}
	if req.CurrentLeaderEpoch != -1 {
		t.Fatalf("unexpected current leader epoch: got %d want -1", req.CurrentLeaderEpoch)
	}
}

func TestNewLatestOffsetPartitionRequestUsesLatestTimestamp(t *testing.T) {
	req := newLatestOffsetPartitionRequest(0)

	if req.Timestamp != -1 {
		t.Fatalf("unexpected timestamp selector: got %d want -1", req.Timestamp)
	}
}

func TestNewEarliestOffsetPartitionRequestPreservesDefaultLeaderEpoch(t *testing.T) {
	req := newEarliestOffsetPartitionRequest(3)

	if req.Partition != 3 {
		t.Fatalf("unexpected partition: got %d want %d", req.Partition, int32(3))
	}
	if req.CurrentLeaderEpoch != -1 {
		t.Fatalf("unexpected current leader epoch: got %d want -1", req.CurrentLeaderEpoch)
	}
}

func TestNewEarliestOffsetPartitionRequestUsesEarliestTimestamp(t *testing.T) {
	req := newEarliestOffsetPartitionRequest(0)

	if req.Timestamp != -2 {
		t.Fatalf("unexpected timestamp selector: got %d want -2", req.Timestamp)
	}
}
