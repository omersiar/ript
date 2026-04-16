package kafka

import (
	"context"
	"testing"
	"time"
)

func TestWorkloadShardUsesPartitionInHash(t *testing.T) {
	const shardCount int32 = 6
	seen := make(map[int32]struct{})

	for partition := int32(0); partition < 32; partition++ {
		seen[workloadShard("orders", partition, shardCount)] = struct{}{}
	}

	if len(seen) <= 1 {
		t.Fatalf("expected same topic to spread across multiple shards, got %d", len(seen))
	}
}

func TestOwnsTopicPartitionCanSplitSameTopicAcrossAssignments(t *testing.T) {
	b := &WorkloadBalancer{
		consumerGroupID:   "group-a",
		trackerPartitions: 6,
		assignedShards:    map[int32]struct{}{2: {}},
	}

	var ownedPartition int32 = -1
	var unownedPartition int32 = -1

	for partition := int32(0); partition < 128; partition++ {
		if b.OwnsTopicPartition("orders", partition) {
			ownedPartition = partition
		} else {
			unownedPartition = partition
		}
		if ownedPartition >= 0 && unownedPartition >= 0 {
			break
		}
	}

	if ownedPartition < 0 || unownedPartition < 0 {
		t.Fatal("expected to find both owned and unowned partitions for same topic")
	}
}

func TestWorkloadBalancerMarksRebalancingOnRevokeAndCompletesOnAssign(t *testing.T) {
	b := &WorkloadBalancer{
		consumerGroupID: "group-a",
		assignedShards:  make(map[int32]struct{}),
		rebalancing:     false,
	}

	b.removeAssignments([]int32{1})
	if !b.IsRebalancing() {
		t.Fatal("expected rebalancing=true after revoke")
	}

	b.addAssignments([]int32{2, 3})
	if b.IsRebalancing() {
		t.Fatal("expected rebalancing=false after assignment")
	}
	if got, want := b.AssignedShardCount(), 2; got != want {
		t.Fatalf("assigned shard count=%d, want %d", got, want)
	}
	if got := b.AssignmentEpoch(); got != 1 {
		t.Fatalf("assignment epoch=%d, want 1", got)
	}
}

func TestWorkloadBalancerWaitForStableAssignments(t *testing.T) {
	b := &WorkloadBalancer{
		consumerGroupID: "group-a",
		assignedShards:  make(map[int32]struct{}),
		rebalancing:     true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan bool, 1)
	go func() {
		done <- b.WaitForStableAssignments(ctx, 2*time.Second)
	}()

	time.Sleep(50 * time.Millisecond)
	b.addAssignments([]int32{0})

	if ok := <-done; !ok {
		t.Fatal("expected WaitForStableAssignments to return true after assignment")
	}
}

func TestWorkloadBalancerWaitForStableAssignmentsTimeout(t *testing.T) {
	b := &WorkloadBalancer{
		consumerGroupID: "group-a",
		assignedShards:  make(map[int32]struct{}),
		rebalancing:     true,
	}

	ctx := context.Background()
	if ok := b.WaitForStableAssignments(ctx, 100*time.Millisecond); ok {
		t.Fatal("expected WaitForStableAssignments to time out while still rebalancing")
	}
}
