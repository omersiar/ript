package kafka

import (
	"context"
	"testing"
	"time"
)

func TestTopicShardDeterminismAndRange(t *testing.T) {
	const shardCount int32 = 6

	// Determinism: same topic always maps to the same shard.
	for _, topic := range []string{"orders", "payments", "user-events", "metrics", "__consumer_offsets"} {
		s1 := topicShard(topic, shardCount)
		s2 := topicShard(topic, shardCount)
		if s1 != s2 {
			t.Fatalf("topicShard(%q) not deterministic: %d != %d", topic, s1, s2)
		}
		if s1 < 0 || s1 >= shardCount {
			t.Fatalf("topicShard(%q) = %d out of range [0, %d)", topic, s1, shardCount)
		}
	}

	// Distribution: a set of topics should spread across more than one shard.
	topics := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	seen := make(map[int32]struct{})
	for _, topic := range topics {
		seen[topicShard(topic, shardCount)] = struct{}{}
	}
	if len(seen) <= 1 {
		t.Fatalf("expected topics to spread across multiple shards, all landed on %v", seen)
	}
}

func TestTopicShardSinglePartitionAlwaysZero(t *testing.T) {
	for _, topic := range []string{"orders", "payments", "anything"} {
		if got := topicShard(topic, 1); got != 0 {
			t.Fatalf("topicShard(%q, 1) = %d, want 0", topic, got)
		}
	}
}

// TestTopicShardMatchesKafkaMurmur2 verifies a known reference value computed
// against Kafka's Java murmur2 implementation. "test-topic" with 6 partitions
// should map to partition 1 (murmur2 = 0xA1B6651B; (0xA1B6651B & 0x7FFFFFFF) % 6 = 1).
// This matches the Java reference: org.apache.kafka.common.utils.Utils.murmur2.
func TestTopicShardMatchesKafkaMurmur2(t *testing.T) {
	const topic = "test-topic"
	const shardCount int32 = 6
	const wantShard int32 = 1 // (0xA1B6651B & 0x7FFFFFFF) % 6 = 1
	if got := topicShard(topic, shardCount); got != wantShard {
		t.Fatalf("topicShard(%q, %d) = %d, want %d (Kafka Murmur2 reference)", topic, shardCount, got, wantShard)
	}
}

func TestOwnsTopicOwnsAllPartitions(t *testing.T) {
	const shardCount int32 = 6
	// Find a topic whose shard we can control.
	// "orders" hashes to shard topicShard("orders", 6).
	ownedShard := topicShard("orders", shardCount)

	b := &WorkloadBalancer{
		consumerGroupID:   "group-a",
		trackerPartitions: shardCount,
		assignedShards:    map[int32]struct{}{ownedShard: {}},
	}

	if !b.OwnsTopic("orders") {
		t.Fatal("expected OwnsTopic(\"orders\") = true for the assigned shard")
	}

	// A topic that hashes to a different shard should not be owned.
	var unownedTopic string
	for _, candidate := range []string{"payments", "user-events", "metrics", "logs", "alerts"} {
		if topicShard(candidate, shardCount) != ownedShard {
			unownedTopic = candidate
			break
		}
	}
	if unownedTopic == "" {
		t.Skip("all candidate topics happen to share the same shard — adjust candidates")
	}
	if b.OwnsTopic(unownedTopic) {
		t.Fatalf("expected OwnsTopic(%q) = false (shard %d not assigned)", unownedTopic, topicShard(unownedTopic, shardCount))
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
