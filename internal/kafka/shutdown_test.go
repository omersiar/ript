package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestEnsureTrackerTopicReturnsOnCancelledContext verifies that
// EnsureTrackerTopic exits promptly when the context is already cancelled,
// without blocking on broker calls or the internal 30-second deadline.
// This is a regression test for the case where SIGTERM arrives during startup
// while the broker is unreachable.
func TestEnsureTrackerTopicReturnsOnCancelledContext(t *testing.T) {
	// Point at a guaranteed-dead address so no real broker is needed.
	rawClient, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Skipf("could not create test kafka client: %v", err)
	}
	defer rawClient.Close()

	c := &Client{
		client: rawClient,
		config: &ClientConfig{Brokers: []string{"127.0.0.1:1"}},
	}
	sm := &StateManager{
		client:       c,
		trackerTopic: "ript-state",
		partitions:   1,
		replFactor:   1,
		loadTimeout:  time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the call

	start := time.Now()
	err = sm.EnsureTrackerTopic(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("EnsureTrackerTopic took %v with a pre-cancelled context (expected prompt return)", elapsed)
	}
}

// TestWorkloadBalancerStopDoesNotHangWhenNeverStarted verifies that Stop()
// returns promptly even when Start() was never called (client is nil).
func TestWorkloadBalancerStopDoesNotHangWhenNeverStarted(t *testing.T) {
	wb, err := NewWorkloadBalancer(WorkloadBalancerOptions{
		Brokers:           []string{"127.0.0.1:1"},
		TrackerTopic:      "ript-state",
		ConsumerGroupID:   "test-group",
		InstanceID:        "test-instance",
		TrackerPartitions: 1,
	})
	if err != nil {
		t.Fatalf("NewWorkloadBalancer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wb.Stop()
		close(done)
	}()

	select {
	case <-done:
		// passed
	case <-time.After(2 * time.Second):
		t.Fatal("WorkloadBalancer.Stop() blocked when balancer was never started")
	}
}

// TestWorkloadBalancerStopReturnsWithinTimeoutWhenBrokerDown verifies that
// Stop() returns within the 5-second close timeout even when the broker is
// unreachable, instead of hanging indefinitely.
// This is a regression test for the SIGTERM exit-137 issue.
func TestWorkloadBalancerStopReturnsWithinTimeoutWhenBrokerDown(t *testing.T) {
	wb, err := NewWorkloadBalancer(WorkloadBalancerOptions{
		Brokers:           []string{"127.0.0.1:1"},
		TrackerTopic:      "ript-state",
		ConsumerGroupID:   "test-group",
		InstanceID:        "test-instance",
		TrackerPartitions: 1,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		RebalanceTimeout:  45 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorkloadBalancer: %v", err)
	}

	// Inject a dead kgo.Client directly (no goroutines running, simulating a
	// post-Start state where the poll loop has already exited or was never run).
	rawClient, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.ConsumerGroup("test-group"),
		kgo.ConsumeTopics("ript-state"),
		kgo.SessionTimeout(30*time.Second),
	)
	if err != nil {
		t.Skipf("could not create test kafka client: %v", err)
	}
	// Inject without starting a poll loop so wg counter stays at 0.
	wb.mu.Lock()
	wb.client = rawClient
	wb.started = true
	wb.mu.Unlock()

	done := make(chan struct{})
	go func() {
		wb.Stop()
		close(done)
	}()

	// Must return within the 5-second close timeout plus a small margin.
	select {
	case <-done:
		// passed — Stop() did not hang
	case <-time.After(7 * time.Second):
		t.Fatal("WorkloadBalancer.Stop() blocked for >7 seconds with unreachable broker (close timeout not working)")
	}
}
