package kafka

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

// WorkloadBalancer uses Kafka consumer-group partition assignment on the
// tracker topic as a stable shard map for scan ownership.
type WorkloadBalancer struct {
	brokers           []string
	trackerTopic      string
	consumerGroupID   string
	instanceID        string
	trackerPartitions int32
	sessionTimeout    time.Duration
	heartbeatInterval time.Duration
	rebalanceTimeout  time.Duration
	clientID          string
	authOpts          []kgo.Opt

	mu             sync.RWMutex
	assignedShards map[int32]struct{}
	started        bool

	client   *kgo.Client
	stopChan chan struct{}
	wg       sync.WaitGroup
}

type WorkloadBalancerOptions struct {
	Brokers           []string
	TrackerTopic      string
	ConsumerGroupID   string
	InstanceID        string
	TrackerPartitions int32
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
	ClientID          string
	AuthOpts          []kgo.Opt
}

func NewWorkloadBalancer(opts WorkloadBalancerOptions) (*WorkloadBalancer, error) {
	if len(opts.Brokers) == 0 {
		return nil, fmt.Errorf("brokers cannot be empty")
	}
	if strings.TrimSpace(opts.TrackerTopic) == "" {
		return nil, fmt.Errorf("tracker topic cannot be empty")
	}
	if strings.TrimSpace(opts.ConsumerGroupID) == "" {
		return nil, fmt.Errorf("consumer group id cannot be empty")
	}
	if opts.TrackerPartitions < 1 {
		return nil, fmt.Errorf("tracker partitions must be at least 1")
	}
	if opts.SessionTimeout <= 0 {
		opts.SessionTimeout = 30 * time.Second
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 3 * time.Second
	}
	if opts.RebalanceTimeout <= 0 {
		opts.RebalanceTimeout = 45 * time.Second
	}
	if strings.TrimSpace(opts.ClientID) == "" {
		opts.ClientID = fmt.Sprintf("ript-%s-group", sanitizeClientIDSegment(opts.InstanceID))
	}

	return &WorkloadBalancer{
		brokers:           slices.Clone(opts.Brokers),
		trackerTopic:      opts.TrackerTopic,
		consumerGroupID:   opts.ConsumerGroupID,
		instanceID:        opts.InstanceID,
		trackerPartitions: opts.TrackerPartitions,
		sessionTimeout:    opts.SessionTimeout,
		heartbeatInterval: opts.HeartbeatInterval,
		rebalanceTimeout:  opts.RebalanceTimeout,
		clientID:          opts.ClientID,
		authOpts:          slices.Clone(opts.AuthOpts),
		assignedShards:    make(map[int32]struct{}),
		stopChan:          make(chan struct{}),
	}, nil
}

func (b *WorkloadBalancer) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return nil
	}
	b.started = true
	b.mu.Unlock()

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(b.brokers...),
		kgo.ClientID(b.clientID),
		kgo.ConsumerGroup(b.consumerGroupID),
		kgo.ConsumeTopics(b.trackerTopic),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(b.sessionTimeout),
		kgo.HeartbeatInterval(b.heartbeatInterval),
		kgo.RebalanceTimeout(b.rebalanceTimeout),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			b.addAssignments(assigned[b.trackerTopic])
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			b.removeAssignments(revoked[b.trackerTopic])
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
			b.updateAssignment(nil)
		}),
	}
	clientOpts = append(clientOpts, b.authOpts...)
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		b.mu.Lock()
		b.started = false
		b.mu.Unlock()
		return fmt.Errorf("failed to create workload balancer client: %w", err)
	}

	b.mu.Lock()
	b.client = client
	b.mu.Unlock()

	b.wg.Add(1)
	go b.pollLoop(ctx)

	logging.Info("Workload balancer started: group=%s topic=%s client_id=%s", b.consumerGroupID, b.trackerTopic, b.clientID)
	return nil
}

func (b *WorkloadBalancer) Stop() {
	close(b.stopChan)

	// Extract the client and nil the field under the lock, then call Close()
	// outside the lock. Close() drives the full LeaveGroup flow and eventually
	// fires OnPartitionsRevoked → removeAssignments() which needs b.mu. Calling
	// Close() while holding b.mu would deadlock.
	b.mu.Lock()
	client := b.client
	b.client = nil
	b.started = false
	b.mu.Unlock()

	if client != nil {
		// client.Close() sends a LeaveGroup request which can block indefinitely
		// when the broker is unreachable. Run it in a goroutine with a hard
		// deadline so Stop() is never stuck behind a down broker.
		closeDone := make(chan struct{})
		go func() {
			client.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(5 * time.Second):
			logging.Warn("Workload balancer client close timed out (broker may be unreachable)")
		}
	}

	b.wg.Wait()
}

func (b *WorkloadBalancer) pollLoop(ctx context.Context) {
	defer b.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopChan:
			return
		default:
		}

		b.mu.RLock()
		client := b.client
		b.mu.RUnlock()
		if client == nil {
			return
		}

		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.IsClientClosed() {
			return
		}
		for _, fe := range fetches.Errors() {
			if fe.Err != nil && fe.Err != context.Canceled && fe.Err != context.DeadlineExceeded {
				logging.Warn("Workload balancer poll error on %s[%d]: %v", fe.Topic, fe.Partition, fe.Err)
			}
		}
	}
}

// updateAssignment replaces the entire shard set atomically. Used on
// ungraceful partition loss (OnPartitionsLost) where a full reset is safest.
func (b *WorkloadBalancer) updateAssignment(partitions []int32) {
	assignment := make(map[int32]struct{}, len(partitions))
	for _, p := range partitions {
		assignment[p] = struct{}{}
	}

	b.mu.Lock()
	b.assignedShards = assignment
	b.mu.Unlock()

	logging.Info("Workload assignment reset: group=%s assigned_shards=%d", b.consumerGroupID, len(partitions))
}

// addAssignments incrementally adds partitions to the shard set.
// Called by OnPartitionsAssigned during cooperative rebalance — only the
// newly granted partitions are delivered, not the full set.
func (b *WorkloadBalancer) addAssignments(partitions []int32) {
	b.mu.Lock()
	for _, p := range partitions {
		b.assignedShards[p] = struct{}{}
	}
	total := len(b.assignedShards)
	b.mu.Unlock()

	logging.Info("Workload assignment gained: group=%s added=%d assigned_shards=%d", b.consumerGroupID, len(partitions), total)
}

// removeAssignments incrementally removes partitions from the shard set.
// Called by OnPartitionsRevoked during cooperative rebalance — only the
// partitions being transferred away are delivered.
func (b *WorkloadBalancer) removeAssignments(partitions []int32) {
	b.mu.Lock()
	for _, p := range partitions {
		delete(b.assignedShards, p)
	}
	total := len(b.assignedShards)
	b.mu.Unlock()

	logging.Info("Workload assignment released: group=%s removed=%d assigned_shards=%d", b.consumerGroupID, len(partitions), total)
}

func (b *WorkloadBalancer) HasAssignments() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.assignedShards) > 0
}

func (b *WorkloadBalancer) WaitForAssignments(ctx context.Context, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if b.HasAssignments() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

func (b *WorkloadBalancer) OwnsTopicPartition(topic string, partition int32) bool {
	shard := workloadShard(topic, b.trackerPartitions)
	b.mu.RLock()
	_, ok := b.assignedShards[shard]
	b.mu.RUnlock()
	return ok
}

func (b *WorkloadBalancer) AssignedShardCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.assignedShards)
}

func (b *WorkloadBalancer) GroupID() string {
	return b.consumerGroupID
}

func workloadShard(topic string, shardCount int32) int32 {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	return int32(h.Sum32() % uint32(shardCount))
}

func sanitizeClientIDSegment(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "instance"
	}
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", ":", "-")
	return replacer.Replace(trimmed)
}
