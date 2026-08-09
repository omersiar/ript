package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
)

// ScanManager coordinates scan scheduling and one-shot scan execution.
type ScanManager struct {
	tracker *TopicTracker
}

type scanTopicData struct {
	name            string
	partitions      []int32
	offsets         map[int32]int64
	earliestOffsets map[int32]int64
}

func NewScanManager(tracker *TopicTracker) *ScanManager {
	return &ScanManager{tracker: tracker}
}

func (m *ScanManager) startLoop(ctx context.Context) {
	t := m.tracker
	t.wg.Add(1)
	go m.scanLoop(ctx)
}

func (m *ScanManager) scanLoop(ctx context.Context) {
	defer m.tracker.wg.Done()

	// Run one scan immediately on startup so the first view is populated
	// without waiting for the first ticker interval.
	m.runScanCycle(ctx)

	ticker := time.NewTicker(m.tracker.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.tracker.stopChan:
			return
		case <-ticker.C:
			m.runScanCycle(ctx)
		}
	}
}

func (m *ScanManager) runOnceScan(ctx context.Context) error {
	if err := m.tracker.stateManager.EnsureTrackerTopic(ctx); err != nil {
		logging.Warn("Could not ensure tracker topic: %v", err)
	}
	snapshot, _, err := m.tracker.stateManager.LoadLatestSnapshot(ctx)
	if err == nil && snapshot != nil {
		m.tracker.syncGlobalFromState(snapshot)
	}
	return m.scanTopics(ctx)
}

func (m *ScanManager) runScanCycle(ctx context.Context) {
	t := m.tracker
	if !t.scanMu.TryLock() {
		logging.Warn("Skipping scan cycle: previous scan still in progress")
		return
	}
	if err := m.prepareForScan(ctx); err != nil {
		t.scanMu.Unlock()
		logging.Warn("Skipping scan cycle: %v", err)
		return
	}
	err := m.scanTopics(ctx)
	t.scanMu.Unlock()
	if err != nil {
		logging.Error("Error during scan: %v", err)
	}
}

func (m *ScanManager) prepareForScan(ctx context.Context) error {
	t := m.tracker
	if t.workloadBalancer == nil {
		return nil
	}

	if !t.workloadBalancer.WaitForStableAssignments(ctx, 30*time.Second) {
		return fmt.Errorf("consumer group rebalance still in progress")
	}
	if t.workloadBalancer.AssignedShardCount() == 0 {
		return fmt.Errorf("no shards assigned after rebalance stabilization")
	}

	epoch := t.workloadBalancer.AssignmentEpoch()
	if epoch == t.assignmentEpoch {
		return nil
	}

	logging.Info("Workload assignment epoch changed: previous=%d current=%d; replaying tracker state", t.assignmentEpoch, epoch)
	if err := m.replayState(ctx); err != nil {
		return fmt.Errorf("failed to replay tracker state after rebalance: %w", err)
	}

	t.assignmentEpoch = epoch
	return nil
}

func (m *ScanManager) replayState(ctx context.Context) error {
	t := m.tracker
	if t.stateManager == nil {
		return nil
	}

	snapshot, loadStats, err := t.stateManager.LoadLatestSnapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return nil
	}

	t.syncGlobalFromState(snapshot)
	t.heartbeatManager.syncInstancesFromState(snapshot)

	if loadStats != nil && loadStats.TopicExists {
		status := "complete"
		if loadStats.TimedOut {
			status = "partial_timeout"
		}
		logging.Info("Post-rebalance state replay stats: total_messages=%d duplicate_keys=%d discarded=%d tombstones=%d unique_keys=%d malformed=%d partitions_with_data=%d duration_ms=%d final_topics=%d final_instances=%d status=%s",
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

	return nil
}

func (m *ScanManager) scanTopics(ctx context.Context) error {
	t := m.tracker
	scanStartedAt := time.Now()

	// Single MetadataRequest returns all topics with their full partition lists,
	// replacing the previous pattern of 1 ListTopics + N GetTopicPartitions calls.
	allTopics, err := t.kafkaClient.ListTopicsWithPartitions(ctx)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	previousGlobalSnapshot := t.globalSnapshot.Load()

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

	// Apply ownership filter: only track topics assigned to this instance.
	// Ownership is per topic (not per partition) — the same partition that the
	// StateManager writes the topic's state record to determines ownership, using
	// Kafka's standard Murmur2 hash of the topic name. All physical partitions of
	// an owned topic are scanned by this instance; none are split across instances.
	ownedTopicPartitions := make(map[string][]int32, len(allTopics))
	for topicName, partitions := range allTopics {
		if t.workloadBalancer != nil && !t.workloadBalancer.OwnsTopic(topicName) {
			continue
		}
		assignedTopics++
		ownedTopicPartitions[topicName] = partitions
	}

	// During transient rebalance/loss windows (for example after host sleep),
	// this instance can briefly own zero shards. Do not replace local snapshot
	// with an empty/partial view in that window, otherwise unchanged partitions
	// lose their previous timestamps and ages reset on the next scan.
	if len(ownedTopicPartitions) == 0 {
		assignedShards := 0
		if t.workloadBalancer != nil {
			assignedShards = t.workloadBalancer.AssignedShardCount()
		}
		logging.Warn("Skipping scan cycle: no owned topic partitions assigned (assigned_shards=%d)", assignedShards)
		return nil
	}

	// Fetch latest (high watermark) and earliest (log-start) offsets in parallel
	// using two sharded ListOffsetsRequests. Franz-go batches each by broker, so
	// both are O(brokers), not O(topics). Both results are read before any error
	// check to ensure goroutines are never leaked.
	type offsetResult struct {
		offsets map[string]map[int32]int64
		err     error
	}

	latestCh := make(chan offsetResult, 1)
	earliestCh := make(chan offsetResult, 1)

	go func() {
		offsets, err := t.kafkaClient.GetHighWatermarksBatch(ctx, ownedTopicPartitions)
		latestCh <- offsetResult{offsets, err}
	}()
	go func() {
		offsets, err := t.kafkaClient.GetEarliestWatermarksBatch(ctx, ownedTopicPartitions)
		earliestCh <- offsetResult{offsets, err}
	}()

	latestResult := <-latestCh
	earliestResult := <-earliestCh

	if latestResult.err != nil {
		return fmt.Errorf("failed to get high watermarks: %w", latestResult.err)
	}
	if earliestResult.err != nil {
		return fmt.Errorf("failed to get earliest offsets: %w", earliestResult.err)
	}

	allOffsets := latestResult.offsets
	allEarliestOffsets := earliestResult.offsets

	// Determine which owned topics need a retention-policy describe refresh.
	// A topic needs refresh when: (a) policy was never fetched (nil), or
	// (b) the cached value is older than the configured TTL.
	cacheTTL := int64(t.configCacheTTLDays) * 86400
	topicsNeedingDescribe := make([]string, 0, len(ownedTopicPartitions))
	for topicName := range ownedTopicPartitions {
		var cachedFetchedAt int64
		if prev, ok := previousGlobalSnapshot.Topics[topicName]; ok && prev.RetentionPolicy != nil {
			cachedFetchedAt = prev.RetentionPolicy.FetchedAt
		}
		if cachedFetchedAt == 0 || (scanTime-cachedFetchedAt) >= cacheTTL {
			topicsNeedingDescribe = append(topicsNeedingDescribe, topicName)
		}
	}

	// Batch-describe topics that need a policy refresh. Franz-go sends one
	// DescribeConfigs request per 200-topic chunk, keeping broker load light.
	freshPolicies := map[string]string{}
	if len(topicsNeedingDescribe) > 0 {
		logging.Info("Describing retention policy for %d topic(s) (cache TTL=%d days)", len(topicsNeedingDescribe), t.configCacheTTLDays)
		var describeErr error
		freshPolicies, describeErr = t.kafkaClient.GetTopicConfigsBatch(ctx, topicsNeedingDescribe)
		if describeErr != nil {
			logging.Warn("Failed to describe topic configs: %v", describeErr)
			freshPolicies = map[string]string{}
		}
	}

	topicData := make([]scanTopicData, 0, len(ownedTopicPartitions))
	for topicName, ownedPartitions := range ownedTopicPartitions {
		offsets, ok := allOffsets[topicName]
		if !ok {
			logging.Warn("No offsets returned for topic %s, skipping", topicName)
			continue
		}
		topicData = append(topicData, scanTopicData{
			name:            topicName,
			partitions:      ownedPartitions,
			offsets:         offsets,
			earliestOffsets: allEarliestOffsets[topicName],
		})
	}

	for _, meta := range topicData {
		topicStatus := &models.TopicStatus{
			Name:           meta.name,
			PartitionCount: int32(len(meta.partitions)),
			Partitions:     make(map[int32]*models.PartitionInfo),
			LastUpdate:     scanTime,
			DiscoveryTime:  scanTime,
		}

		// Preserve the Ignored flag from the previous snapshot to ensure that
		// ignore state set via API persists across scan cycles.
		// Also carry forward the existing retention policy as a fallback.
		if previousTopic, ok := previousGlobalSnapshot.Topics[meta.name]; ok {
			topicStatus.Ignored = previousTopic.Ignored
			topicStatus.IgnoredAt = previousTopic.IgnoredAt
			if previousTopic.DiscoveryTime > 0 {
				topicStatus.DiscoveryTime = previousTopic.DiscoveryTime
			}
			topicStatus.RetentionPolicy = previousTopic.RetentionPolicy
		}

		// Apply freshly described retention policy if available.
		if policy, ok := freshPolicies[meta.name]; ok {
			topicStatus.RetentionPolicy = &models.RetentionPolicy{
				CleanupPolicy: policy,
				FetchedAt:     scanTime,
			}
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

			earliestOffset := meta.earliestOffsets[partID]
			previous := previousPartitionInfo(previousGlobalSnapshot, meta.name, partID)
			partInfo := buildPartitionInfo(partID, offset, earliestOffset, previous, scanTime)

			topicStatus.Partitions[partID] = partInfo

			if oldestTimestamp == 0 || partInfo.Timestamp < oldestTimestamp {
				oldestTimestamp = partInfo.Timestamp
			}
			if newestTimestamp == 0 || partInfo.Timestamp > newestTimestamp {
				newestTimestamp = partInfo.Timestamp
			}
			if !topicStatus.RetentionPolicy.IsCompacted() {
				topicStatus.TotalMessageCount += partInfo.MessageCount
			}
		}

		if oldestTimestamp > 0 {
			topicStatus.OldestPartitionAge = models.CalculateDuration(time.Unix(oldestTimestamp, 0).UTC())
		}
		if newestTimestamp > 0 {
			topicStatus.NewestPartitionAge = models.CalculateDuration(time.Unix(newestTimestamp, 0).UTC())
		}

		// Include non-owned partitions from the global snapshot so that the
		// record written to the tracker topic always carries the full partition
		// state for this topic. After log compaction only the latest record per
		// topic key survives; omitting partitions owned by other instances would
		// cause their state to disappear from cold-start replays, which in turn
		// triggers accidental timestamp resets for those partitions on every scan.
		if globalTopic, ok := previousGlobalSnapshot.Topics[meta.name]; ok {
			for partID, partInfo := range globalTopic.Partitions {
				if _, owned := topicStatus.Partitions[partID]; !owned {
					topicStatus.Partitions[partID] = partInfo
					if !topicStatus.RetentionPolicy.IsCompacted() {
						topicStatus.TotalMessageCount += partInfo.MessageCount
					}
				}
			}
		}

		// For compacted topics message counts are meaningless (compaction removes
		// records), so we use -1 as a sentinel that the API and UI render as N/A.
		if topicStatus.RetentionPolicy.IsCompacted() {
			topicStatus.TotalMessageCount = -1
			for _, p := range topicStatus.Partitions {
				p.MessageCount = -1
			}
		}

		// A topic is empty when every partition reports earliest == latest offset.
		// Non-owned partitions carried over from the global snapshot preserve their
		// IsEmpty value, so the computation is correct in multi-instance mode too.
		topicStatus.IsEmpty = len(topicStatus.Partitions) > 0
		for _, p := range topicStatus.Partitions {
			if !p.IsEmpty {
				topicStatus.IsEmpty = false
				break
			}
		}

		snapshot.Topics[meta.name] = topicStatus
		processedTopics++
	}

	if err := t.stateManager.SaveSnapshot(ctx, snapshot); err != nil {
		logging.Warn("Failed to save snapshot: %v", err)
	}

	// Emit tombstones for topics that existed in the previous snapshot but are
	// no longer present in Kafka. This keeps the compacted tracker topic clean
	// so that deleted topics do not reappear when the tracker restarts.
	for topicName := range previousGlobalSnapshot.Topics {
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
