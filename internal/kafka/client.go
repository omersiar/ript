package kafka

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Client struct {
	client *kgo.Client
	config *ClientConfig
	idSeq  uint64
}

type HeaderLookupTargets map[string]map[int32]int64

// GetLatestTimestampsFromHeadersBatch reads specific record offsets across
// topics/partitions in a single consumer pass and extracts header timestamps.
func (c *Client) GetLatestTimestampsFromHeadersBatch(ctx context.Context, targets HeaderLookupTargets, headerKey string) (map[string]map[int32]time.Time, error) {
	if len(targets) == 0 {
		return map[string]map[int32]time.Time{}, nil
	}

	lookupStartedAt := time.Now()

	consumeMap := make(map[string]map[int32]kgo.Offset, len(targets))
	targetOffsetsByTopic := make(map[string]map[int32]int64, len(targets))
	totalTargets := 0
	for topic, partitions := range targets {
		for partition, targetOffset := range partitions {
			if targetOffset < 0 {
				continue
			}
			if _, ok := consumeMap[topic]; !ok {
				consumeMap[topic] = make(map[int32]kgo.Offset)
				targetOffsetsByTopic[topic] = make(map[int32]int64)
			}
			consumeMap[topic][partition] = kgo.NewOffset().At(targetOffset)
			targetOffsetsByTopic[topic][partition] = targetOffset
			totalTargets++
		}
	}

	if totalTargets == 0 {
		return map[string]map[int32]time.Time{}, nil
	}

	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(c.config.Brokers...),
		kgo.ClientID(c.nextClientID("header-lookup")),
		kgo.ConsumePartitions(consumeMap),
		kgo.FetchMaxBytes(10 * 1024 * 1024),
		kgo.FetchMaxWait(250 * time.Millisecond),
		// Disable incremental fetch sessions: this is a short-lived client that
		// reads a handful of offsets and closes. Without this, the broker's
		// session cache receives zero-UUID "forget" entries on Close() which
		// trigger a NullPointerException in Kafka 2.8.x (FetchSession.scala:184).
		kgo.DisableFetchSessions(),
	}
	consumerOpts = append(consumerOpts, c.config.AuthOpts...)
	consumerClient, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create header lookup consumer client: %w", err)
	}
	defer consumerClient.Close()

	timestamps := make(map[string]map[int32]time.Time, len(targetOffsetsByTopic))
	for topic := range targetOffsetsByTopic {
		timestamps[topic] = make(map[int32]time.Time)
	}

	pollCycles := 0
	resolved := 0
	for resolved < totalTargets {
		pollCycles++
		fetches := consumerClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		if err := ctx.Err(); err != nil {
			break
		}

		for _, fe := range fetches.Errors() {
			if fe.Err != nil && fe.Err != context.Canceled && fe.Err != context.DeadlineExceeded {
				return nil, fmt.Errorf("failed to fetch header records for %s partition %d: %w", fe.Topic, fe.Partition, fe.Err)
			}
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			topicTargets, ok := targetOffsetsByTopic[p.Topic]
			if !ok {
				return
			}

			targetOffset, ok := topicTargets[p.Partition]
			if !ok {
				return
			}

			p.EachRecord(func(record *kgo.Record) {
				if _, exists := timestamps[p.Topic][p.Partition]; exists {
					return
				}
				if record.Offset != targetOffset {
					return
				}

				ts, ok := timestampFromHeaders(record.Headers, headerKey)
				if ok {
					timestamps[p.Topic][p.Partition] = ts
					resolved++
				}
			})
		})
	}

	totalResolved := 0
	for _, topicTimestamps := range timestamps {
		totalResolved += len(topicTimestamps)
	}

	logging.Info("Header timestamp lookup batch completed in %s: target_topics=%d target_partitions=%d resolved=%d poll_cycles=%d",
		time.Since(lookupStartedAt), len(targetOffsetsByTopic), totalTargets, totalResolved, pollCycles)

	return timestamps, nil
}

type ClientConfig struct {
	Brokers  []string
	ClientID string
	AuthOpts []kgo.Opt
}

func NewClient(brokers []string) (*Client, error) {
	return NewClientWithConfig(ClientConfig{Brokers: brokers})
}

func NewClientWithConfig(cfg ClientConfig) (*Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
	}
	if strings.TrimSpace(cfg.ClientID) != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	opts = append(opts, cfg.AuthOpts...)

	// Enable franz-go debug logging if FRANZ_GO_DEBUG is set
	if os.Getenv("FRANZ_GO_DEBUG") != "" {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	c := &Client{
		client: client,
		config: &ClientConfig{
			Brokers:  cfg.Brokers,
			ClientID: cfg.ClientID,
			AuthOpts: cfg.AuthOpts,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to Kafka brokers: %w", err)
	}

	return c, nil
}

func (c *Client) nextClientID(role string) string {
	seq := atomic.AddUint64(&c.idSeq, 1)
	base := strings.TrimSpace(c.config.ClientID)
	if base == "" {
		base = "ript"
	}
	if strings.TrimSpace(role) == "" {
		return fmt.Sprintf("%s-%d", base, seq)
	}
	return fmt.Sprintf("%s-%s-%d", base, role, seq)
}

func (c *Client) NextClientID(role string) string {
	return c.nextClientID(role)
}

func (c *Client) Close() error {
	c.client.Flush(context.Background())
	c.client.Close()
	return nil
}

func (c *Client) ListTopics(ctx context.Context) ([]string, error) {
	req := &kmsg.MetadataRequest{}
	resp, err := c.client.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	metaResp := resp.(*kmsg.MetadataResponse)
	var topics []string

	for _, topic := range metaResp.Topics {
		topicName := *topic.Topic
		if !isSystemTopic(topicName) {
			topics = append(topics, topicName)
		}
	}

	return topics, nil
}

// ListTopicsWithPartitions fetches metadata for all topics in a single request
// and returns a map of topic name to its partition IDs. System topics are excluded.
// This replaces the previous pattern of calling ListTopics followed by one
// GetTopicPartitions call per topic, reducing 1+N requests to exactly 1.
func (c *Client) ListTopicsWithPartitions(ctx context.Context) (map[string][]int32, error) {
	req := &kmsg.MetadataRequest{}
	resp, err := c.client.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	metaResp := resp.(*kmsg.MetadataResponse)
	result := make(map[string][]int32, len(metaResp.Topics))

	for _, topic := range metaResp.Topics {
		topicName := *topic.Topic
		if isSystemTopic(topicName) {
			continue
		}
		partitions := make([]int32, 0, len(topic.Partitions))
		for _, p := range topic.Partitions {
			partitions = append(partitions, p.Partition)
		}
		result[topicName] = partitions
	}

	return result, nil
}

// GetHighWatermarksBatch fetches the high watermark (log-end offset) for all
// partitions of all provided topics in a single sharded request. Franz-go
// automatically splits the ListOffsetsRequest by partition leader and sends
// one sub-request per broker, so this is always 1 logical call regardless of
// topic or broker count.
//
// Broker-level failures are returned as errors. Per-partition error codes are
// logged as warnings and those partitions are omitted from the result, consistent
// with the per-topic warn-and-continue behaviour of GetHighWatermarks.
func (c *Client) GetHighWatermarksBatch(ctx context.Context, topics map[string][]int32) (map[string]map[int32]int64, error) {
	if len(topics) == 0 {
		return map[string]map[int32]int64{}, nil
	}

	reqTopics := make([]kmsg.ListOffsetsRequestTopic, 0, len(topics))
	for topicName, partitions := range topics {
		partReqs := make([]kmsg.ListOffsetsRequestTopicPartition, len(partitions))
		for i, p := range partitions {
			partReqs[i] = newLatestOffsetPartitionRequest(p)
		}
		reqTopics = append(reqTopics, kmsg.ListOffsetsRequestTopic{
			Topic:      topicName,
			Partitions: partReqs,
		})
	}

	req := &kmsg.ListOffsetsRequest{Topics: reqTopics}
	shards := c.client.RequestSharded(ctx, req)

	result := make(map[string]map[int32]int64, len(topics))
	for _, shard := range shards {
		if shard.Err != nil {
			return nil, fmt.Errorf("failed to list offsets from broker %s:%d: %w", shard.Meta.Host, shard.Meta.Port, shard.Err)
		}
		offsetResp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, tr := range offsetResp.Topics {
			if result[tr.Topic] == nil {
				result[tr.Topic] = make(map[int32]int64)
			}
			for _, pr := range tr.Partitions {
				if pr.ErrorCode != 0 {
					logging.Warn("Error getting high watermark for %s partition %d: %v (error code %d)", tr.Topic, pr.Partition, kerr.ErrorForCode(pr.ErrorCode), pr.ErrorCode)
					continue
				}
				result[tr.Topic][pr.Partition] = pr.Offset
			}
		}
	}
	return result, nil
}

func (c *Client) GetTopicPartitions(ctx context.Context, topic string) ([]int32, error) {
	req := &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{
			{Topic: &topic},
		},
	}
	resp, err := c.client.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	metaResp := resp.(*kmsg.MetadataResponse)
	if len(metaResp.Topics) == 0 {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	topicMeta := metaResp.Topics[0]
	var partitions []int32
	for _, partition := range topicMeta.Partitions {
		partitions = append(partitions, partition.Partition)
	}

	return partitions, nil
}

// GetTopicConfigs returns config key/value pairs for a topic. If keys is empty,
// all topic configs are returned.
func (c *Client) GetTopicConfigs(ctx context.Context, topic string, keys []string) (map[string]string, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.Resources = []kmsg.DescribeConfigsRequestResource{
		{
			ResourceType: kmsg.ConfigResourceTypeTopic,
			ResourceName: topic,
			ConfigNames:  keys,
		},
	}

	resp, err := c.client.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to describe configs for topic %s: %w", topic, err)
	}

	describeResp := resp.(*kmsg.DescribeConfigsResponse)
	if len(describeResp.Resources) == 0 {
		return nil, fmt.Errorf("describe configs returned no resources for topic %s", topic)
	}

	resource := describeResp.Resources[0]
	if resource.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to describe configs for topic %s: %v (error code %d)", topic, kerr.ErrorForCode(resource.ErrorCode), resource.ErrorCode)
	}

	configs := make(map[string]string, len(resource.Configs))
	for _, cfg := range resource.Configs {
		if cfg.Value == nil {
			continue
		}
		configs[cfg.Name] = *cfg.Value
	}

	return configs, nil
}

// CreateTopicsIfNotExist issues one create-topics request for the provided
// topics and waits for metadata to confirm they are available. Kafka topic
// creation is asynchronous, so non-zero response codes such as
// REQUEST_TIMED_OUT can still result in a successfully created topic shortly
// after the request returns.
func (c *Client) CreateTopicsIfNotExist(ctx context.Context, topics []string, partitions int32, replicationFactor int16, configs map[string]string) error {
	if len(topics) == 0 {
		return nil
	}

	uniqueTopics := make([]string, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		uniqueTopics = append(uniqueTopics, topic)
	}

	availability, err := c.topicsExistByMetadata(ctx, uniqueTopics)
	if err != nil {
		return fmt.Errorf("failed to check topic metadata before create: %w", err)
	}

	toCreate := make([]string, 0, len(uniqueTopics))
	for _, topic := range uniqueTopics {
		if !availability[topic] {
			toCreate = append(toCreate, topic)
		}
	}
	if len(toCreate) == 0 {
		return nil
	}

	topicCfgs := make([]kmsg.CreateTopicsRequestTopicConfig, 0, len(configs))
	for k, v := range configs {
		val := v
		topicCfgs = append(topicCfgs, kmsg.CreateTopicsRequestTopicConfig{
			Name:  k,
			Value: &val,
		})
	}

	reqTopics := make([]kmsg.CreateTopicsRequestTopic, 0, len(toCreate))
	for _, topic := range toCreate {
		reqTopics = append(reqTopics, kmsg.CreateTopicsRequestTopic{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
			Configs:           topicCfgs,
		})
	}

	req := &kmsg.CreateTopicsRequest{Topics: reqTopics}
	resp, err := c.client.Request(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create topics: %w", err)
	}

	createResp := resp.(*kmsg.CreateTopicsResponse)
	createResultByTopic := make(map[string]kmsg.CreateTopicsResponseTopic, len(createResp.Topics))
	for _, topicResp := range createResp.Topics {
		createResultByTopic[topicResp.Topic] = topicResp
	}

	pendingTopics := make([]string, 0, len(toCreate))
	for _, topic := range toCreate {
		topicResp, ok := createResultByTopic[topic]
		if !ok {
			pendingTopics = append(pendingTopics, topic)
			continue
		}

		if topicResp.ErrorCode == int16(kerr.TopicAlreadyExists.Code) {
			continue
		}

		if topicResp.ErrorCode == int16(kerr.LeaderNotAvailable.Code) {
			logging.Info("CreateTopics: topic %s returned LEADER_NOT_AVAILABLE (transient, will poll metadata)", topic)
		}

		pendingTopics = append(pendingTopics, topic)
	}
	if len(pendingTopics) == 0 {
		return nil
	}

	deadline := time.Now().Add(30 * time.Second)
	pollInterval := 500 * time.Millisecond
	for {
		availability, err = c.topicsExistByMetadata(ctx, pendingTopics)
		if err == nil {
			missing := make([]string, 0)
			for _, topic := range pendingTopics {
				if !availability[topic] {
					missing = append(missing, topic)
				}
			}
			if len(missing) == 0 {
				nonZeroConfirmed := make([]string, 0)
				for _, topic := range pendingTopics {
					topicResp, ok := createResultByTopic[topic]
					if !ok || topicResp.ErrorCode == 0 {
						continue
					}
					nonZeroConfirmed = append(nonZeroConfirmed, fmt.Sprintf("%s(code=%d)", topic, topicResp.ErrorCode))
				}
				if len(nonZeroConfirmed) > 0 {
					logSample := nonZeroConfirmed
					if len(logSample) > 10 {
						logSample = logSample[:10]
					}
					logging.Info("CreateTopics returned non-zero codes for %d topic(s), but metadata confirms availability; sample=[%s]",
						len(nonZeroConfirmed), strings.Join(logSample, ", "))
				}
				return nil
			}
		}

		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("timed out waiting for created topics metadata: %w", err)
			}

			missing := make([]string, 0)
			for _, topic := range pendingTopics {
				if !availability[topic] {
					missing = append(missing, topic)
				}
			}

			resultSummaries := make([]string, 0, len(missing))
			for _, topic := range missing {
				topicResp, ok := createResultByTopic[topic]
				if !ok {
					resultSummaries = append(resultSummaries, fmt.Sprintf("%s=no-create-response", topic))
					continue
				}
				respErr := kerr.ErrorForCode(topicResp.ErrorCode)
				resultSummaries = append(resultSummaries, fmt.Sprintf("%s=code:%d err:%v msg:%s", topic, topicResp.ErrorCode, respErr, derefErrMsg(topicResp.ErrorMessage)))
			}

			return fmt.Errorf("timed out waiting for created topics to appear in metadata; missing=%d/%d details=[%s]",
				len(missing), len(pendingTopics), strings.Join(resultSummaries, ", "))
		}

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

func derefErrMsg(msg *string) string {
	if msg == nil {
		return ""
	}
	return *msg
}

// topicsExistByMetadata returns topic existence map based on metadata lookup.
// A topic is considered available when it does not return UNKNOWN_TOPIC and
// has at least one partition in metadata.
func (c *Client) topicsExistByMetadata(ctx context.Context, topics []string) (map[string]bool, error) {
	availability := make(map[string]bool, len(topics))
	if len(topics) == 0 {
		return availability, nil
	}

	metaTopics := make([]kmsg.MetadataRequestTopic, 0, len(topics))
	for _, topic := range topics {
		topicName := topic
		availability[topicName] = false
		metaTopics = append(metaTopics, kmsg.MetadataRequestTopic{Topic: &topicName})
	}

	metaReq := &kmsg.MetadataRequest{
		Topics: metaTopics,
	}
	metaResp, err := c.client.Request(ctx, metaReq)
	if err != nil {
		return nil, fmt.Errorf("failed to probe topic metadata: %w", err)
	}
	metadata := metaResp.(*kmsg.MetadataResponse)
	for _, topicMeta := range metadata.Topics {
		if topicMeta.Topic == nil {
			continue
		}
		topicName := *topicMeta.Topic
		if _, ok := availability[topicName]; !ok {
			continue
		}
		availability[topicName] = topicMeta.ErrorCode != int16(kerr.UnknownTopicOrPartition.Code) && len(topicMeta.Partitions) > 0
	}

	return availability, nil
}

// GetHighWatermarks fetches the high watermark (log-end offset) for all specified
// partitions of a topic in a single request. Efficient for multi-partition topics.
func (c *Client) GetHighWatermarks(ctx context.Context, topic string, partitions []int32) (map[int32]int64, error) {
	partReqs := make([]kmsg.ListOffsetsRequestTopicPartition, len(partitions))
	for i, p := range partitions {
		partReqs[i] = newLatestOffsetPartitionRequest(p)
	}

	req := &kmsg.ListOffsetsRequest{
		Topics: []kmsg.ListOffsetsRequestTopic{
			{Topic: topic, Partitions: partReqs},
		},
	}

	resp, err := c.client.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list offsets for %s: %w", topic, err)
	}

	offsetResp := resp.(*kmsg.ListOffsetsResponse)
	result := make(map[int32]int64, len(partitions))
	for _, tr := range offsetResp.Topics {
		for _, pr := range tr.Partitions {
			if pr.ErrorCode != 0 {
				return nil, fmt.Errorf("error getting high watermark for %s partition %d: %w (error code %d)", topic, pr.Partition, kerr.ErrorForCode(pr.ErrorCode), pr.ErrorCode)
			}
			result[pr.Partition] = pr.Offset
		}
	}
	return result, nil
}

// newLatestOffsetPartitionRequest creates a ListOffsets partition request using
// franz-go defaults (notably CurrentLeaderEpoch=-1) and asks for latest offset.
func newLatestOffsetPartitionRequest(partition int32) kmsg.ListOffsetsRequestTopicPartition {
	partReq := kmsg.NewListOffsetsRequestTopicPartition()
	partReq.Partition = partition
	partReq.Timestamp = -1 // -1 = latest (high watermark)
	return partReq
}

func isSystemTopic(topic string) bool {
	if len(topic) == 0 {
		return false
	}
	return topic[0] == '_'
}

func timestampFromHeaders(headers []kgo.RecordHeader, headerKey string) (time.Time, bool) {
	for _, h := range headers {
		if h.Key != headerKey {
			continue
		}

		raw := strings.TrimSpace(string(h.Value))
		if raw == "" {
			return time.Time{}, false
		}

		if ts, err := strconv.ParseInt(raw, 10, 64); err == nil {
			if ts > 100000000000 {
				return time.UnixMilli(ts).UTC(), true
			}
			return time.Unix(ts, 0).UTC(), true
		}

		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return parsed.UTC(), true
		}

		return time.Time{}, false
	}

	return time.Time{}, false
}
