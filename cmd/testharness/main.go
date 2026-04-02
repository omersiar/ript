package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/omersiar/ript/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type options struct {
	brokers           []string
	topicPrefix       string
	topicCount        int
	partitions        int
	replicationFactor int
	createWorkers     int

	virtualDays        int
	inactiveTopicRatio float64
	eventsPerPartMin   int
	eventsPerPartMax   int
	headerKey          string
	seed               int64

	apiURL        string
	thresholdDays int
	assertTimeout time.Duration
	assertPoll    time.Duration
	skipAssert    bool
}

type partitionPlan struct {
	topic      string
	partition  int32
	timestamps []time.Time
}

type topicExpectation struct {
	newestPartitionTS time.Time
	isUnused          bool
}

type statsResponse struct {
	TotalTopics     int `json:"total_topics"`
	UnusedTopics    int `json:"unused_topics"`
	StalePartitions int `json:"stale_partitions"`
}

func main() {
	opts, err := parseFlags()
	if err != nil {
		fatalf("invalid flags: %v", err)
	}

	randSrc := rand.New(rand.NewSource(opts.seed))

	ctx := context.Background()
	kafkaClient, err := kafka.NewClient(opts.brokers)
	if err != nil {
		fatalf("failed to create kafka client: %v", err)
	}
	defer kafkaClient.Close()

	fmt.Printf("[harness] creating %d topics (%d partitions each) with %d workers\n", opts.topicCount, opts.partitions, opts.createWorkers)
	if err := createTopics(ctx, kafkaClient, opts); err != nil {
		fatalf("topic creation failed: %v", err)
	}

	plans, expectedUnused, err := buildSimulationPlans(opts, randSrc)
	if err != nil {
		fatalf("failed to build simulation plans: %v", err)
	}
	fmt.Printf("[harness] generated simulation plan: %d partitions, expected unused topics at threshold %d days = %d\n", len(plans), opts.thresholdDays, expectedUnused)

	if err := producePlans(ctx, opts, plans); err != nil {
		fatalf("failed while producing simulation events: %v", err)
	}

	fmt.Println("[harness] production completed")

	if opts.skipAssert {
		fmt.Println("[harness] skipping API assertion (--skip-assert)")
		return
	}

	fmt.Println("[harness] waiting for tracker scan and validating API counts")
	if err := assertTrackerCounts(opts, expectedUnused); err != nil {
		fatalf("assertion failed: %v", err)
	}

	fmt.Println("[harness] success: tracker counts match expected compressed-time simulation")
}

func parseFlags() (*options, error) {
	var brokers string
	opts := &options{}

	flag.StringVar(&brokers, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&opts.topicPrefix, "topic-prefix", "harness-topic", "prefix for generated topic names")
	flag.IntVar(&opts.topicCount, "topics", 10000, "number of topics to create")
	flag.IntVar(&opts.partitions, "partitions", 4, "partitions per topic")
	flag.IntVar(&opts.replicationFactor, "replication-factor", 1, "topic replication factor")
	flag.IntVar(&opts.createWorkers, "create-workers", 32, "parallel workers for topic creation")

	flag.IntVar(&opts.virtualDays, "virtual-days", 180, "historical window to simulate in virtual days")
	flag.Float64Var(&opts.inactiveTopicRatio, "inactive-ratio", 0.25, "ratio of topics that should end as unused")
	flag.IntVar(&opts.eventsPerPartMin, "events-per-partition-min", 2, "minimum number of events generated per partition")
	flag.IntVar(&opts.eventsPerPartMax, "events-per-partition-max", 5, "maximum number of events generated per partition")
	flag.StringVar(&opts.headerKey, "header-key", "x-event-time-ms", "message header key carrying event timestamp")
	flag.Int64Var(&opts.seed, "seed", 20260328, "random seed for deterministic runs")

	flag.StringVar(&opts.apiURL, "api-url", "http://localhost:8080", "tracker API base URL")
	flag.IntVar(&opts.thresholdDays, "threshold-days", 30, "unused threshold days to compare against")
	flag.DurationVar(&opts.assertTimeout, "assert-timeout", 5*time.Minute, "maximum wait for tracker to surface expected counts")
	flag.DurationVar(&opts.assertPoll, "assert-poll-interval", 5*time.Second, "poll interval for stats assertion")
	flag.BoolVar(&opts.skipAssert, "skip-assert", false, "skip tracker API assertion")

	flag.Parse()

	opts.brokers = splitAndTrim(brokers)
	if len(opts.brokers) == 0 {
		return nil, errors.New("brokers cannot be empty")
	}
	if opts.topicCount < 1 {
		return nil, errors.New("topics must be at least 1")
	}
	if opts.partitions < 1 {
		return nil, errors.New("partitions must be at least 1")
	}
	if opts.replicationFactor < 1 {
		return nil, errors.New("replication-factor must be at least 1")
	}
	if opts.createWorkers < 1 {
		return nil, errors.New("create-workers must be at least 1")
	}
	if opts.virtualDays < 1 {
		return nil, errors.New("virtual-days must be at least 1")
	}
	if opts.inactiveTopicRatio < 0 || opts.inactiveTopicRatio > 1 {
		return nil, errors.New("inactive-ratio must be between 0 and 1")
	}
	if opts.eventsPerPartMin < 1 || opts.eventsPerPartMax < opts.eventsPerPartMin {
		return nil, errors.New("events-per-partition bounds are invalid")
	}
	if strings.TrimSpace(opts.headerKey) == "" {
		return nil, errors.New("header-key cannot be empty")
	}
	if opts.thresholdDays < 1 {
		return nil, errors.New("threshold-days must be at least 1")
	}
	if opts.assertTimeout < time.Second {
		return nil, errors.New("assert-timeout must be at least 1s")
	}
	if opts.assertPoll < time.Second {
		return nil, errors.New("assert-poll-interval must be at least 1s")
	}

	return opts, nil
}

func createTopics(ctx context.Context, client *kafka.Client, opts *options) error {
	topics := make([]string, 0, opts.topicCount)
	for i := 0; i < opts.topicCount; i++ {
		topics = append(topics, topicName(opts.topicPrefix, i))
	}

	batchSize := opts.createWorkers * 64
	if batchSize < 100 {
		batchSize = 100
	}

	for start := 0; start < len(topics); start += batchSize {
		end := start + batchSize
		if end > len(topics) {
			end = len(topics)
		}

		batch := topics[start:end]
		var lastErr error
		for attempt := 1; attempt <= 5; attempt++ {
			lastErr = client.CreateTopicsIfNotExist(ctx, batch, int32(opts.partitions), int16(opts.replicationFactor), map[string]string{})
			if lastErr == nil {
				break
			}
			backoff := time.Duration(attempt*attempt) * time.Second
			fmt.Printf("[harness] create topics batch %d-%d attempt %d failed: %v, retrying in %s\n", start, end-1, attempt, lastErr, backoff)
			time.Sleep(backoff)
		}
		if lastErr != nil {
			return fmt.Errorf("create topics batch failed (%d-%d) after retries: %w", start, end-1, lastErr)
		}

		fmt.Printf("[harness] created topics: %d/%d\n", end, opts.topicCount)
	}

	return nil
}

func buildSimulationPlans(opts *options, randSrc *rand.Rand) ([]partitionPlan, int, error) {
	now := time.Now().UTC()
	windowStart := now.Add(-time.Duration(opts.virtualDays) * 24 * time.Hour)
	activeEndLowerBound := now.Add(-14 * 24 * time.Hour)
	inactiveEndUpperBound := now.Add(-35 * 24 * time.Hour)

	inactiveCount := int(math.Round(float64(opts.topicCount) * opts.inactiveTopicRatio))
	if inactiveCount > opts.topicCount {
		inactiveCount = opts.topicCount
	}

	plans := make([]partitionPlan, 0, opts.topicCount*opts.partitions)
	expectedUnused := 0

	for topicIdx := 0; topicIdx < opts.topicCount; topicIdx++ {
		isInactive := topicIdx < inactiveCount
		topic := topicName(opts.topicPrefix, topicIdx)
		meta := topicExpectation{}

		for partition := 0; partition < opts.partitions; partition++ {
			events := opts.eventsPerPartMin
			if opts.eventsPerPartMax > opts.eventsPerPartMin {
				events += randSrc.Intn(opts.eventsPerPartMax - opts.eventsPerPartMin + 1)
			}

			timestamps := make([]time.Time, 0, events)
			for eventIdx := 0; eventIdx < events; eventIdx++ {
				var ts time.Time
				switch {
				case isInactive:
					ts = sampleWeightedTimestamp(randSrc, windowStart, inactiveEndUpperBound)
				case eventIdx == events-1:
					if activeEndLowerBound.After(now) {
						activeEndLowerBound = windowStart
					}
					ts = sampleWeightedTimestamp(randSrc, activeEndLowerBound, now)
				default:
					ts = sampleWeightedTimestamp(randSrc, windowStart, now)
				}
				timestamps = append(timestamps, ts)
			}

			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i].Before(timestamps[j])
			})

			lastTS := timestamps[len(timestamps)-1]
			if meta.newestPartitionTS.IsZero() || lastTS.After(meta.newestPartitionTS) {
				meta.newestPartitionTS = lastTS
			}

			plans = append(plans, partitionPlan{
				topic:      topic,
				partition:  int32(partition),
				timestamps: timestamps,
			})
		}

		meta.isUnused = now.Sub(meta.newestPartitionTS) >= time.Duration(opts.thresholdDays)*24*time.Hour
		if meta.isUnused {
			expectedUnused++
		}
	}

	return plans, expectedUnused, nil
}

func sampleWeightedTimestamp(randSrc *rand.Rand, start, end time.Time) time.Time {
	if !end.After(start) {
		return start.UTC()
	}
	span := end.Sub(start)

	for {
		candidate := start.Add(time.Duration(randSrc.Int63n(int64(span))))
		if randSrc.Float64()*2.5 <= trafficWeight(candidate) {
			return candidate.UTC()
		}
	}
}

func trafficWeight(ts time.Time) float64 {
	weight := 1.0

	hour := ts.Hour()
	if hour >= 8 && hour <= 19 {
		weight *= 1.8
	} else if hour <= 5 {
		weight *= 0.25
	} else {
		weight *= 0.7
	}

	switch ts.Weekday() {
	case time.Saturday, time.Sunday:
		weight *= 0.35
	default:
		weight *= 1.3
	}

	if ts.Day()%14 == 0 && hour >= 10 && hour <= 12 {
		weight *= 1.25
	}

	return weight
}

func producePlans(ctx context.Context, opts *options, plans []partitionPlan) error {
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(opts.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordRetries(10),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
			return backoff
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create producer client: %w", err)
	}
	defer producer.Close()

	batch := make([]*kgo.Record, 0, 2000)
	var totalEvents int64
	for _, plan := range plans {
		for _, ts := range plan.timestamps {
			rec := &kgo.Record{
				Topic:     plan.topic,
				Partition: plan.partition,
				Value:     []byte("load-event"),
				Timestamp: ts,
				Headers: []kgo.RecordHeader{
					{Key: opts.headerKey, Value: []byte(strconv.FormatInt(ts.UnixMilli(), 10))},
				},
			}
			batch = append(batch, rec)
			totalEvents++
			if len(batch) >= 2000 {
				if err := produceBatchWithRetry(ctx, producer, batch); err != nil {
					return err
				}
				batch = batch[:0]
				if totalEvents%10000 == 0 {
					fmt.Printf("[harness] produced events: %d\n", totalEvents)
				}
			}
		}
	}

	if len(batch) > 0 {
		if err := produceBatchWithRetry(ctx, producer, batch); err != nil {
			return err
		}
	}

	fmt.Printf("[harness] total events produced: %d\n", totalEvents)
	return nil
}

func produceBatchWithRetry(ctx context.Context, producer *kgo.Client, batch []*kgo.Record) error {
	for attempt := 1; attempt <= 5; attempt++ {
		produceCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := producer.ProduceSync(produceCtx, batch...).FirstErr()
		cancel()
		if err == nil {
			return nil
		}
		if attempt == 5 {
			return fmt.Errorf("produce batch failed after %d attempts: %w", attempt, err)
		}
		backoff := time.Duration(attempt*attempt) * time.Second
		fmt.Printf("[harness] produce batch attempt %d failed: %v, retrying in %s\n", attempt, err, backoff)
		time.Sleep(backoff)
	}
	return nil
}

func assertTrackerCounts(opts *options, expectedUnused int) error {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(opts.assertTimeout)
	for {
		stats, err := fetchStats(httpClient, opts.apiURL, opts.thresholdDays)
		if err == nil {
			fmt.Printf("[harness] tracker stats: total_topics=%d unused_topics=%d stale_partitions=%d\n", stats.TotalTopics, stats.UnusedTopics, stats.StalePartitions)
			if stats.TotalTopics >= opts.topicCount && stats.UnusedTopics == expectedUnused {
				unusedCount, err := fetchUnusedCount(httpClient, opts.apiURL, opts.thresholdDays)
				if err != nil {
					return err
				}
				if unusedCount != expectedUnused {
					return fmt.Errorf("unused endpoint count mismatch: got=%d want=%d", unusedCount, expectedUnused)
				}
				return nil
			}
		}

		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("timed out waiting for tracker stats: %w", err)
			}
			return fmt.Errorf("timed out waiting for expected counts: got topics=%d unused=%d want topics>=%d unused=%d", stats.TotalTopics, stats.UnusedTopics, opts.topicCount, expectedUnused)
		}

		time.Sleep(opts.assertPoll)
	}
}

func fetchStats(httpClient *http.Client, apiURL string, thresholdDays int) (*statsResponse, error) {
	endpoint, err := url.JoinPath(apiURL, "/api/stats")
	if err != nil {
		return nil, fmt.Errorf("build stats endpoint: %w", err)
	}
	reqURL := fmt.Sprintf("%s?threshold_days=%d", endpoint, thresholdDays)
	resp, err := httpClient.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("request stats endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats endpoint returned status %d", resp.StatusCode)
	}

	var stats statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("decode stats response: %w", err)
	}
	return &stats, nil
}

func fetchUnusedCount(httpClient *http.Client, apiURL string, thresholdDays int) (int, error) {
	endpoint, err := url.JoinPath(apiURL, "/api/unused")
	if err != nil {
		return 0, fmt.Errorf("build unused endpoint: %w", err)
	}

	reqURL := fmt.Sprintf("%s?threshold_days=%d", endpoint, thresholdDays)
	resp, err := httpClient.Get(reqURL)
	if err != nil {
		return 0, fmt.Errorf("request unused endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unused endpoint returned status %d", resp.StatusCode)
	}

	var payload struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, fmt.Errorf("decode unused response: %w", err)
	}
	return payload.Count, nil
}

func topicName(prefix string, idx int) string {
	return fmt.Sprintf("%s-%05d", prefix, idx)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[harness] ERROR: "+format+"\n", args...)
	os.Exit(1)
}
