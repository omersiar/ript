package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
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
	seed               int64
}

type partitionPlan struct {
	topic      string
	partition  int32
	timestamps []time.Time
}

var realisticTopicDomains = []string{
	"commerce",
	"billing",
	"identity",
	"catalog",
	"inventory",
	"fulfillment",
	"analytics",
	"notifications",
	"fraud",
	"support",
}

var realisticTopicEntities = []string{
	"orders",
	"payments",
	"customers",
	"products",
	"shipments",
	"invoices",
	"sessions",
	"returns",
	"accounts",
	"tickets",
}

var realisticTopicFeeds = []string{
	"events",
	"cdc",
	"snapshots",
	"commands",
	"audit",
	"retries",
}

var realisticTopicEnvironments = []string{
	"prod",
	"staging",
	"dev",
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

	plans := buildPlans(opts, randSrc)
	fmt.Printf("[harness] generated plan: %d partition plans\n", len(plans))

	if err := producePlans(ctx, opts, plans); err != nil {
		fatalf("failed while producing events: %v", err)
	}

	fmt.Println("[harness] done")
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

	flag.IntVar(&opts.virtualDays, "virtual-days", 180, "historical window to simulate in days")
	flag.Float64Var(&opts.inactiveTopicRatio, "inactive-ratio", 0.25, "ratio of topics that should appear inactive")
	flag.IntVar(&opts.eventsPerPartMin, "events-per-partition-min", 2, "minimum events per partition")
	flag.IntVar(&opts.eventsPerPartMax, "events-per-partition-max", 5, "maximum events per partition")
	flag.Int64Var(&opts.seed, "seed", 20260328, "random seed for deterministic runs")

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

	return opts, nil
}

func createTopics(ctx context.Context, client *kafka.Client, opts *options) error {
	topics := make([]string, 0, opts.topicCount)
	for i := 0; i < opts.topicCount; i++ {
		topics = append(topics, topicName(opts.topicPrefix, i))
	}

	const maxPartitionsPerCreateRequest = 7500
	maxTopicsByPartitions := maxPartitionsPerCreateRequest / opts.partitions
	if maxTopicsByPartitions < 1 {
		return fmt.Errorf("partitions per topic (%d) exceed broker per-request limit of %d", opts.partitions, maxPartitionsPerCreateRequest)
	}

	batchSize := opts.createWorkers * 64
	if batchSize < 100 {
		batchSize = 100
	}
	if batchSize > maxTopicsByPartitions {
		batchSize = maxTopicsByPartitions
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

func buildPlans(opts *options, randSrc *rand.Rand) []partitionPlan {
	now := time.Now().UTC()
	windowStart := now.Add(-time.Duration(opts.virtualDays) * 24 * time.Hour)
	activeEndLowerBound := now.Add(-14 * 24 * time.Hour)
	inactiveEndUpperBound := now.Add(-35 * 24 * time.Hour)

	inactiveCount := int(math.Round(float64(opts.topicCount) * opts.inactiveTopicRatio))
	if inactiveCount > opts.topicCount {
		inactiveCount = opts.topicCount
	}

	plans := make([]partitionPlan, 0, opts.topicCount*opts.partitions)

	for topicIdx := 0; topicIdx < opts.topicCount; topicIdx++ {
		isInactive := topicIdx < inactiveCount
		topic := topicName(opts.topicPrefix, topicIdx)

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

			plans = append(plans, partitionPlan{
				topic:      topic,
				partition:  int32(partition),
				timestamps: timestamps,
			})
		}
	}

	return plans
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
				Value:     []byte(strconv.FormatInt(ts.Unix(), 10)),
				Timestamp: ts,
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

func topicName(prefix string, idx int) string {
	normalizedPrefix := normalizeTopicPrefix(prefix)
	domain := realisticTopicDomains[idx%len(realisticTopicDomains)]
	entity := realisticTopicEntities[(idx/len(realisticTopicDomains))%len(realisticTopicEntities)]
	feed := realisticTopicFeeds[(idx/(len(realisticTopicDomains)*len(realisticTopicEntities)))%len(realisticTopicFeeds)]
	environment := realisticTopicEnvironments[(idx/(len(realisticTopicDomains)*len(realisticTopicEntities)*len(realisticTopicFeeds)))%len(realisticTopicEnvironments)]
	version := (idx % 3) + 1

	parts := []string{domain, entity, feed, environment, fmt.Sprintf("v%d", version), fmt.Sprintf("%05d", idx)}
	if normalizedPrefix != "" {
		parts = append([]string{normalizedPrefix}, parts...)
	}

	return strings.Join(parts, ".")
}

func normalizeTopicPrefix(prefix string) string {
	normalized := strings.ToLower(strings.TrimSpace(prefix))
	normalized = strings.NewReplacer("/", "-", "\\", "-", " ", "-", "_", "-", "..", ".", "--", "-").Replace(normalized)
	return strings.Trim(normalized, ".-")
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
