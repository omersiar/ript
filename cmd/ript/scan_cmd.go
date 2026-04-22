package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
	"github.com/omersiar/ript/internal/tracker"
	"github.com/spf13/cobra"
)

type scanOptions struct {
	prefix     string
	search     string
	regex      bool
	output     string
	staleDays  int
	unusedDays int
	logLevel   string
}

func newScanCmd() *cobra.Command {
	opts := &scanOptions{}
	cmd := &cobra.Command{
		Use:   "scan",
		Short: "Replay state, perform one scan, write state topic, then exit",
		Long: `Performs a single scan cycle: ensures the state topic exists, replays
previous state for timestamp continuity, fetches current metadata and offsets
for ALL topics, persists the updated snapshot, and emits tombstones for deleted
topics. Use --prefix or --search to filter the summary output only; all topics
are always written to state.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runScan(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.prefix, "prefix", "", "Filter summary output to topics with this prefix")
	cmd.Flags().StringVar(&opts.search, "search", "", "Filter summary output by substring or regex")
	cmd.Flags().BoolVar(&opts.regex, "regex", false, "Interpret --search as regex")
	cmd.Flags().StringVar(&opts.output, "output", outputTable, "Output format: table|json")
	cmd.Flags().IntVar(&opts.staleDays, "stale-days", 7, "Partition stale threshold in days (for summary)")
	cmd.Flags().IntVar(&opts.unusedDays, "unused-days", 30, "Topic unused threshold in days (for summary)")
	cmd.Flags().StringVar(&opts.logLevel, "log-level", "error", "Log level: debug|info|warn|error")

	return cmd
}

func runScan(ctx context.Context, opts *scanOptions) error {
	logging.Init(opts.logLevel)

	if opts.staleDays < 1 {
		return fmt.Errorf("--stale-days must be at least 1")
	}
	if opts.unusedDays < opts.staleDays {
		return fmt.Errorf("--unused-days must be greater than or equal to --stale-days")
	}
	if err := validateOutputMode(opts.output); err != nil {
		return err
	}

	runtime, err := newCLIRuntime("cli-scan")
	if err != nil {
		return err
	}
	defer runtime.Close()

	// nil workload balancer = this instance owns all partitions (scan everything).
	topicTracker := tracker.NewWithOptions(runtime.kafkaClient, runtime.state, nil, 1, tracker.Options{
		InstanceID: runtime.cfg.InstanceID,
	})

	if err := topicTracker.RunOnceScan(ctx); err != nil {
		return err
	}

	// Reload state to build the summary from the freshly written snapshot.
	topics, err := loadTopicStatusesFromState(ctx, runtime.state)
	if err != nil {
		return err
	}

	matcher, err := newTopicMatcher(opts.prefix, opts.search, opts.regex)
	if err != nil {
		return err
	}

	return printScanSummary(topics, matcher, opts)
}

func printScanSummary(topics map[string]*models.TopicStatus, matcher func(string) bool, opts *scanOptions) error {
	matched := make([]*models.TopicStatus, 0, len(topics))
	for _, topic := range topics {
		if matcher(topic.Name) {
			matched = append(matched, topic)
		}
	}

	if normalizeOutput(opts.output) == outputJSON {
		payload := struct {
			GeneratedAt string                `json:"generated_at"`
			TotalTopics int                   `json:"total_topics"`
			MatchCount  int                   `json:"match_count"`
			Topics      []*models.TopicStatus `json:"topics"`
		}{
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			TotalTopics: len(topics),
			MatchCount:  len(matched),
			Topics:      matched,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(payload)
	}

	fmt.Fprintf(os.Stdout, "Scan completed: wrote %d topic records to state topic\n", len(topics))
	unusedCount := 0
	for _, topic := range matched {
		if classifyTopicStatus(topic, opts.staleDays, opts.unusedDays) == "unused" {
			unusedCount++
		}
	}
	fmt.Fprintf(os.Stdout, "Unused topics (threshold=%d days): %d\n", opts.unusedDays, unusedCount)
	return nil
}
