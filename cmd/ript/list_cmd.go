package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
	"github.com/spf13/cobra"
)

type listOptions struct {
	includeStalePartitions bool
	prefix                 string
	search                 string
	regex                  bool
	staleDays              int
	unusedDays             int
	output                 string
	logLevel               string
}

func newListCmd() *cobra.Command {
	opts := &listOptions{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List unused topics from replayed state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runList(cmd.Context(), opts)
		},
	}

	cmd.Flags().BoolVar(&opts.includeStalePartitions, "include-stale-partitions", false, "Include stale partition details for each topic")
	cmd.Flags().StringVar(&opts.prefix, "prefix", "", "Only include topics with this prefix")
	cmd.Flags().StringVar(&opts.search, "search", "", "Filter topics by substring or regex")
	cmd.Flags().BoolVar(&opts.regex, "regex", false, "Interpret --search as regex")
	cmd.Flags().IntVar(&opts.staleDays, "stale-days", 7, "Partition stale threshold in days")
	cmd.Flags().IntVar(&opts.unusedDays, "unused-days", 30, "Topic unused threshold in days")
	cmd.Flags().StringVar(&opts.output, "output", outputTable, "Output format: table|json")
	cmd.Flags().StringVar(&opts.logLevel, "log-level", "error", "Log level: debug|info|warn|error")

	return cmd
}

func runList(ctx context.Context, opts *listOptions) error {
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

	runtime, err := newCLIRuntime("cli-list")
	if err != nil {
		return err
	}
	defer runtime.Close()

	topics, err := loadTopicStatusesFromState(ctx, runtime.state)
	if err != nil {
		return err
	}

	filtered, err := filterAndSortTopics(topics, opts.prefix, opts.search, opts.regex, opts.unusedDays)
	if err != nil {
		return err
	}

	if normalizeOutput(opts.output) == outputJSON {
		return printListJSON(filtered, opts)
	}
	printListTable(filtered, opts)
	return nil
}

type listJSONItem struct {
	Name              string          `json:"name"`
	Status            string          `json:"status"`
	PartitionCount    int32           `json:"partition_count"`
	OldestPartition   string          `json:"oldest_partition_age"`
	NewestPartition   string          `json:"newest_partition_age"`
	StalePartitions   int             `json:"stale_partitions"`
	LastUpdateUnix    int64           `json:"last_update_unix"`
	StalePartitionSet []partitionJSON `json:"stale_partition_details,omitempty"`
}

type partitionJSON struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Age       string `json:"age"`
}

func printListJSON(topics []*models.TopicStatus, opts *listOptions) error {
	items := make([]listJSONItem, 0, len(topics))
	for _, topic := range topics {
		stalePartitions := collectStalePartitions(topic, opts.staleDays)
		item := listJSONItem{
			Name:            topic.Name,
			Status:          classifyTopicStatus(topic, opts.staleDays, opts.unusedDays),
			PartitionCount:  topic.PartitionCount,
			OldestPartition: topic.OldestPartitionAge.String(),
			NewestPartition: topic.NewestPartitionAge.String(),
			StalePartitions: len(stalePartitions),
			LastUpdateUnix:  topic.LastUpdate,
		}
		if opts.includeStalePartitions {
			item.StalePartitionSet = make([]partitionJSON, 0, len(stalePartitions))
			for _, part := range stalePartitions {
				item.StalePartitionSet = append(item.StalePartitionSet, partitionJSON{
					Partition: part.Partition,
					Offset:    part.Offset,
					Age:       part.Age.String(),
				})
			}
		}
		items = append(items, item)
	}

	payload := struct {
		GeneratedAt string         `json:"generated_at"`
		Count       int            `json:"count"`
		Topics      []listJSONItem `json:"topics"`
	}{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Count:       len(items),
		Topics:      items,
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(payload)
}

func printListTable(topics []*models.TopicStatus, opts *listOptions) {
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tSTATUS\tPARTITIONS\tSTALE_PARTITIONS\tOLDEST_AGE\tNEWEST_AGE")
	for _, topic := range topics {
		stalePartitions := collectStalePartitions(topic, opts.staleDays)
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n",
			topic.Name,
			classifyTopicStatus(topic, opts.staleDays, opts.unusedDays),
			topic.PartitionCount,
			len(stalePartitions),
			topic.OldestPartitionAge.String(),
			topic.NewestPartitionAge.String(),
		)
		if opts.includeStalePartitions {
			for _, part := range stalePartitions {
				fmt.Fprintf(w, "  - p%d\t\t\t\t\t%s (offset=%d)\n", part.Partition, part.Age.String(), part.Offset)
			}
		}
	}
	w.Flush()
}

func collectStalePartitions(topic *models.TopicStatus, staleDays int) []*models.PartitionInfo {
	parts := make([]*models.PartitionInfo, 0, len(topic.Partitions))
	for _, part := range topic.Partitions {
		if part.Age.Days >= staleDays {
			parts = append(parts, part)
		}
	}
	return parts
}
