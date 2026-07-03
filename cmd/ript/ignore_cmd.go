package main

import (
	"context"
	"fmt"
	"time"

	"github.com/omersiar/ript/internal/logging"
	"github.com/spf13/cobra"
)

type ignoreOptions struct {
	logLevel string
}

func newIgnoreCmd() *cobra.Command {
	opts := &ignoreOptions{}
	cmd := &cobra.Command{
		Use:   "ignore <topic-name> [<topic-name2> ...]",
		Short: "Mark one or more topics as ignored",
		Long:  "Mark one or more topics as ignored. Ignored topics will still be tracked but hidden by default from the dashboard and CLI.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIgnore(cmd.Context(), opts, args)
		},
	}

	cmd.Flags().StringVar(&opts.logLevel, "log-level", "error", "Log level: debug|info|warn|error")

	return cmd
}

func newUnignoreCmd() *cobra.Command {
	opts := &ignoreOptions{}
	cmd := &cobra.Command{
		Use:   "unignore <topic-name> [<topic-name2> ...]",
		Short: "Unmark one or more topics as ignored",
		Long:  "Unmark one or more topics as ignored. Unignored topics will appear again on the dashboard and CLI.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUnignore(cmd.Context(), opts, args)
		},
	}

	cmd.Flags().StringVar(&opts.logLevel, "log-level", "error", "Log level: debug|info|warn|error")

	return cmd
}

type ignoreResult struct {
	Topic   string
	Success bool
	Error   string
}

func runIgnore(ctx context.Context, opts *ignoreOptions, topics []string) error {
	return updateTopicIgnored(ctx, opts, topics, true)
}

func runUnignore(ctx context.Context, opts *ignoreOptions, topics []string) error {
	return updateTopicIgnored(ctx, opts, topics, false)
}

func updateTopicIgnored(ctx context.Context, opts *ignoreOptions, topics []string, ignored bool) error {
	logging.Init(opts.logLevel)

	runtime, err := newCLIRuntime("cli-ignore")
	if err != nil {
		return err
	}
	defer runtime.Close()

	topicStates, err := loadTopicStatusesFromState(ctx, runtime.state)
	if err != nil {
		return err
	}

	action := "ignored"
	if !ignored {
		action = "unignored"
	}

	results := make([]ignoreResult, 0, len(topics))
	successCount := 0

	for _, topicName := range topics {
		topic, exists := topicStates[topicName]
		if !exists || topic == nil {
			results = append(results, ignoreResult{Topic: topicName, Success: false, Error: fmt.Sprintf("topic not found: %s", topicName)})
			continue
		}

		topic.Ignored = ignored
		if ignored {
			now := time.Now().UTC().Unix()
			topic.IgnoredAt = &now
		} else {
			topic.IgnoredAt = nil
		}

		if err := runtime.state.SaveTopicState(ctx, topicName, topic); err != nil {
			results = append(results, ignoreResult{Topic: topicName, Success: false, Error: fmt.Sprintf("failed to persist %s state: %v", action, err)})
			continue
		}

		topicStates[topicName] = topic
		results = append(results, ignoreResult{Topic: topicName, Success: true})
		successCount++
	}

	if successCount == len(topics) {
		fmt.Printf("Successfully %s %d topic(s):\n", action, successCount)
		for _, result := range results {
			if result.Success {
				fmt.Printf("  ✓ %s\n", result.Topic)
			}
		}
		return nil
	}

	fmt.Printf("Partially succeeded: %d/%d topics %s\n", successCount, len(topics), action)
	for _, result := range results {
		if result.Success {
			fmt.Printf("  ✓ %s\n", result.Topic)
		} else {
			fmt.Printf("  ✗ %s: %s\n", result.Topic, result.Error)
		}
	}

	return fmt.Errorf("operation did not complete successfully")
}
