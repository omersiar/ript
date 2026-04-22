package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/omersiar/ript/internal/config"
	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/models"
	"github.com/omersiar/ript/internal/tracker"
	"github.com/omersiar/ript/internal/version"
	"github.com/twmb/franz-go/pkg/kgo"
)

type cliRuntime struct {
	cfg         *config.Config
	authOpts    []kgo.Opt
	kafkaClient *kafka.Client
	state       *kafka.StateManager
}

func newCLIRuntime(clientRoleSuffix string) (*cliRuntime, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	authOpts, err := buildAuthOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid Kafka authentication configuration: %w", err)
	}

	clientID := fmt.Sprintf("ript-%s-%s-%s", version.BuildTag(), cfg.InstanceID, clientRoleSuffix)
	client, err := kafka.NewClientWithConfig(kafka.ClientConfig{Brokers: cfg.KafkaBrokers, ClientID: clientID, AuthOpts: authOpts})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Kafka: %w", err)
	}

	stateManager := kafka.NewStateManager(
		client,
		cfg.TrackerTopic,
		int32(cfg.TrackerTopicPartitions),
		int16(cfg.TrackerTopicReplicationFactor),
		cfg.TrackerTopicSegmentMS,
		cfg.TrackerTopicMinCleanableRatio,
		time.Duration(cfg.StateLoadTimeoutSeconds)*time.Second,
	)

	return &cliRuntime{
		cfg:         cfg,
		authOpts:    authOpts,
		kafkaClient: client,
		state:       stateManager,
	}, nil
}

func (r *cliRuntime) Close() error {
	if r.kafkaClient == nil {
		return nil
	}
	return r.kafkaClient.Close()
}

func loadTopicStatusesFromState(ctx context.Context, state *kafka.StateManager) (map[string]*models.TopicStatus, error) {
	snapshot, _, err := state.LoadLatestSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to replay state: %w", err)
	}
	if snapshot == nil {
		return map[string]*models.TopicStatus{}, nil
	}
	return tracker.BuildTopicStatusesFromSnapshot(snapshot), nil
}

func filterAndSortTopics(topics map[string]*models.TopicStatus, prefix string, search string, regexMode bool, unusedDays int) ([]*models.TopicStatus, error) {
	matcher, err := newTopicMatcher(prefix, search, regexMode)
	if err != nil {
		return nil, err
	}

	filtered := make([]*models.TopicStatus, 0, len(topics))
	for _, topic := range topics {
		if topic.NewestPartitionAge.Days < unusedDays {
			continue
		}
		if !matcher(topic.Name) {
			continue
		}
		filtered = append(filtered, topic)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].NewestPartitionAge.Days == filtered[j].NewestPartitionAge.Days {
			return filtered[i].Name < filtered[j].Name
		}
		return filtered[i].NewestPartitionAge.Days > filtered[j].NewestPartitionAge.Days
	})

	return filtered, nil
}

func classifyTopicStatus(topic *models.TopicStatus, staleDays int, unusedDays int) string {
	if topic.NewestPartitionAge.Days >= unusedDays {
		return "unused"
	}
	if topic.OldestPartitionAge.Days >= staleDays {
		return "stale"
	}
	return "active"
}

func normalizeOutput(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
