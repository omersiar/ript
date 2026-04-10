package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omersiar/ript/internal/api"
	"github.com/omersiar/ript/internal/config"
	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/tracker"
	"github.com/omersiar/ript/internal/version"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logging.Fatal("Failed to load configuration: %v", err)
	}

	logging.Init(cfg.LogLevel)
	logging.Info("RIPT %s", version.Full())
	logging.Info("Configuration loaded: %s", cfg.String())

	authOpts, err := kafka.BuildAuthOpts(kafka.AuthConfig{
		SecurityProtocol:   cfg.KafkaSecurityProtocol,
		SASLMechanism:      cfg.KafkaSASLMechanism,
		SASLUsername:       cfg.KafkaSASLUsername,
		SASLPassword:       cfg.KafkaSASLPassword,
		OAuthTokenEndpoint: cfg.KafkaSASLOAuthTokenEndpoint,
		OAuthClientID:      cfg.KafkaSASLOAuthClientID,
		OAuthClientSecret:  cfg.KafkaSASLOAuthClientSecret,
		OAuthScope:         cfg.KafkaSASLOAuthScope,
		TLSCACertFile:      cfg.KafkaTLSCACertFile,
		TLSClientCertFile:  cfg.KafkaTLSClientCertFile,
		TLSClientKeyFile:   cfg.KafkaTLSClientKeyFile,
		TLSInsecureSkip:    cfg.KafkaTLSInsecureSkip,
	})
	if err != nil {
		logging.Fatal("Invalid Kafka authentication configuration: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaClient, err := connectKafkaWithRetry(ctx, cfg.KafkaBrokers, cfg.InstanceID, authOpts, cfg.KafkaConnectRetrySeconds)
	if err != nil {
		logging.Fatal("Failed to connect to Kafka: %v", err)
	}
	logging.Info("Successfully connected to Kafka")

	defer func() {
		if kafkaClient != nil {
			if err := kafkaClient.Close(); err != nil {
				logging.Error("Error closing Kafka client: %v", err)
			}
		}
	}()

	stateManager := kafka.NewStateManager(
		kafkaClient,
		cfg.TrackerTopic,
		int32(cfg.TrackerTopicPartitions),
		int16(cfg.TrackerTopicReplicationFactor),
		cfg.TrackerTopicSegmentMS,
		cfg.TrackerTopicMinCleanableRatio,
		time.Duration(cfg.StateLoadTimeoutSeconds)*time.Second,
	)
	workloadBalancer, err := kafka.NewWorkloadBalancer(kafka.WorkloadBalancerOptions{
		Brokers:           cfg.KafkaBrokers,
		TrackerTopic:      cfg.TrackerTopic,
		ConsumerGroupID:   cfg.TrackerConsumerGroupID,
		InstanceID:        cfg.InstanceID,
		TrackerPartitions: int32(cfg.TrackerTopicPartitions),
		SessionTimeout:    time.Duration(cfg.TrackerGroupSessionTimeoutMS) * time.Millisecond,
		HeartbeatInterval: time.Duration(cfg.TrackerGroupHeartbeatMS) * time.Millisecond,
		RebalanceTimeout:  time.Duration(cfg.TrackerGroupRebalanceTimeoutMS) * time.Millisecond,
		ClientID:          fmt.Sprintf("ript-%s-%s-group", version.BuildTag(), cfg.InstanceID),
		AuthOpts:          authOpts,
	})
	if err != nil {
		logging.Fatal("Failed to initialize workload balancer: %v", err)
	}

	topicTracker := tracker.NewWithOptions(kafkaClient, stateManager, workloadBalancer, cfg.ScanIntervalMinutes, tracker.Options{
		TimestampSource:          cfg.TrackerTimestampSource,
		EventTimeHeader:          cfg.TrackerEventTimeHeader,
		EventLookupTO:            time.Duration(cfg.TrackerEventLookupTimeoutMS) * time.Millisecond,
		InstanceID:               cfg.InstanceID,
		ConsumerGroupID:          cfg.TrackerConsumerGroupID,
		InstanceHeartbeatSeconds: cfg.InstanceHeartbeatIntervalSeconds,
	})

	// Register signal handler BEFORE any potentially blocking startup call.
	// This ensures SIGTERM/SIGINT is always responsive — whether the signal
	// arrives during startup (e.g. broker unreachable) or at runtime.
	// The goroutine below cancels the root context so that blocking broker
	// operations (EnsureTrackerTopic, WaitForAssignments, etc.) unblock promptly.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigChan:
			logging.Info("Shutdown signal received (%v), stopping...", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := topicTracker.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			// SIGTERM arrived during startup; perform a clean stop and exit.
			topicTracker.Stop()
			return
		}
		logging.Fatal("Failed to start topic tracker: %v", err)
	}

	server := api.New(topicTracker, cfg.HTTPHost+":"+fmt.Sprintf("%d", cfg.HTTPPort), cfg)
	server.Initialize()

	serverErrChan := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil {
			logging.Error("API server error: %v", err)
			serverErrChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		// Signal was received — already logged by the goroutine above.
	case err := <-serverErrChan:
		logging.Error("Server error, initiating shutdown: %v", err)
		cancel()
	}

	topicTracker.Stop()
	logging.Info("Shutdown complete")
}

func connectKafkaWithRetry(ctx context.Context, brokers []string, instanceID string, authOpts []kgo.Opt, retrySec int) (*kafka.Client, error) {
	adminClientID := fmt.Sprintf("ript-%s-%s-admin", version.BuildTag(), instanceID)
	retryInterval := time.Duration(retrySec) * time.Second
	if retryInterval <= 0 {
		retryInterval = 5 * time.Second
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		client, err := kafka.NewClientWithConfig(kafka.ClientConfig{Brokers: brokers, ClientID: adminClientID, AuthOpts: authOpts})
		if err == nil {
			return client, nil
		}

		logging.Warn("Failed to connect to Kafka (retrying): %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}
