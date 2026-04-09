package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	KafkaBrokers                   []string
	ScanIntervalMinutes            int
	TrackerTopic                   string
	TrackerConsumerGroupID         string
	TrackerGroupSessionTimeoutMS   int
	TrackerGroupHeartbeatMS        int
	TrackerGroupRebalanceTimeoutMS int
	TrackerTimestampSource         string
	TrackerEventTimeHeader         string
	TrackerEventLookupTimeoutMS    int
	TrackerTopicPartitions         int
	TrackerTopicReplicationFactor  int
	TrackerTopicSegmentMS          int
	TrackerTopicMinCleanableRatio  float64
	StateLoadTimeoutSeconds        int
	InstanceID                     string
	HTTPPort                       int
	HTTPHost                       string
	LogLevel                       string

	// Staleness thresholds
	StalePartitionDays int
	UnusedTopicDays    int

	// Pagination
	DefaultPageSize int
	MaxPageSize     int

	// Instance management
	InstanceHeartbeatIntervalSeconds int
	KafkaConnectRetrySeconds         int

	// Kafka authentication
	KafkaSecurityProtocol string
	KafkaSASLMechanism    string
	KafkaSASLUsername     string
	KafkaSASLPassword     string

	KafkaSASLOAuthTokenEndpoint string
	KafkaSASLOAuthClientID      string
	KafkaSASLOAuthClientSecret  string
	KafkaSASLOAuthScope         string

	KafkaTLSCACertFile     string
	KafkaTLSClientCertFile string
	KafkaTLSClientKeyFile  string
	KafkaTLSInsecureSkip   bool
}

func Load() (*Config, error) {
	godotenv.Load()

	cfg := &Config{
		KafkaBrokers:                   getStringSlice("RIPT_KAFKA_BROKERS", []string{"localhost:9092"}),
		ScanIntervalMinutes:            getInt("RIPT_SCAN_INTERVAL_MINUTES", 5),
		TrackerTopic:                   getString("RIPT_STATE_TOPIC", "ript-state"),
		TrackerConsumerGroupID:         getString("RIPT_KAFKA_CONSUMER_GROUP_ID", "ript-scan"),
		TrackerGroupSessionTimeoutMS:   getInt("RIPT_KAFKA_CONSUMER_SESSION_TIMEOUT_MS", 30000),
		TrackerGroupHeartbeatMS:        getInt("RIPT_KAFKA_CONSUMER_HEARTBEAT_MS", 3000),
		TrackerGroupRebalanceTimeoutMS: getInt("RIPT_KAFKA_CONSUMER_REBALANCE_TIMEOUT_MS", 45000),
		TrackerTimestampSource:         strings.ToLower(getString("RIPT_TIMESTAMP_SOURCE", "offset")),
		TrackerEventTimeHeader:         getString("RIPT_EVENT_TIME_HEADER", "x-event-time-ms"),
		TrackerEventLookupTimeoutMS:    getInt("RIPT_EVENT_LOOKUP_TIMEOUT_MS", 3000),
		TrackerTopicPartitions:         getInt("RIPT_STATE_TOPIC_PARTITIONS", 6),
		TrackerTopicReplicationFactor:  getInt("RIPT_STATE_TOPIC_REPLICATION_FACTOR", 1),
		TrackerTopicSegmentMS:          getInt("RIPT_STATE_TOPIC_SEGMENT_MS", 86400000),
		TrackerTopicMinCleanableRatio:  getFloat("RIPT_STATE_TOPIC_MIN_CLEANABLE_DIRTY_RATIO", 0.1),
		StateLoadTimeoutSeconds:        getInt("RIPT_STATE_LOAD_TIMEOUT_SECONDS", 30),
		InstanceID:                     getString("RIPT_INSTANCE_ID", defaultInstanceID()),
		HTTPPort:                       getInt("RIPT_HTTP_PORT", 8080),
		HTTPHost:                       getString("RIPT_HTTP_HOST", "0.0.0.0"),
		LogLevel:                       getString("RIPT_LOG_LEVEL", "info"),

		StalePartitionDays:               getInt("RIPT_STALE_PARTITION_DAYS", 7),
		UnusedTopicDays:                  getInt("RIPT_UNUSED_TOPIC_DAYS", 30),
		DefaultPageSize:                  getInt("RIPT_DEFAULT_PAGE_SIZE", 50),
		MaxPageSize:                      getInt("RIPT_MAX_PAGE_SIZE", 500),
		InstanceHeartbeatIntervalSeconds: getInt("RIPT_INSTANCE_HEARTBEAT_SECONDS", 30),
		KafkaConnectRetrySeconds:         getInt("RIPT_KAFKA_CONNECT_RETRY_SECONDS", 5),

		KafkaSecurityProtocol: getString("RIPT_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
		KafkaSASLMechanism:    getString("RIPT_KAFKA_SASL_MECHANISM", ""),
		KafkaSASLUsername:     getString("RIPT_KAFKA_SASL_USERNAME", ""),
		KafkaSASLPassword:     getString("RIPT_KAFKA_SASL_PASSWORD", ""),

		KafkaSASLOAuthTokenEndpoint: getString("RIPT_KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL", ""),
		KafkaSASLOAuthClientID:      getString("RIPT_KAFKA_SASL_OAUTHBEARER_CLIENT_ID", ""),
		KafkaSASLOAuthClientSecret:  getString("RIPT_KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET", ""),
		KafkaSASLOAuthScope:         getString("RIPT_KAFKA_SASL_OAUTHBEARER_SCOPE", ""),

		KafkaTLSCACertFile:     getString("RIPT_KAFKA_TLS_CA_CERT_FILE", ""),
		KafkaTLSClientCertFile: getString("RIPT_KAFKA_TLS_CLIENT_CERT_FILE", ""),
		KafkaTLSClientKeyFile:  getString("RIPT_KAFKA_TLS_CLIENT_KEY_FILE", ""),
		KafkaTLSInsecureSkip:   getBool("RIPT_KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("RIPT_KAFKA_BROKERS cannot be empty")
	}
	if c.ScanIntervalMinutes < 1 {
		return fmt.Errorf("RIPT_SCAN_INTERVAL_MINUTES must be at least 1")
	}
	if strings.TrimSpace(c.TrackerConsumerGroupID) == "" {
		return fmt.Errorf("RIPT_KAFKA_CONSUMER_GROUP_ID cannot be empty")
	}
	if c.TrackerGroupSessionTimeoutMS < 1000 {
		return fmt.Errorf("RIPT_KAFKA_CONSUMER_SESSION_TIMEOUT_MS must be at least 1000")
	}
	if c.TrackerGroupHeartbeatMS < 250 {
		return fmt.Errorf("RIPT_KAFKA_CONSUMER_HEARTBEAT_MS must be at least 250")
	}
	if c.TrackerGroupHeartbeatMS >= c.TrackerGroupSessionTimeoutMS {
		return fmt.Errorf("RIPT_KAFKA_CONSUMER_HEARTBEAT_MS must be less than RIPT_KAFKA_CONSUMER_SESSION_TIMEOUT_MS")
	}
	if c.TrackerGroupRebalanceTimeoutMS < c.TrackerGroupSessionTimeoutMS {
		return fmt.Errorf("RIPT_KAFKA_CONSUMER_REBALANCE_TIMEOUT_MS must be greater than or equal to RIPT_KAFKA_CONSUMER_SESSION_TIMEOUT_MS")
	}
	if c.TrackerTimestampSource != "offset" && c.TrackerTimestampSource != "header" {
		return fmt.Errorf("RIPT_TIMESTAMP_SOURCE must be either 'offset' or 'header'")
	}
	if strings.TrimSpace(c.TrackerEventTimeHeader) == "" {
		return fmt.Errorf("RIPT_EVENT_TIME_HEADER cannot be empty")
	}
	if c.TrackerEventLookupTimeoutMS < 100 {
		return fmt.Errorf("RIPT_EVENT_LOOKUP_TIMEOUT_MS must be at least 100")
	}
	if c.TrackerTopicPartitions < 1 {
		return fmt.Errorf("RIPT_STATE_TOPIC_PARTITIONS must be at least 1")
	}
	if c.TrackerTopicReplicationFactor < 1 {
		return fmt.Errorf("RIPT_STATE_TOPIC_REPLICATION_FACTOR must be at least 1")
	}
	if c.TrackerTopicSegmentMS < 1800000 {
		return fmt.Errorf("RIPT_STATE_TOPIC_SEGMENT_MS must be at least 1800000")
	}
	if c.TrackerTopicMinCleanableRatio <= 0 || c.TrackerTopicMinCleanableRatio > 1 {
		return fmt.Errorf("RIPT_STATE_TOPIC_MIN_CLEANABLE_DIRTY_RATIO must be > 0 and <= 1")
	}
	if c.StateLoadTimeoutSeconds < 5 {
		return fmt.Errorf("RIPT_STATE_LOAD_TIMEOUT_SECONDS must be at least 5")
	}
	if strings.TrimSpace(c.InstanceID) == "" {
		return fmt.Errorf("RIPT_INSTANCE_ID cannot be empty")
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("RIPT_HTTP_PORT must be between 1 and 65535")
	}
	if c.StalePartitionDays < 1 {
		return fmt.Errorf("RIPT_STALE_PARTITION_DAYS must be at least 1")
	}
	if c.UnusedTopicDays < 1 {
		return fmt.Errorf("RIPT_UNUSED_TOPIC_DAYS must be at least 1")
	}
	if c.UnusedTopicDays < c.StalePartitionDays {
		return fmt.Errorf("RIPT_UNUSED_TOPIC_DAYS must be greater than or equal to RIPT_STALE_PARTITION_DAYS")
	}
	if c.DefaultPageSize < 1 {
		return fmt.Errorf("RIPT_DEFAULT_PAGE_SIZE must be at least 1")
	}
	if c.MaxPageSize < c.DefaultPageSize {
		return fmt.Errorf("RIPT_MAX_PAGE_SIZE must be greater than or equal to RIPT_DEFAULT_PAGE_SIZE")
	}
	if c.InstanceHeartbeatIntervalSeconds < 5 {
		return fmt.Errorf("RIPT_INSTANCE_HEARTBEAT_SECONDS must be at least 5")
	}
	if c.KafkaConnectRetrySeconds < 1 {
		return fmt.Errorf("RIPT_KAFKA_CONNECT_RETRY_SECONDS must be at least 1")
	}
	return nil
}

func (c *Config) String() string {
	return fmt.Sprintf("Config{Brokers: %v, ScanInterval: %dm, StateTopic: %s, ConsumerGroupID: %s, GroupSessionTimeoutMS: %d, GroupHeartbeatMS: %d, GroupRebalanceTimeoutMS: %d, TimestampSource: %s, EventHeader: %s, EventLookupTimeoutMS: %d, Partitions: %d, RF: %d, SegmentMS: %d, MinCleanableRatio: %g, StateLoadTimeout: %ds, InstanceID: %s, HTTPPort: %d, LogLevel: %s, StaleDays: %d, UnusedDays: %d, HeartbeatInterval: %ds, ConnectRetry: %ds}",
		c.KafkaBrokers, c.ScanIntervalMinutes, c.TrackerTopic,
		c.TrackerConsumerGroupID, c.TrackerGroupSessionTimeoutMS, c.TrackerGroupHeartbeatMS, c.TrackerGroupRebalanceTimeoutMS,
		c.TrackerTimestampSource, c.TrackerEventTimeHeader, c.TrackerEventLookupTimeoutMS,
		c.TrackerTopicPartitions, c.TrackerTopicReplicationFactor, c.TrackerTopicSegmentMS, c.TrackerTopicMinCleanableRatio, c.StateLoadTimeoutSeconds,
		c.InstanceID, c.HTTPPort, c.LogLevel, c.StalePartitionDays, c.UnusedTopicDays,
		c.InstanceHeartbeatIntervalSeconds, c.KafkaConnectRetrySeconds)
}

func defaultInstanceID() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "ript"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func getString(key string, defaultVal string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return defaultVal
}

func getInt(key string, defaultVal int) int {
	if val, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getFloat(key string, defaultVal float64) float64 {
	if val, exists := os.LookupEnv(key); exists {
		if floatVal, err := strconv.ParseFloat(val, 64); err == nil {
			return floatVal
		}
	}
	return defaultVal
}

func getStringSlice(key string, defaultVal []string) []string {
	if val, exists := os.LookupEnv(key); exists {
		parts := strings.Split(val, ",")
		var result []string
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultVal
}

func getBool(key string, defaultVal bool) bool {
	if val, exists := os.LookupEnv(key); exists {
		switch strings.ToLower(strings.TrimSpace(val)) {
		case "true", "1", "yes":
			return true
		case "false", "0", "no":
			return false
		}
	}
	return defaultVal
}
