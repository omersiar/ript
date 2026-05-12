package config

import (
	"strings"
	"testing"
)

func TestConfigStringRedactsSensitiveFields(t *testing.T) {
	cfg := &Config{
		KafkaBrokers:                     []string{"broker-1:9092"},
		ScanIntervalMinutes:              5,
		TrackerTopic:                     "ript-state",
		TrackerConsumerGroupID:           "ript-scan",
		TrackerGroupSessionTimeoutMS:     30000,
		TrackerGroupHeartbeatMS:          3000,
		TrackerGroupRebalanceTimeoutMS:   45000,
		TrackerTopicPartitions:           6,
		TrackerTopicReplicationFactor:    1,
		TrackerTopicSegmentMS:            86400000,
		TrackerTopicMinCleanableRatio:    0.1,
		StateLoadTimeoutSeconds:          30,
		InstanceID:                       "instance-1",
		HTTPHost:                         "127.0.0.1",
		HTTPPort:                         8080,
		StaticFilesDir:                   "./web/static",
		LogLevel:                         "info",
		StalePartitionDays:               7,
		UnusedTopicDays:                  30,
		InstanceHeartbeatIntervalSeconds: 30,
		KafkaConnectRetrySeconds:         5,
		KafkaSecurityProtocol:            "SASL_SSL",
		KafkaSASLMechanism:               "PLAIN",
		KafkaSASLUsername:                "alice",
		KafkaSASLPassword:                "super-secret-password",
		KafkaSASLOAuthTokenEndpoint:      "https://issuer.example/token",
		KafkaSASLOAuthClientID:           "client-id",
		KafkaSASLOAuthClientSecret:       "oauth-secret",
		KafkaSASLOAuthScope:              "ript.read",
		KafkaTLSCACertFile:               "/etc/certs/ca.pem",
		KafkaTLSClientCertFile:           "/etc/certs/client.pem",
		KafkaTLSClientKeyFile:            "/etc/certs/client.key",
		KafkaTLSInsecureSkip:             true,
	}

	got := cfg.String()

	for _, secret := range []string{"super-secret-password", "oauth-secret", "/etc/certs/client.key"} {
		if strings.Contains(got, secret) {
			t.Fatalf("config string leaked sensitive value %q: %s", secret, got)
		}
	}

	for _, expected := range []string{
		"KafkaSASLPassword: [sensitive]",
		"KafkaSASLOAuthClientSecret: [sensitive]",
		"KafkaTLSClientKeyFile: [sensitive]",
		"KafkaSASLUsername: alice",
		"KafkaSASLOAuthClientID: client-id",
		"KafkaSASLOAuthTokenEndpoint: https://issuer.example/token",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("config string missing %q: %s", expected, got)
		}
	}
}

func TestRedactSensitive(t *testing.T) {
	if got := redactSensitive(""); got != "" {
		t.Fatalf("redactSensitive(\"\") = %q, want empty string", got)
	}

	if got := redactSensitive("  value  "); got != sensitiveValueLabel {
		t.Fatalf("redactSensitive(non-empty) = %q, want %q", got, sensitiveValueLabel)
	}
}
