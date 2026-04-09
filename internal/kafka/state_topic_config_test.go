package kafka

import "testing"

func TestCleanupPolicyHasCompact(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   bool
	}{
		{name: "exact compact", policy: "compact", want: true},
		{name: "compact delete", policy: "compact,delete", want: true},
		{name: "whitespace and case", policy: " delete, COMPACT ", want: true},
		{name: "delete only", policy: "delete", want: false},
		{name: "empty", policy: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cleanupPolicyHasCompact(tt.policy); got != tt.want {
				t.Fatalf("cleanupPolicyHasCompact(%q) = %v, want %v", tt.policy, got, tt.want)
			}
		})
	}
}

func TestValidateTrackerTopicConfigs(t *testing.T) {
	expected := map[string]string{
		"cleanup.policy":            "compact",
		"segment.ms":                "104857600",
		"min.cleanable.dirty.ratio": "0.1",
		"retention.ms":              "-1",
		"retention.bytes":           "-1",
	}

	t.Run("errors when cleanup policy is not compacted", func(t *testing.T) {
		actual := map[string]string{
			"cleanup.policy": "delete",
		}

		warnings, err := validateTrackerTopicConfigs(actual, expected)
		if err == nil {
			t.Fatal("expected error for non-compacted cleanup policy")
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings on hard failure, got %d", len(warnings))
		}
	})

	t.Run("returns warnings for non-critical mismatches", func(t *testing.T) {
		actual := map[string]string{
			"cleanup.policy": "compact",
			"segment.ms":     "2048",
		}

		warnings, err := validateTrackerTopicConfigs(actual, expected)
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if len(warnings) == 0 {
			t.Fatal("expected warning(s) for non-critical config mismatch")
		}
	})
}
