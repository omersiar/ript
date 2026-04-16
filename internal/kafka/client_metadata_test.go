package kafka

import (
	"testing"

	"github.com/omersiar/ript/internal/version"
)

func TestNormalizeSoftwareToken(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		fallback string
		want     string
	}{
		{name: "keeps valid token", input: "ript-franz-go", fallback: "unknown", want: "ript-franz-go"},
		{name: "replaces spaces and plus", input: "RIPT + FRANZ GO", fallback: "unknown", want: "ript-franz-go"},
		{name: "strips invalid leading and trailing characters", input: " (devel) ", fallback: "unknown", want: "devel"},
		{name: "collapses separators", input: "v1..2+++3", fallback: "unknown", want: "v1.2-3"},
		{name: "falls back on empty", input: "   ", fallback: "unknown", want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeSoftwareToken(tt.input, tt.fallback)
			if got != tt.want {
				t.Fatalf("normalizeSoftwareToken(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSoftwareNameAndVersion(t *testing.T) {
	originalVersion := version.Version
	version.Version = "dev build+1"
	t.Cleanup(func() {
		version.Version = originalVersion
	})

	softwareName, softwareVersion := softwareNameAndVersion()
	if softwareName != "ript-franz-go" {
		t.Fatalf("software name = %q, want %q", softwareName, "ript-franz-go")
	}
	if softwareVersion == "" {
		t.Fatal("software version must not be empty")
	}
	if softwareVersion[0] == '-' || softwareVersion[len(softwareVersion)-1] == '-' {
		t.Fatalf("software version %q should not start or end with a separator", softwareVersion)
	}
	if softwareVersion[:10] != "dev-build-" {
		t.Fatalf("software version = %q, want prefix %q", softwareVersion, "dev-build-")
	}
}
