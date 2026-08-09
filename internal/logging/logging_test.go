package logging

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Level
	}{
		{name: "default", input: "", expected: INFO},
		{name: "debug", input: "debug", expected: DEBUG},
		{name: "info", input: "info", expected: INFO},
		{name: "warn", input: "warn", expected: WARN},
		{name: "error", input: "error", expected: ERROR},
		{name: "unknown", input: "trace", expected: INFO},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseLevel(tt.input); got != tt.expected {
				t.Fatalf("parseLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNewLoggerWritesToProvidedWriter(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(INFO, &buf)

	logger.log(INFO, "hello %s", "world")

	output := buf.String()
	if !strings.Contains(output, "hello world") {
		t.Fatalf("expected log output to contain message, got %q", output)
	}
}
