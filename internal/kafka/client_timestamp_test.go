package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTimestampFromHeadersUnixMillis(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: "x-event-time-ms", Value: []byte("1710000000123")}}

	ts, ok := timestampFromHeaders(headers, "x-event-time-ms")
	if !ok {
		t.Fatal("expected timestamp to parse")
	}
	if got, want := ts.UTC(), time.UnixMilli(1710000000123).UTC(); !got.Equal(want) {
		t.Fatalf("unexpected timestamp: got %v want %v", got, want)
	}
}

func TestTimestampFromHeadersRFC3339(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: "x-event-time-ms", Value: []byte("2026-03-28T12:34:56Z")}}

	ts, ok := timestampFromHeaders(headers, "x-event-time-ms")
	if !ok {
		t.Fatal("expected timestamp to parse")
	}
	if got, want := ts.UTC(), time.Date(2026, time.March, 28, 12, 34, 56, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("unexpected timestamp: got %v want %v", got, want)
	}
}

func TestTimestampFromHeadersMissingOrInvalid(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: "other", Value: []byte("1710000000123")}}
	if _, ok := timestampFromHeaders(headers, "x-event-time-ms"); ok {
		t.Fatal("expected missing header to fail parse")
	}

	headers = []kgo.RecordHeader{{Key: "x-event-time-ms", Value: []byte("not-a-ts")}}
	if _, ok := timestampFromHeaders(headers, "x-event-time-ms"); ok {
		t.Fatal("expected invalid header to fail parse")
	}
}
