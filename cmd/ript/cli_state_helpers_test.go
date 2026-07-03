package main

import (
	"reflect"
	"testing"

	"github.com/omersiar/ript/internal/models"
)

func TestFilterAndSortTopicsIgnoredModes(t *testing.T) {
	topics := map[string]*models.TopicStatus{
		"ignored-topic": {
			Name:               "ignored-topic",
			NewestPartitionAge: models.Duration{Days: 40},
			Ignored:            true,
		},
		"active-topic": {
			Name:               "active-topic",
			NewestPartitionAge: models.Duration{Days: 40},
			Ignored:            false,
		},
	}

	filtered, err := filterAndSortTopics(topics, "", "", false, 30, false, "false")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got, want := topicNames(filtered), []string{"active-topic"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected topics for ignored=false: got %v want %v", got, want)
	}

	filtered, err = filterAndSortTopics(topics, "", "", false, 30, false, "true")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got, want := topicNames(filtered), []string{"ignored-topic"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected topics for ignored=true: got %v want %v", got, want)
	}

	filtered, err = filterAndSortTopics(topics, "", "", false, 30, false, "all")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got, want := topicNames(filtered), []string{"active-topic", "ignored-topic"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected topics for ignored=all: got %v want %v", got, want)
	}
}

func topicNames(topics []*models.TopicStatus) []string {
	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		names = append(names, topic.Name)
	}
	return names
}
