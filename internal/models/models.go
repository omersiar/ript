package models

import (
	"strconv"
	"strings"
	"time"
)

type TopicStatus struct {
	Name               string                   `json:"name"`
	PartitionCount     int32                    `json:"partition_count"`
	Partitions         map[int32]*PartitionInfo `json:"partitions"`
	OldestPartitionAge Duration                 `json:"oldest_partition_age"`
	NewestPartitionAge Duration                 `json:"newest_partition_age"`
	LastUpdate         int64                    `json:"last_update"`
	IsEmpty            bool                     `json:"is_empty"`
}

type PartitionInfo struct {
	Partition int32    `json:"partition"`
	Offset    int64    `json:"offset"`
	Timestamp int64    `json:"timestamp"`
	Age       Duration `json:"age"`
	IsEmpty   bool     `json:"is_empty"`
}

type ClusterSnapshot struct {
	Topics          map[string]*TopicStatus `json:"topics"`
	Timestamp       int64                   `json:"timestamp"`
	Version         int                     `json:"version"`
	IsGlobal        bool                    `json:"is_global"`
	LocalInstanceID string                  `json:"local_instance_id,omitempty"`
}

type InstanceInfo struct {
	InstanceID           string `json:"instance_id"`
	LastHeartbeatAt      int64  `json:"last_heartbeat_at"`
	HeartbeatIntervalSec int    `json:"heartbeat_interval_sec"`
	ScanIntervalSec      int    `json:"scan_interval_sec,omitempty"`
	GroupID              string `json:"group_id,omitempty"`
	AssignedShards       int    `json:"assigned_shards,omitempty"`
	IsActive             bool   `json:"is_active"`
}

type Duration struct {
	Days    int `json:"days"`
	Hours   int `json:"hours"`
	Minutes int `json:"minutes"`
	Seconds int `json:"seconds"`
}

func (d Duration) String() string {
	units := []struct {
		val  int
		name string
	}{
		{d.Days, "day"},
		{d.Hours, "hour"},
		{d.Minutes, "minute"},
		{d.Seconds, "second"},
	}

	var parts []string
	for _, u := range units {
		if u.val > 0 {
			parts = append(parts, d.pluralize(u.val, u.name))
		}
		if len(parts) == 2 {
			break
		}
	}
	if len(parts) == 0 {
		return d.pluralize(0, "second")
	}
	return strings.Join(parts, " ")
}

func (d Duration) pluralize(count int, unit string) string {
	if count == 1 {
		return "1 " + unit
	}
	return strconv.Itoa(count) + " " + unit + "s"
}

func CalculateDuration(since time.Time) Duration {
	elapsed := time.Since(since)
	days := int(elapsed.Hours()) / 24
	hours := int(elapsed.Hours()) % 24
	minutes := int(elapsed.Minutes()) % 60
	seconds := int(elapsed.Seconds()) % 60

	return Duration{
		Days:    days,
		Hours:   hours,
		Minutes: minutes,
		Seconds: seconds,
	}
}
