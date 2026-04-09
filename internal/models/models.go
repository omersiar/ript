package models

import (
	"strconv"
	"time"
)

type TopicStatus struct {
	Name               string                   `json:"name"`
	PartitionCount     int32                    `json:"partition_count"`
	Partitions         map[int32]*PartitionInfo `json:"partitions"`
	OldestPartitionAge Duration                 `json:"oldest_partition_age"`
	NewestPartitionAge Duration                 `json:"newest_partition_age"`
	LastUpdate         int64                    `json:"last_update"`
}

type PartitionInfo struct {
	Partition int32    `json:"partition"`
	Offset    int64    `json:"offset"`
	Timestamp int64    `json:"timestamp"`
	Age       Duration `json:"age"`
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
	if d.Days > 0 {
		return d.formatWithDays()
	}
	if d.Hours > 0 {
		return d.formatWithHours()
	}
	if d.Minutes > 0 {
		return d.formatWithMinutes()
	}
	return d.formatWithSeconds()
}

func (d Duration) formatWithDays() string {
	if d.Days == 1 && d.Hours == 0 {
		return "1 day"
	}
	if d.Hours == 0 {
		return d.pluralize(d.Days, "day")
	}
	return d.pluralize(d.Days, "day") + " " + d.pluralize(d.Hours, "hour")
}

func (d Duration) formatWithHours() string {
	if d.Hours == 1 && d.Minutes == 0 {
		return "1 hour"
	}
	if d.Minutes == 0 {
		return d.pluralize(d.Hours, "hour")
	}
	return d.pluralize(d.Hours, "hour") + " " + d.pluralize(d.Minutes, "minute")
}

func (d Duration) formatWithMinutes() string {
	if d.Minutes == 1 && d.Seconds == 0 {
		return "1 minute"
	}
	if d.Seconds == 0 {
		return d.pluralize(d.Minutes, "minute")
	}
	return d.pluralize(d.Minutes, "minute") + " " + d.pluralize(d.Seconds, "second")
}

func (d Duration) formatWithSeconds() string {
	return d.pluralize(d.Seconds, "second")
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
