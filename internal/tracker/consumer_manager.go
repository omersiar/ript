package tracker

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/logging"
)

// ConsumerManager owns global tracker-topic consumption and record merging.
type ConsumerManager struct {
	tracker *TopicTracker
}

func NewConsumerManager(tracker *TopicTracker) *ConsumerManager {
	return &ConsumerManager{tracker: tracker}
}

func (m *ConsumerManager) startGlobalConsumerLoop(ctx context.Context, resumeOffsets map[int32]int64) {
	loopCtx, cancel := context.WithCancel(ctx)
	m.tracker.globalCancel = cancel

	m.tracker.wg.Add(1)
	go func() {
		defer m.tracker.wg.Done()
		logging.Info("Global consumer loop started (combined multi-instance view)")
		m.tracker.stateManager.SubscribeGlobalUpdates(loopCtx, resumeOffsets, m.applyGlobalRecord)
		logging.Info("Global consumer loop stopped")
	}()
}

func (m *ConsumerManager) stopGlobalConsumerLoop() {
	if m.tracker.globalCancel != nil {
		m.tracker.globalCancel()
	}
}

func (m *ConsumerManager) applyGlobalRecord(key string, value []byte) {
	if instanceID, ok := strings.CutPrefix(key, "tracker-instance:"); ok {
		m.tracker.applyHeartbeatRecord(instanceID, value)
		return
	}

	m.tracker.globalMu.Lock()
	defer m.tracker.globalMu.Unlock()

	if value == nil {
		delete(m.tracker.globalTopics, key)
	} else {
		var state kafka.TopicState
		if err := json.Unmarshal(value, &state); err != nil {
			logging.Warn("applyGlobalRecord: failed to unmarshal topic state for key %s: %v", key, err)
			return
		}
		incoming := buildTopicStatusFromState(&state)
		// Merge rather than replace: preserve partition data from the existing
		// global state for any partitions absent in the incoming record. This
		// guards against partial writes (e.g. records written by a single
		// instance that only owns a subset of the topic's partitions) silently
		// discarding state for partitions owned by other instances.
		m.tracker.globalTopics[state.Topic] = mergeGlobalTopicRecord(m.tracker.globalTopics[state.Topic], incoming)
	}

	m.tracker.globalSnapshot.Store(m.tracker.buildGlobalSnapshotLocked())
}
