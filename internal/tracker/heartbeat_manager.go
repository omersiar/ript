package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/omersiar/ript/internal/kafka"
	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
)

// HeartbeatManager owns tracker instance heartbeat lifecycle and active-instance sync.
type HeartbeatManager struct {
	tracker              *TopicTracker
	assignmentSignalChan chan struct{}
}

func NewHeartbeatManager(tracker *TopicTracker) *HeartbeatManager {
	return &HeartbeatManager{
		tracker:              tracker,
		assignmentSignalChan: make(chan struct{}, 1),
	}
}

func (m *HeartbeatManager) startLoop(ctx context.Context) {
	t := m.tracker
	t.wg.Add(1)
	go m.heartbeatLoop(ctx)
}

func (m *HeartbeatManager) heartbeatLoop(ctx context.Context) {
	defer m.tracker.wg.Done()

	ticker := time.NewTicker(m.tracker.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.tracker.stopChan:
			return
		case <-ticker.C:
			if err := m.writeLocalHeartbeat(ctx); err != nil {
				logging.Warn("Heartbeat write failed: %v", err)
			}
		case <-m.assignmentSignalChan:
			// Flush assignment count changes immediately after rebalances so
			// active-instance shard counts do not lag until next periodic tick.
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := m.writeLocalHeartbeat(writeCtx); err != nil {
				logging.Warn("Heartbeat write after assignment change failed: %v", err)
			}
			cancel()
		}
	}
}

func (m *HeartbeatManager) signalAssignmentChanged() {
	select {
	case m.assignmentSignalChan <- struct{}{}:
	default:
	}
}

func (m *HeartbeatManager) deregisterOnShutdown() {
	// Write a tombstone for this instance before shutting down so other
	// instances (and the next restart) immediately remove it from the
	// active-instances list.
	if m.tracker.stateManager == nil || m.tracker.instanceID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := m.tracker.stateManager.DeregisterInstance(ctx, m.tracker.instanceID); err != nil {
		logging.Warn("Failed to write instance tombstone on shutdown: %v", err)
	}
	cancel()
}

func (m *HeartbeatManager) syncInstancesFromState(snapshot *kafka.StateSnapshot) {
	now := time.Now().UTC()
	instances := make(map[string]models.InstanceInfo, len(snapshot.Instances))

	var expiredIDs []string

	for instanceID, hb := range snapshot.Instances {
		if !hb.IsActive(now) {
			expiredIDs = append(expiredIDs, instanceID)
			continue
		}

		instances[instanceID] = models.InstanceInfo{
			InstanceID:           instanceID,
			LastHeartbeatAt:      hb.LastHeartbeatAt,
			HeartbeatIntervalSec: hb.HeartbeatIntervalSec,
			ScanIntervalSec:      hb.ScanIntervalSec,
			GroupID:              hb.GroupID,
			AssignedShards:       hb.AssignedShards,
			IsActive:             true,
		}
	}

	m.tracker.activeInstances.Store(&instances)

	// Write tombstones for expired instances so they don't reappear on
	// subsequent restarts. Fire-and-forget with a short timeout; failure
	// is harmless — the instances will simply be pruned again next time.
	if len(expiredIDs) > 0 && m.tracker.stateManager != nil {
		logging.Info("Pruning %d expired instance(s) from state: %v", len(expiredIDs), expiredIDs)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range expiredIDs {
			if err := m.tracker.stateManager.DeregisterInstance(ctx, id); err != nil {
				logging.Warn("Failed to write tombstone for expired instance %s: %v", id, err)
			}
		}
	}
}

func (m *HeartbeatManager) writeLocalHeartbeat(ctx context.Context) error {
	if m.tracker.stateManager == nil {
		return nil
	}
	now := time.Now().UTC().Unix()
	heartbeatIntervalSec := int(m.tracker.heartbeatInterval / time.Second)

	var assignedShards int
	if m.tracker.workloadBalancer != nil {
		assignedShards = m.tracker.workloadBalancer.AssignedShardCount()
	}

	record := &kafka.HeartbeatRecord{
		Version:              1,
		InstanceID:           m.tracker.instanceID,
		LastHeartbeatAt:      now,
		HeartbeatIntervalSec: heartbeatIntervalSec,
		ScanIntervalSec:      int(m.tracker.scanInterval / time.Second),
		GroupID:              m.tracker.consumerGroupID,
		AssignedShards:       assignedShards,
	}

	if err := m.tracker.stateManager.SaveInstanceHeartbeat(ctx, record); err != nil {
		return fmt.Errorf("failed to write local heartbeat: %w", err)
	}
	return nil
}
