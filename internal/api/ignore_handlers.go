package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/omersiar/ript/internal/logging"
)

// ignoreRequest is the request body for updating a topic's ignored flag
type ignoreRequest struct {
	Ignored bool `json:"ignored"`
}

// bulkIgnoreRequest is the request body for bulk updating ignored flags
type bulkIgnoreRequest struct {
	Topics  []string `json:"topics" binding:"required,min=1"`
	Ignored bool     `json:"ignored"`
}

// ignoreResponse represents the result of an ignore/unignore operation for a single topic
type ignoreResponse struct {
	Topic   string `json:"topic"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Ignored bool   `json:"ignored,omitempty"`
}

// handleUpdateTopicIgnored sets or unsets the ignored flag for a single topic via PUT
func (s *Server) handleUpdateTopicIgnored(c *gin.Context) {
	if !s.ensureTrackerReady(c) {
		return
	}

	topicName := c.Param("name")
	var req ignoreRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Get current topic state
	topic := s.trackerPtr.GetTopic(topicName)
	if topic == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "topic not found"})
		return
	}

	// Update the ignored flag
	if err := s.trackerPtr.UpdateTopicIgnored(c.Request.Context(), topicName, req.Ignored); err != nil {
		logging.Warn("Failed to update ignored flag for topic %s: %v", topicName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update topic: %v", err)})
		return
	}

	// Get updated topic
	updatedTopic := s.trackerPtr.GetTopic(topicName)
	if updatedTopic == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch updated topic"})
		return
	}

	// Log the audit event
	logIgnoreAuditEvent(c, topicName, req.Ignored)

	c.JSON(http.StatusOK, gin.H{
		"topic":      topicName,
		"ignored":    updatedTopic.Ignored,
		"ignored_at": updatedTopic.IgnoredAt,
	})
}

// handleBulkUpdateIgnored sets or unsets the ignored flag for multiple topics via POST
func (s *Server) handleBulkUpdateIgnored(c *gin.Context) {
	if !s.ensureTrackerReady(c) {
		return
	}

	var req bulkIgnoreRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	results := make([]ignoreResponse, 0, len(req.Topics))

	for _, topicName := range req.Topics {
		// Get current topic state
		topic := s.trackerPtr.GetTopic(topicName)
		if topic == nil {
			results = append(results, ignoreResponse{
				Topic:   topicName,
				Success: false,
				Error:   "topic not found",
			})
			continue
		}

		// Update the ignored flag
		if err := s.trackerPtr.UpdateTopicIgnored(c.Request.Context(), topicName, req.Ignored); err != nil {
			logging.Warn("Failed to update ignored flag for topic %s: %v", topicName, err)
			results = append(results, ignoreResponse{
				Topic:   topicName,
				Success: false,
				Error:   err.Error(),
			})
			continue
		}

		// Get updated topic to confirm
		updatedTopic := s.trackerPtr.GetTopic(topicName)
		if updatedTopic == nil {
			results = append(results, ignoreResponse{
				Topic:   topicName,
				Success: false,
				Error:   "failed to fetch updated topic",
			})
			continue
		}

		// Log the audit event
		logIgnoreAuditEvent(c, topicName, req.Ignored)

		results = append(results, ignoreResponse{
			Topic:   topicName,
			Success: true,
			Ignored: updatedTopic.Ignored,
		})
	}

	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"results":       results,
		"success_count": successCount,
		"total_count":   len(results),
		"timestamp":     time.Now().UTC().Unix(),
	})
}

// logIgnoreAuditEvent logs an audit trail for ignore/unignore operations
func logIgnoreAuditEvent(c *gin.Context, topicName string, ignored bool) {
	action := "unignore"
	if ignored {
		action = "ignore"
	}
	logging.Info("Topic %s %sd by %s (at %s)", topicName, action, c.ClientIP(), time.Now().UTC().Format(time.RFC3339))
}
