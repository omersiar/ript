package api

import (
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/omersiar/ript/internal/config"
	"github.com/omersiar/ript/internal/logging"
	"github.com/omersiar/ript/internal/models"
	"github.com/omersiar/ript/internal/tracker"
)

type Server struct {
	trackerPtr *tracker.TopicTracker
	router     *gin.Engine
	addr       string
	cfg        *config.Config
}

func New(trackerPtr *tracker.TopicTracker, addr string, cfg *config.Config) *Server {
	return &Server{
		trackerPtr: trackerPtr,
		addr:       addr,
		cfg:        cfg,
	}
}

type pagination struct {
	Page   int
	Limit  int
	Offset int
}

type topicResponse struct {
	Name                     string `json:"name"`
	PartitionCount           int32  `json:"partition_count"`
	OldestPartitionTimestamp int64  `json:"oldest_partition_timestamp"`
	NewestPartitionTimestamp int64  `json:"newest_partition_timestamp"`
}

func (s *Server) Initialize() {
	s.router = gin.New()
	s.router.Use(gin.Recovery())
	if gin.Mode() != gin.ReleaseMode {
		s.router.Use(gin.Logger())
	}
	s.router.LoadHTMLGlob("web/templates/*.html")
	s.setupRoutes()
}

func (s *Server) setupRoutes() {
	// API routes
	s.router.GET("/api/health", s.handleHealth)
	s.router.GET("/api/topics", s.handleListTopics)
	s.router.GET("/api/topics/:name", s.handleGetTopic)
	s.router.GET("/api/unused", s.handleGetUnused)
	s.router.GET("/api/stats", s.handleStats)
	s.router.GET("/api/instances", s.handleInstances)

	// Static files and dashboard
	s.router.Static("/assets", "./web/static")
	s.router.GET("/", s.handleDashboard)
	s.router.HEAD("/", s.handleDashboard)
}

func (s *Server) Start() error {
	logging.Info("Starting API server on %s", s.addr)
	return s.router.Run(s.addr)
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) handleListTopics(c *gin.Context) {
	if s.trackerPtr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":    "Not yet initialized, connecting to Kafka...",
			"count":    0,
			"topics":   []interface{}{},
			"total":    0,
			"page":     1,
			"limit":    s.cfg.DefaultPageSize,
			"has_more": false,
		})
		return
	}

	p, err := s.parsePagination(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	snapshot := s.trackerPtr.GetSnapshot()

	sortBy, sortDir := parseSort(c)
	searchRe, err := parseSearch(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	topics := make([]topicResponse, 0, len(snapshot.Topics))
	for _, topic := range snapshot.Topics {
		r := buildTopicResponse(topic)
		if searchRe != nil && !searchRe.MatchString(r.Name) {
			continue
		}
		topics = append(topics, r)
	}

	s.sortTopicResponses(topics, sortBy, sortDir)

	total := len(topics)
	paged, hasMore := paginateTopicResponses(topics, p)

	c.JSON(http.StatusOK, gin.H{
		"topics":    paged,
		"count":     len(paged),
		"total":     total,
		"page":      p.Page,
		"limit":     p.Limit,
		"has_more":  hasMore,
		"timestamp": snapshot.Timestamp.Unix(),
	})
}

func (s *Server) handleGetTopic(c *gin.Context) {
	if s.trackerPtr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Not yet initialized, connecting to Kafka...",
		})
		return
	}

	topicName := c.Param("name")
	topic := s.trackerPtr.GetTopic(topicName)

	if topic == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "topic not found"})
		return
	}

	type PartitionResponse struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
		Timestamp int64 `json:"timestamp"`
	}

	var partitions []PartitionResponse
	var oldestTS, newestTS time.Time
	for _, part := range topic.Partitions {
		partitions = append(partitions, PartitionResponse{
			Partition: part.Partition,
			Offset:    part.Offset,
			Timestamp: part.Timestamp.Unix(),
		})
		if oldestTS.IsZero() || part.Timestamp.Before(oldestTS) {
			oldestTS = part.Timestamp
		}
		if newestTS.IsZero() || part.Timestamp.After(newestTS) {
			newestTS = part.Timestamp
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"name":             topic.Name,
		"partition_count":  topic.PartitionCount,
		"partitions":       partitions,
		"oldest_timestamp": oldestTS.Unix(),
		"newest_timestamp": newestTS.Unix(),
		"last_update":      topic.LastUpdate.Format(time.RFC3339),
	})
}

func (s *Server) handleGetUnused(c *gin.Context) {
	if s.trackerPtr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":         "Not yet initialized, connecting to Kafka...",
			"count":         0,
			"unused_topics": []interface{}{},
			"total":         0,
			"page":          1,
			"limit":         s.cfg.DefaultPageSize,
			"has_more":      false,
		})
		return
	}

	p, err := s.parsePagination(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	daysStr := c.DefaultQuery("threshold_days", strconv.Itoa(s.cfg.UnusedTopicDays))
	days, err := strconv.Atoi(daysStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid threshold_days"})
		return
	}

	sortBy, sortDir := parseSort(c)
	searchRe, err := parseSearch(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	unused := s.trackerPtr.GetUnusedTopics(days)

	topics := make([]topicResponse, 0, len(unused))
	for _, topic := range unused {
		r := buildTopicResponse(topic)
		if searchRe != nil && !searchRe.MatchString(r.Name) {
			continue
		}
		topics = append(topics, r)
	}

	s.sortTopicResponses(topics, sortBy, sortDir)

	total := len(topics)
	paged, hasMore := paginateTopicResponses(topics, p)

	c.JSON(http.StatusOK, gin.H{
		"threshold_days": days,
		"unused_topics":  paged,
		"count":          len(paged),
		"total":          total,
		"page":           p.Page,
		"limit":          p.Limit,
		"has_more":       hasMore,
	})
}

func (s *Server) handleStats(c *gin.Context) {
	if s.trackerPtr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":            "Not yet initialized, connecting to Kafka...",
			"total_topics":     0,
			"total_partitions": 0,
			"unused_topics":    0,
			"stale_partitions": 0,
			"active_instances": 0,
		})
		return
	}

	snapshot := s.trackerPtr.GetSnapshot()

	unusedDaysStr := c.DefaultQuery("threshold_days", strconv.Itoa(s.cfg.UnusedTopicDays))
	unusedDays, err := strconv.Atoi(unusedDaysStr)
	if err != nil || unusedDays < 1 {
		unusedDays = s.cfg.UnusedTopicDays
	}

	staleDaysStr := c.DefaultQuery("stale_days", strconv.Itoa(s.cfg.StalePartitionDays))
	staleDays, err := strconv.Atoi(staleDaysStr)
	if err != nil || staleDays < 1 {
		staleDays = s.cfg.StalePartitionDays
	}

	var totalPartitions int32
	var unusedCount int
	var staleCount int

	for _, topic := range snapshot.Topics {
		totalPartitions += topic.PartitionCount
		if topic.NewestPartitionAge.Days >= unusedDays {
			unusedCount++
		}
		for _, part := range topic.Partitions {
			if part.Age.Days >= staleDays {
				staleCount++
			}
		}
	}

	instances := s.trackerPtr.GetInstances()
	activeInstances := 0
	for _, instance := range instances {
		if instance.IsActive {
			activeInstances++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"total_topics":      len(snapshot.Topics),
		"total_partitions":  totalPartitions,
		"unused_topics":     unusedCount,
		"stale_partitions":  staleCount,
		"active_instances":  activeInstances,
		"last_scan":         snapshot.Timestamp.Format(time.RFC3339),
		"is_global":         snapshot.IsGlobal,
		"local_instance_id": snapshot.LocalInstanceID,
	})
}

func (s *Server) handleInstances(c *gin.Context) {
	if s.trackerPtr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":     "Not yet initialized, connecting to Kafka...",
			"instances": []interface{}{},
			"count":     0,
		})
		return
	}

	instances := s.trackerPtr.GetInstances()

	c.JSON(http.StatusOK, gin.H{
		"instances": instances,
		"count":     len(instances),
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) handleDashboard(c *gin.Context) {
	c.HTML(http.StatusOK, "dashboard.html", gin.H{
		"StaleDays":  s.cfg.StalePartitionDays,
		"UnusedDays": s.cfg.UnusedTopicDays,
		"PageSize":   s.cfg.DefaultPageSize,
	})
}

func (s *Server) parsePagination(c *gin.Context) (pagination, error) {
	page, err := parsePositiveInt(c.DefaultQuery("page", "1"), "page")
	if err != nil {
		return pagination{}, err
	}

	limit, err := parsePositiveInt(c.DefaultQuery("limit", strconv.Itoa(s.cfg.DefaultPageSize)), "limit")
	if err != nil {
		return pagination{}, err
	}
	if limit > s.cfg.MaxPageSize {
		limit = s.cfg.MaxPageSize
	}

	return pagination{
		Page:   page,
		Limit:  limit,
		Offset: (page - 1) * limit,
	}, nil
}

func parsePositiveInt(raw string, field string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("invalid %s", field)
	}
	return value, nil
}

func buildTopicResponse(topic *models.TopicStatus) topicResponse {
	var oldestTS, newestTS time.Time
	for _, part := range topic.Partitions {
		if oldestTS.IsZero() || part.Timestamp.Before(oldestTS) {
			oldestTS = part.Timestamp
		}
		if newestTS.IsZero() || part.Timestamp.After(newestTS) {
			newestTS = part.Timestamp
		}
	}
	return topicResponse{
		Name:                     topic.Name,
		PartitionCount:           topic.PartitionCount,
		OldestPartitionTimestamp: oldestTS.Unix(),
		NewestPartitionTimestamp: newestTS.Unix(),
	}
}

func paginateTopicResponses(items []topicResponse, p pagination) ([]topicResponse, bool) {
	if p.Offset >= len(items) {
		return []topicResponse{}, false
	}

	end := p.Offset + p.Limit
	if end > len(items) {
		end = len(items)
	}

	return items[p.Offset:end], end < len(items)
}

func parseSearch(c *gin.Context) (*regexp.Regexp, error) {
	q := strings.TrimSpace(c.Query("search"))
	if q == "" {
		return nil, nil
	}
	re, err := regexp.Compile("(?i)" + q)
	if err != nil {
		return nil, fmt.Errorf("invalid search regex: %w", err)
	}
	return re, nil
}

func parseSort(c *gin.Context) (sortBy, sortDir string) {
	sortBy = c.DefaultQuery("sort_by", "name")
	sortDir = c.DefaultQuery("sort_dir", "asc")

	validSortBy := map[string]bool{"name": true, "partitions": true, "age": true, "status": true}
	if !validSortBy[sortBy] {
		sortBy = "name"
	}
	if sortDir != "asc" && sortDir != "desc" {
		sortDir = "asc"
	}
	return sortBy, sortDir
}

// statusOrder returns a sort bucket: 0=Active, 1=Has Stale, 2=Unused.
func statusOrder(t topicResponse, now int64, staleDays, unusedDays int) int {
	newestAgeDays := float64(now-t.NewestPartitionTimestamp) / 86400
	oldestAgeDays := float64(now-t.OldestPartitionTimestamp) / 86400
	if newestAgeDays >= float64(unusedDays) {
		return 2
	}
	if oldestAgeDays >= float64(staleDays) {
		return 1
	}
	return 0
}

func (s *Server) sortTopicResponses(topics []topicResponse, sortBy, sortDir string) {
	now := time.Now().Unix()
	staleDays := s.cfg.StalePartitionDays
	unusedDays := s.cfg.UnusedTopicDays
	sort.Slice(topics, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "partitions":
			less = topics[i].PartitionCount < topics[j].PartitionCount
		case "age":
			// Ascending = smallest timestamp first (oldest partition age first).
			less = topics[i].OldestPartitionTimestamp < topics[j].OldestPartitionTimestamp
		case "status":
			oi, oj := statusOrder(topics[i], now, staleDays, unusedDays), statusOrder(topics[j], now, staleDays, unusedDays)
			if oi != oj {
				less = oi < oj
			} else {
				less = strings.ToLower(topics[i].Name) < strings.ToLower(topics[j].Name)
			}
		default: // "name"
			less = strings.ToLower(topics[i].Name) < strings.ToLower(topics[j].Name)
		}
		if sortDir == "desc" {
			return !less
		}
		return less
	})
}
