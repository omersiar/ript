package api

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/omersiar/ript/internal/config"
)

func testServer() *Server {
	return &Server{
		cfg: &config.Config{
			DefaultPageSize:    50,
			MaxPageSize:        500,
			StalePartitionDays: 7,
			UnusedTopicDays:    30,
			StaticFilesDir:     "./web/static",
		},
	}
}

func daysAgo(days int64) int64 {
	return time.Now().Unix() - (days * 86400)
}

func TestParsePaginationDefaults(t *testing.T) {
	s := testServer()
	c := newPaginationContext("")

	p, err := s.parsePagination(c)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p.Page != 1 {
		t.Fatalf("expected page 1, got %d", p.Page)
	}
	if p.Limit != s.cfg.DefaultPageSize {
		t.Fatalf("expected default limit %d, got %d", s.cfg.DefaultPageSize, p.Limit)
	}
	if p.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", p.Offset)
	}
}

func TestParsePaginationClampsLimit(t *testing.T) {
	s := testServer()
	c := newPaginationContext("page=2&limit=900")

	p, err := s.parsePagination(c)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p.Limit != s.cfg.MaxPageSize {
		t.Fatalf("expected clamped limit %d, got %d", s.cfg.MaxPageSize, p.Limit)
	}
	if p.Offset != s.cfg.MaxPageSize {
		t.Fatalf("expected offset %d, got %d", s.cfg.MaxPageSize, p.Offset)
	}
}

func TestParsePaginationInvalid(t *testing.T) {
	s := testServer()
	tests := []string{
		"page=0",
		"page=x",
		"limit=0",
		"limit=abc",
	}

	for _, query := range tests {
		c := newPaginationContext(query)
		if _, err := s.parsePagination(c); err == nil {
			t.Fatalf("expected error for query %q", query)
		}
	}
}

func TestPaginateTopicResponses(t *testing.T) {
	items := []topicResponse{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
		{Name: "d"},
	}

	page, hasMore := paginateTopicResponses(items, pagination{Page: 2, Limit: 2, Offset: 2})
	if len(page) != 2 {
		t.Fatalf("expected 2 items, got %d", len(page))
	}
	if page[0].Name != "c" || page[1].Name != "d" {
		t.Fatalf("unexpected page results: %+v", page)
	}
	if hasMore {
		t.Fatal("expected hasMore=false on final page")
	}

	empty, hasMore := paginateTopicResponses(items, pagination{Page: 5, Limit: 2, Offset: 8})
	if len(empty) != 0 {
		t.Fatalf("expected empty page, got %d items", len(empty))
	}
	if hasMore {
		t.Fatal("expected hasMore=false for empty out-of-range page")
	}
}

func TestParseThresholdsDefaults(t *testing.T) {
	s := testServer()
	c := newPaginationContext("")

	thresholds, err := s.parseThresholds(c)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if thresholds.StaleDays != s.cfg.StalePartitionDays {
		t.Fatalf("expected stale %d, got %d", s.cfg.StalePartitionDays, thresholds.StaleDays)
	}
	if thresholds.UnusedDays != s.cfg.UnusedTopicDays {
		t.Fatalf("expected unused %d, got %d", s.cfg.UnusedTopicDays, thresholds.UnusedDays)
	}
}

func TestParseThresholdsInvalidRelation(t *testing.T) {
	s := testServer()
	c := newPaginationContext("stale_days=20&unused_days=10")

	_, err := s.parseThresholds(c)
	if err == nil {
		t.Fatal("expected error for unused < stale")
	}
	if !strings.Contains(err.Error(), "unused_days") {
		t.Fatalf("expected unused_days relation error, got %v", err)
	}
}

func TestParseThresholdsRejectsInvalidValue(t *testing.T) {
	s := testServer()
	c := newPaginationContext("stale_days=bad")

	_, err := s.parseThresholds(c)
	if err == nil {
		t.Fatal("expected invalid stale_days error")
	}
	if !strings.Contains(err.Error(), "stale_days") {
		t.Fatalf("expected stale_days error, got %v", err)
	}
}

func TestStatusOrderUsesProvidedThresholds(t *testing.T) {
	now := time.Now().Unix()
	topic := topicResponse{
		Name:                     "threshold-topic",
		PartitionCount:           1,
		OldestPartitionTimestamp: daysAgo(12),
		NewestPartitionTimestamp: daysAgo(12),
	}

	if got := statusOrder(topic, now, 7, 30); got != 1 {
		t.Fatalf("expected Has Stale status bucket=1, got %d", got)
	}
	if got := statusOrder(topic, now, 15, 30); got != 0 {
		t.Fatalf("expected Active status bucket=0 with higher stale threshold, got %d", got)
	}
}

func TestSortTopicResponsesByAgeUsesDeterministicNameTieBreak(t *testing.T) {
	s := testServer()
	topics := []topicResponse{
		{Name: "beta", OldestPartitionTimestamp: daysAgo(10), NewestPartitionTimestamp: daysAgo(10)},
		{Name: "alpha", OldestPartitionTimestamp: daysAgo(10), NewestPartitionTimestamp: daysAgo(10)},
		{Name: "gamma", OldestPartitionTimestamp: daysAgo(2), NewestPartitionTimestamp: daysAgo(2)},
	}

	s.sortTopicResponses(topics, "age", "desc", 7, 30)

	got := []string{topics[0].Name, topics[1].Name, topics[2].Name}
	want := []string{"gamma", "alpha", "beta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected topic order: got %v want %v", got, want)
	}
}

func TestHandleUIConfig(t *testing.T) {
	s := testServer()
	s.Initialize()

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/ui-config", nil)
	s.router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var payload struct {
		StaleDays  int `json:"stale_days"`
		UnusedDays int `json:"unused_days"`
		PageSize   int `json:"page_size"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload.StaleDays != s.cfg.StalePartitionDays {
		t.Fatalf("expected stale_days %d, got %d", s.cfg.StalePartitionDays, payload.StaleDays)
	}
	if payload.UnusedDays != s.cfg.UnusedTopicDays {
		t.Fatalf("expected unused_days %d, got %d", s.cfg.UnusedTopicDays, payload.UnusedDays)
	}
	if payload.PageSize != s.cfg.DefaultPageSize {
		t.Fatalf("expected page_size %d, got %d", s.cfg.DefaultPageSize, payload.PageSize)
	}
}

func TestStaticFilesDirIsConfigurable(t *testing.T) {
	staticDir := t.TempDir()
	dashboardPath := filepath.Join(staticDir, "dashboard.html")
	assetPath := filepath.Join(staticDir, "test.txt")

	if err := os.WriteFile(dashboardPath, []byte("custom-dashboard"), 0o644); err != nil {
		t.Fatalf("failed to write dashboard fixture: %v", err)
	}
	if err := os.WriteFile(assetPath, []byte("custom-asset"), 0o644); err != nil {
		t.Fatalf("failed to write asset fixture: %v", err)
	}

	s := &Server{
		cfg: &config.Config{
			DefaultPageSize:    50,
			MaxPageSize:        500,
			StalePartitionDays: 7,
			UnusedTopicDays:    30,
			StaticFilesDir:     staticDir,
		},
	}
	s.Initialize()

	dashboardResp := httptest.NewRecorder()
	dashboardReq := httptest.NewRequest("GET", "/", nil)
	s.router.ServeHTTP(dashboardResp, dashboardReq)
	if dashboardResp.Code != 200 {
		t.Fatalf("expected dashboard status 200, got %d", dashboardResp.Code)
	}
	if !strings.Contains(dashboardResp.Body.String(), "custom-dashboard") {
		t.Fatalf("expected dashboard body to contain custom content, got %q", dashboardResp.Body.String())
	}

	assetResp := httptest.NewRecorder()
	assetReq := httptest.NewRequest("GET", "/assets/test.txt", nil)
	s.router.ServeHTTP(assetResp, assetReq)
	if assetResp.Code != 200 {
		t.Fatalf("expected asset status 200, got %d", assetResp.Code)
	}
	if strings.TrimSpace(assetResp.Body.String()) != "custom-asset" {
		t.Fatalf("expected asset body custom-asset, got %q", assetResp.Body.String())
	}
}

func newPaginationContext(query string) *gin.Context {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	url := "/api/topics"
	if query != "" {
		url += "?" + query
	}
	c.Request = httptest.NewRequest("GET", url, nil)
	return c
}
