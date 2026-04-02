package api

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/omersiar/ript/internal/config"
)

func testServer() *Server {
	return &Server{
		cfg: &config.Config{
			DefaultPageSize: 50,
			MaxPageSize:     500,
		},
	}
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
