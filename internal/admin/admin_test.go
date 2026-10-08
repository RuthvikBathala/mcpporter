package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/RuthvikBathala/mcpporter/internal/cache"
)

type fakeProvider struct{}

func (fakeProvider) Version() string         { return "test-1.0" }
func (fakeProvider) Uptime() time.Duration   { return 90 * time.Second }
func (fakeProvider) ToolCount() int          { return 7 }
func (fakeProvider) CacheStats() cache.Stats { return cache.Stats{Hits: 3, Misses: 1, Size: 2} }
func (fakeProvider) UpstreamHealth() map[string]bool {
	return map[string]bool{"demo": true, "down": false}
}

func TestHealthz(t *testing.T) {
	s := New("127.0.0.1:0", fakeProvider{})
	// Exercise the mux directly without binding a port.
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
	var body map[string]bool
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || !body["ok"] {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestStatus(t *testing.T) {
	s := New("127.0.0.1:0", fakeProvider{})
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	var st Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Version != "test-1.0" || st.Tools != 7 {
		t.Errorf("status = %+v", st)
	}
	if !st.Upstreams["demo"] || st.Upstreams["down"] {
		t.Errorf("upstreams = %+v", st.Upstreams)
	}
	if st.Cache.Hits != 3 {
		t.Errorf("cache = %+v", st.Cache)
	}
}
