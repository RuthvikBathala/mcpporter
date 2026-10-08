// Package admin serves the mcpporter HTTP admin endpoint: /healthz for
// liveness probes and /status for a JSON snapshot of gateway state.
package admin

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/RuthvikBathala/mcpporter/internal/cache"
)

// Provider is the subset of the gateway the admin endpoint needs.
type Provider interface {
	Version() string
	Uptime() time.Duration
	ToolCount() int
	CacheStats() cache.Stats
	UpstreamHealth() map[string]bool
}

// Status is the JSON payload of GET /status.
type Status struct {
	Version   string          `json:"version"`
	Uptime    string          `json:"uptime"`
	Tools     int             `json:"tools"`
	Upstreams map[string]bool `json:"upstreams"`
	Cache     cache.Stats     `json:"cache"`
}

// Server is the admin HTTP server.
type Server struct {
	provider Provider
	server   *http.Server
}

// New returns an admin server bound to addr (e.g. "127.0.0.1:8080").
func New(addr string, p Provider) *Server {
	s := &Server{provider: p}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		st := Status{
			Version:   p.Version(),
			Uptime:    p.Uptime().Round(time.Second).String(),
			Tools:     p.ToolCount(),
			Upstreams: p.UpstreamHealth(),
			Cache:     p.CacheStats(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	s.server = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Start serves until ctx is done or the server fails.
func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() error {
	return s.server.Close()
}
