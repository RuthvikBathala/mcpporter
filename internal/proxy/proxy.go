// Package proxy is the mcpporter gateway core: it presents itself as a
// single MCP server to clients while routing tool calls to upstream MCP
// servers according to the configured routes.
package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/RuthvikBathala/mcpporter/internal/audit"
	"github.com/RuthvikBathala/mcpporter/internal/cache"
	"github.com/RuthvikBathala/mcpporter/internal/config"
	"github.com/RuthvikBathala/mcpporter/internal/mcp"
	"github.com/RuthvikBathala/mcpporter/internal/ratelimit"
	"github.com/RuthvikBathala/mcpporter/internal/upstream"
)

// toolRef maps a gateway-visible tool name to its upstream and original name.
type toolRef struct {
	upstream string
	original string
	route    config.RouteConfig
}

// Gateway is an MCP server that proxies to upstream MCP servers.
type Gateway struct {
	cfg      *config.Config
	clients  map[string]upstream.Client
	mu       sync.RWMutex // guards tools and toolDefs
	tools    map[string]toolRef
	toolDefs map[string]mcp.Tool
	cache    *cache.Cache
	audit    *audit.Logger
	limiters map[string]*ratelimit.Limiter // keyed by route pattern
	started  time.Time
	version  string
	debug    bool
}

// New builds a gateway from cfg. Upstream clients are dialed lazily by
// Connect; construct with New, then Connect, then Serve*.
func New(cfg *config.Config, auditLog *audit.Logger, version string) *Gateway {
	g := &Gateway{
		cfg:      cfg,
		clients:  map[string]upstream.Client{},
		tools:    map[string]toolRef{},
		cache:    cache.New(),
		audit:    auditLog,
		limiters: map[string]*ratelimit.Limiter{},
		started:  time.Now(),
		version:  version,
		debug:    cfg.Gate.Debug,
	}
	for _, r := range cfg.Routes {
		if r.RateLimit != nil {
			g.limiters[r.Pattern] = ratelimit.New(r.RateLimit.PerSecond, r.RateLimit.Burst)
		}
	}
	return g
}

// Connect dials every upstream and performs the MCP handshake.
func (g *Gateway) Connect(ctx context.Context) error {
	for _, u := range g.cfg.Upstreams {
		var (
			c   upstream.Client
			err error
		)
		switch u.Transport {
		case "stdio":
			c, err = upstream.NewStdio(u.Command, u.Args, u.Env)
		case "sse":
			c, err = upstream.NewSSE(u.URL, u.Headers)
		case "streamable-http":
			c, err = upstream.NewStreamableHTTP(u.URL, u.Headers)
		}
		if err != nil {
			return fmt.Errorf("dial upstream %q: %w", u.Name, err)
		}
		if _, err := c.Initialize(ctx); err != nil {
			_ = c.Close()
			return fmt.Errorf("upstream %q initialize: %w", u.Name, err)
		}
		g.clients[u.Name] = c
	}
	return g.RefreshTools(ctx)
}

// RefreshTools rebuilds the aggregated tool catalog from all upstreams.
// Tools are prefixed with the upstream name (unless disabled) and only
// tools matching at least one route are exposed.
func (g *Gateway) RefreshTools(ctx context.Context) error {
	index := map[string]toolRef{}
	defs := map[string]mcp.Tool{}
	for _, u := range g.cfg.Upstreams {
		c := g.clients[u.Name]
		tools, err := c.ListTools(ctx)
		if err != nil {
			return fmt.Errorf("upstream %q tools/list: %w", u.Name, err)
		}
		for _, t := range tools {
			name := t.Name
			if g.cfg.Gate.PrefixEnabled() {
				name = u.Name + g.cfg.Gate.Separator() + t.Name
			}
			route := g.matchRoute(name)
			if route == nil {
				// No route matches: the tool stays hidden from clients.
				continue
			}
			index[name] = toolRef{upstream: u.Name, original: t.Name, route: *route}
			t.Name = name
			defs[name] = t
		}
	}
	g.mu.Lock()
	g.tools = index
	g.toolDefs = defs
	g.mu.Unlock()
	return nil
}

// matchRoute returns the first route whose glob matches name.
func (g *Gateway) matchRoute(name string) *config.RouteConfig {
	for i := range g.cfg.Routes {
		r := &g.cfg.Routes[i]
		if ok, _ := filepath.Match(r.Pattern, name); ok {
			return r
		}
	}
	return nil
}

// ServeStdio serves the MCP protocol on stdin/stdout. Logs go to stderr.
func (g *Gateway) ServeStdio(ctx context.Context) error {
	return g.serveStream(ctx, os.Stdin, os.Stdout)
}

func (g *Gateway) serveStream(ctx context.Context, r io.Reader, w io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	bw := bufio.NewWriter(w)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		g.debugDump("client -> gateway", line)
		var req mcp.Request
		if err := json.Unmarshal(line, &req); err != nil {
			resp := mcp.NewErrorResponse(nil, mcp.ErrParse, "parse error")
			g.writeResponse(bw, resp)
			continue
		}
		if req.IsNotification() {
			g.handleNotification(&req)
			continue
		}
		resp := g.handleRequest(ctx, &req, "stdio")
		g.writeResponse(bw, resp)
	}
	return sc.Err()
}

func (g *Gateway) writeResponse(w *bufio.Writer, resp *mcp.Response) {
	raw, err := json.Marshal(resp)
	if err != nil {
		return
	}
	g.debugDump("gateway -> client", raw)
	raw = append(raw, '\n')
	_, _ = w.Write(raw)
	_ = w.Flush()
}

// ServeHTTP serves JSON-RPC over HTTP POST /mcp (one request per POST).
func (g *Gateway) ServeHTTP() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 16*1024*1024))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var req mcp.Request
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, mcp.NewErrorResponse(nil, mcp.ErrParse, "parse error"))
			return
		}
		if req.IsNotification() {
			g.handleNotification(&req)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		resp := g.handleRequest(r.Context(), &req, "http")
		writeJSON(w, resp)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, resp *mcp.Response) {
	w.Header().Set("Content-Type", "application/json")
	raw, _ := json.Marshal(resp)
	_, _ = w.Write(raw)
}

func (g *Gateway) handleNotification(req *mcp.Request) {
	// Currently no client notifications require action.
	_ = req
}

// handleRequest dispatches one JSON-RPC request.
func (g *Gateway) handleRequest(ctx context.Context, req *mcp.Request, client string) *mcp.Response {
	switch req.Method {
	case "initialize":
		return mcp.NewResultResponse(req.ID, mcp.InitializeResult{
			ProtocolVersion: mcp.ProtocolVersion,
			ServerInfo:      mcp.ServerInfo{Name: gatewayName(), Version: g.version},
			Capabilities:    mcp.Capabilities{Tools: map[string]any{}},
		})
	case "ping":
		return mcp.NewResultResponse(req.ID, map[string]any{})
	case "tools/list":
		return g.handleToolsList(req)
	case "tools/call":
		return g.handleToolsCall(ctx, req, client)
	default:
		return mcp.NewErrorResponse(req.ID, mcp.ErrMethodNotFound, "method not found: "+req.Method)
	}
}

func gatewayName() string { return "mcpporter" }

func (g *Gateway) handleToolsList(req *mcp.Request) *mcp.Response {
	g.mu.RLock()
	defer g.mu.RUnlock()
	tools := make([]mcp.Tool, 0, len(g.toolDefs))
	for _, t := range g.toolDefs {
		tools = append(tools, t)
	}
	return mcp.NewResultResponse(req.ID, mcp.ToolsListResult{Tools: tools})
}

func (g *Gateway) handleToolsCall(ctx context.Context, req *mcp.Request, clientID string) *mcp.Response {
	var params mcp.CallToolParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return mcp.NewErrorResponse(req.ID, mcp.ErrInvalidParams, "invalid params: "+err.Error())
	}
	g.mu.RLock()
	ref, ok := g.tools[params.Name]
	g.mu.RUnlock()
	if !ok {
		return mcp.NewErrorResponse(req.ID, mcp.ErrInvalidParams, "unknown tool: "+params.Name)
	}

	start := time.Now()
	auditEntry := audit.Entry{
		Client:   clientID,
		Tool:     params.Name,
		ArgsHash: audit.ArgsHash(params.Arguments),
		Upstream: ref.upstream,
		Cache:    "bypass",
	}

	// Rate limiting.
	if lim, ok := g.limiters[ref.route.Pattern]; ok {
		if !lim.Allow(params.Name) {
			auditEntry.Status = "rate_limited"
			auditEntry.LatencyMs = time.Since(start).Milliseconds()
			g.audit.Log(auditEntry)
			return mcp.NewErrorResponse(req.ID, mcp.ErrRateLimited, "rate limit exceeded for tool "+params.Name)
		}
	}

	// Cache lookup.
	if ref.route.Cache {
		key := cache.Key(params.Name, params.Arguments)
		if raw, hit := g.cache.Get(key); hit {
			auditEntry.Status = "ok"
			auditEntry.Cache = "hit"
			auditEntry.LatencyMs = time.Since(start).Milliseconds()
			g.audit.Log(auditEntry)
			return &mcp.Response{JSONRPC: "2.0", ID: req.ID, Result: raw}
		}
		auditEntry.Cache = "miss"
	}

	c := g.clients[ref.upstream]
	result, err := c.CallTool(ctx, ref.original, params.Arguments)
	auditEntry.LatencyMs = time.Since(start).Milliseconds()
	if err != nil {
		auditEntry.Status = "error"
		g.audit.Log(auditEntry)
		return mcp.NewErrorResponse(req.ID, mcp.ErrInternal, "upstream error: "+err.Error())
	}
	auditEntry.Status = "ok"
	g.audit.Log(auditEntry)

	raw, merr := json.Marshal(result)
	if merr != nil {
		return mcp.NewErrorResponse(req.ID, mcp.ErrInternal, "marshal error: "+merr.Error())
	}
	if ref.route.Cache {
		g.cache.Set(cache.Key(params.Name, params.Arguments), raw, ref.route.CacheTTLDuration())
	}
	return &mcp.Response{JSONRPC: "2.0", ID: req.ID, Result: raw}
}

// Close shuts down upstream connections and the audit log.
func (g *Gateway) Close() error {
	var first error
	for name, c := range g.clients {
		if err := c.Close(); err != nil && first == nil {
			first = fmt.Errorf("close upstream %q: %w", name, err)
		}
	}
	if err := g.audit.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// CacheStats exposes cache counters for the admin endpoint.
func (g *Gateway) CacheStats() cache.Stats { return g.cache.Stats() }

// UpstreamHealth reports per-upstream health.
func (g *Gateway) UpstreamHealth() map[string]bool {
	out := map[string]bool{}
	for name, c := range g.clients {
		out[name] = c.Healthy()
	}
	return out
}

// ToolCount returns the number of aggregated tools.
func (g *Gateway) ToolCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.tools)
}

// Uptime returns how long the gateway has been running.
func (g *Gateway) Uptime() time.Duration { return time.Since(g.started) }

// Version returns the gateway version.
func (g *Gateway) Version() string { return g.version }

// debugDump writes a JSON-RPC frame to stderr with secrets redacted.
func (g *Gateway) debugDump(dir string, raw json.RawMessage) {
	if !g.debug {
		return
	}
	fmt.Fprintf(os.Stderr, "[mcpporter] %s: %s\n", dir, redactSecrets(string(raw)))
}

// redactSecrets masks values of keys that look like secrets.
func redactSecrets(s string) string {
	lower := strings.ToLower(s)
	_ = lower
	// Simple approach: redact JSON string values for sensitive keys.
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	redactValue(v)
	raw, err := json.Marshal(v)
	if err != nil {
		return s
	}
	return string(raw)
}

func redactValue(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			lk := strings.ToLower(k)
			if strings.Contains(lk, "token") || strings.Contains(lk, "secret") ||
				strings.Contains(lk, "password") || strings.Contains(lk, "authorization") ||
				strings.Contains(lk, "api_key") || strings.Contains(lk, "apikey") {
				t[k] = "***REDACTED***"
			} else {
				redactValue(val)
			}
		}
	case []any:
		for _, e := range t {
			redactValue(e)
		}
	}
}
