package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RuthvikBathala/mcpporter/internal/mcp"
)

// seenRequest records one request a fake stateless server handled.
type seenRequest struct {
	method string
	header http.Header
	meta   map[string]any // params._meta, decoded
}

// fakeStatelessServer is a stateless-era (2026-07-28) Streamable HTTP
// endpoint: it implements server/discover, rejects the initialize
// handshake, requires the per-request _meta envelope on every request,
// and never issues a session.
type fakeStatelessServer struct {
	mu       sync.Mutex
	seen     []seenRequest
	deletes  int
	discover int
}

func (f *fakeStatelessServer) handler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			f.mu.Lock()
			f.deletes++
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		var req mcp.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad", 400)
			return
		}
		if req.IsNotification() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		// Decode params._meta.
		var params struct {
			Meta map[string]any `json:"_meta"`
		}
		_ = json.Unmarshal(req.Params, &params)
		f.mu.Lock()
		f.seen = append(f.seen, seenRequest{method: req.Method, header: r.Header.Clone(), meta: params.Meta})
		if req.Method == "server/discover" {
			f.discover++
		}
		f.mu.Unlock()

		writeErr := func(code int, msg string) {
			raw, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID,
				Error: &mcp.RPCError{Code: code, Message: msg}})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(raw)
		}
		writeResult := func(result any) {
			raw, _ := json.Marshal(result)
			resp, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID, Result: raw})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(resp)
		}

		if req.Method == "initialize" {
			// Stateless servers have no handshake.
			writeErr(mcp.ErrMethodNotFound, "Method not found: initialize")
			return
		}
		// Every stateless request must carry the _meta envelope.
		if params.Meta[mcp.MetaProtocolVersion] != mcp.StatelessProtocolVersion ||
			params.Meta[mcp.MetaClientCapabilities] == nil {
			writeErr(mcp.ErrInvalidParams, "missing required _meta fields")
			return
		}
		switch req.Method {
		case "server/discover":
			var disc mcp.DiscoverResult
			disc.ResultType = "complete"
			disc.SupportedVersions = []string{mcp.StatelessProtocolVersion}
			disc.Capabilities = mcp.Capabilities{Tools: map[string]any{}}
			disc.TTLMs = 3600000
			disc.CacheScope = "public"
			disc.Meta.ServerInfo = &mcp.ServerInfo{Name: "fake-stateless", Version: "1.0.0"}
			writeResult(disc)
		case "tools/list":
			writeResult(mcp.ToolsListResult{
				Tools:      []mcp.Tool{{Name: "ping", InputSchema: map[string]any{}}},
				ResultType: "complete", TTLMs: 60000, CacheScope: "private",
			})
		case "tools/call":
			res := mcp.TextResult("pong")
			res.ResultType = "complete"
			writeResult(res)
		default:
			writeErr(mcp.ErrMethodNotFound, "Method not found: "+req.Method)
		}
	})
	return mux
}

func (f *fakeStatelessServer) methods() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.seen))
	for i, s := range f.seen {
		out[i] = s.method
	}
	return out
}

func (f *fakeStatelessServer) last(method string) *seenRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.seen) - 1; i >= 0; i-- {
		if f.seen[i].method == method {
			s := f.seen[i]
			return &s
		}
	}
	return nil
}

// TestStreamableHTTPStateless exercises the full stateless path:
// discover instead of initialize, per-request _meta, SEP-2243 routing
// headers, and no session anywhere.
func TestStreamableHTTPStateless(t *testing.T) {
	fake := &fakeStatelessServer{}
	srv := httptest.NewServer(fake.handler(t))
	t.Cleanup(srv.Close)

	c, err := NewStreamableHTTP(srv.URL+"/mcp", nil)
	if err != nil {
		t.Fatalf("NewStreamableHTTP: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	init, err := c.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != mcp.StatelessProtocolVersion {
		t.Errorf("protocol version = %q, want %q", init.ProtocolVersion, mcp.StatelessProtocolVersion)
	}
	if init.ServerInfo.Name != "fake-stateless" {
		t.Errorf("server info = %+v, want fake-stateless (from discover _meta)", init.ServerInfo)
	}

	// The handshake must never happen; discover is the opening call.
	for _, m := range fake.methods() {
		if m == "initialize" || m == "notifications/initialized" {
			t.Errorf("stateless upstream saw legacy method %q; methods = %v", m, fake.methods())
		}
	}
	if len(fake.methods()) == 0 || fake.methods()[0] != "server/discover" {
		t.Errorf("first method = %v, want server/discover first", fake.methods())
	}
	if fake.discover != 1 {
		t.Errorf("server/discover called %d times, want 1", fake.discover)
	}

	// The client reports its stateless detection.
	info, ok := c.(StatelessInfo)
	if !ok {
		t.Fatal("streamable client does not implement StatelessInfo")
	}
	if !info.Stateless() {
		t.Error("Stateless() = false, want true")
	}
	if d := info.DiscoverResult(); d == nil || !d.Supports(mcp.StatelessProtocolVersion) {
		t.Errorf("DiscoverResult = %+v, want supported %s", d, mcp.StatelessProtocolVersion)
	}

	tools, err := c.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "ping" {
		t.Fatalf("tools = %+v", tools)
	}
	res, err := c.CallTool(ctx, "ping", nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(res.Content) != 1 || res.Content[0].Text != "pong" {
		t.Errorf("result = %+v", res)
	}

	// Per-request envelope and routing headers on a tools/call.
	call := fake.last("tools/call")
	if call == nil {
		t.Fatal("server never saw tools/call")
	}
	if got := call.header.Get("MCP-Protocol-Version"); got != mcp.StatelessProtocolVersion {
		t.Errorf("MCP-Protocol-Version = %q, want %q", got, mcp.StatelessProtocolVersion)
	}
	if got := call.header.Get("Mcp-Method"); got != "tools/call" {
		t.Errorf("Mcp-Method = %q, want tools/call", got)
	}
	if got := call.header.Get("Mcp-Name"); got != "ping" {
		t.Errorf("Mcp-Name = %q, want ping", got)
	}
	if got := call.meta[mcp.MetaProtocolVersion]; got != mcp.StatelessProtocolVersion {
		t.Errorf("_meta protocolVersion = %v, want %q", got, mcp.StatelessProtocolVersion)
	}
	if call.meta[mcp.MetaClientInfo] == nil {
		t.Error("_meta missing clientInfo")
	}

	// No request may carry a session header in the stateless era.
	for _, s := range fake.seen {
		if got := s.header.Get("Mcp-Session-Id"); got != "" {
			t.Errorf("%s carried Mcp-Session-Id %q; stateless era has no sessions", s.method, got)
		}
	}

	// Close must not try to DELETE a session that never existed.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if fake.deletes != 0 {
		t.Errorf("Close sent %d DELETEs, want 0 in stateless era", fake.deletes)
	}
}

// TestStreamableHTTPStatelessUnsupportedVersion: a stateless server
// that answers discover with UnsupportedProtocolVersionError (-32022)
// listing only versions mcpporter does not speak must produce a clear
// error — and must NOT be mistaken for a legacy server (no initialize
// fallback).
func TestStreamableHTTPStatelessUnsupportedVersion(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad", 400)
			return
		}
		mu.Lock()
		methods = append(methods, req.Method)
		mu.Unlock()
		data, _ := json.Marshal(mcp.UnsupportedVersionData{
			Supported: []string{"2027-01-01"}, Requested: mcp.StatelessProtocolVersion,
		})
		raw, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID, Error: &mcp.RPCError{
			Code: mcp.ErrUnsupportedProtocolVersion, Message: "Unsupported protocol version", Data: data,
		}})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(raw)
	}))
	t.Cleanup(srv.Close)

	c, err := NewStreamableHTTP(srv.URL+"/mcp", nil)
	if err != nil {
		t.Fatalf("NewStreamableHTTP: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.Initialize(ctx)
	if err == nil {
		t.Fatal("Initialize succeeded, want unsupported-version error")
	}
	if !strings.Contains(err.Error(), "2027-01-01") {
		t.Errorf("error = %v, want it to name the supported versions", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, m := range methods {
		if m == "initialize" {
			t.Errorf("client fell back to initialize against a stateless server; methods = %v", methods)
		}
	}
}

// TestStreamableHTTPModernErrorFallsBackToLegacyVersion: a dual-era
// server answering discover with -32022 listing only a legacy version
// mcpporter speaks is retried via the legacy initialize handshake.
func TestStreamableHTTPModernErrorFallsBackToLegacyVersion(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad", 400)
			return
		}
		if req.IsNotification() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		mu.Lock()
		methods = append(methods, req.Method)
		mu.Unlock()
		writeResult := func(result any) {
			raw, _ := json.Marshal(result)
			resp, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID, Result: raw})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(resp)
		}
		switch req.Method {
		case "server/discover":
			data, _ := json.Marshal(mcp.UnsupportedVersionData{
				Supported: []string{mcp.ProtocolVersion}, Requested: mcp.StatelessProtocolVersion,
			})
			raw, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID, Error: &mcp.RPCError{
				Code: mcp.ErrUnsupportedProtocolVersion, Message: "Unsupported protocol version", Data: data,
			}})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(raw)
		case "initialize":
			writeResult(mcp.InitializeResult{ProtocolVersion: mcp.ProtocolVersion,
				ServerInfo: mcp.ServerInfo{Name: "fake-dual", Version: "0.0.1"}})
		case "tools/list":
			writeResult(mcp.ToolsListResult{Tools: []mcp.Tool{{Name: "ping", InputSchema: map[string]any{}}}})
		default:
			raw, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID,
				Error: &mcp.RPCError{Code: mcp.ErrMethodNotFound, Message: "Method not found"}})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(raw)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := NewStreamableHTTP(srv.URL+"/mcp", nil)
	if err != nil {
		t.Fatalf("NewStreamableHTTP: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	init, err := c.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if init.ProtocolVersion != mcp.ProtocolVersion {
		t.Errorf("protocol version = %q, want legacy %q", init.ProtocolVersion, mcp.ProtocolVersion)
	}
	if info, ok := c.(StatelessInfo); ok && info.Stateless() {
		t.Error("Stateless() = true, want false after legacy fallback")
	}
	tools, err := c.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "ping" {
		t.Errorf("tools = %+v", tools)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"server/discover", "initialize", "tools/list"}
	if fmt.Sprint(methods) != fmt.Sprint(want) {
		t.Errorf("methods = %v, want %v", methods, want)
	}
}
