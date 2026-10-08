package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/RuthvikBathala/mcpporter/internal/audit"
	"github.com/RuthvikBathala/mcpporter/internal/config"
	"github.com/RuthvikBathala/mcpporter/internal/mcp"
)

// mockClient is an in-memory upstream.Client for tests.
type mockClient struct {
	tools   []mcp.Tool
	calls   int
	healthy bool
}

func (m *mockClient) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	return &mcp.InitializeResult{ProtocolVersion: mcp.ProtocolVersion}, nil
}
func (m *mockClient) ListTools(ctx context.Context) ([]mcp.Tool, error) { return m.tools, nil }
func (m *mockClient) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	m.calls++
	if name == "fail" {
		return nil, errors.New("upstream failure")
	}
	raw, _ := json.Marshal(args)
	return mcp.TextResult("called:" + name + ":" + string(raw)), nil
}
func (m *mockClient) Close() error  { return nil }
func (m *mockClient) Healthy() bool { return m.healthy }

func testConfig() *config.Config {
	return &config.Config{
		Gate: config.GateConfig{Name: "test"},
		Upstreams: []config.UpstreamConfig{
			{Name: "demo", Transport: "stdio", Command: "x"},
			{Name: "other", Transport: "stdio", Command: "y"},
		},
		Routes: []config.RouteConfig{
			{Pattern: "demo__echo", Upstream: "demo", Cache: true, CacheTTL: "1m"},
			{Pattern: "demo__limited", Upstream: "demo", RateLimit: &config.RateLimitConfig{PerSecond: 1000, Burst: 1}},
			{Pattern: "*", Upstream: "other"},
		},
	}
}

func newTestGateway(t *testing.T, cfg *config.Config, clients map[string]*mockClient) *Gateway {
	t.Helper()
	auditLog, err := audit.New("")
	if err != nil {
		t.Fatal(err)
	}
	gw := New(cfg, auditLog, "test")
	for name, c := range clients {
		gw.clients[name] = c
	}
	if err := gw.RefreshTools(context.Background()); err != nil {
		t.Fatalf("RefreshTools: %v", err)
	}
	return gw
}

func defaultClients() map[string]*mockClient {
	return map[string]*mockClient{
		"demo": {tools: []mcp.Tool{
			{Name: "echo", InputSchema: map[string]any{}},
			{Name: "limited", InputSchema: map[string]any{}},
			{Name: "fail", InputSchema: map[string]any{}},
		}, healthy: true},
		"other": {tools: []mcp.Tool{
			{Name: "ping", InputSchema: map[string]any{}},
		}, healthy: true},
	}
}

func callRequest(id any, tool string, args map[string]any) *mcp.Request {
	params, _ := json.Marshal(mcp.CallToolParams{Name: tool, Arguments: args})
	return &mcp.Request{JSONRPC: "2.0", ID: id, Method: "tools/call", Params: params}
}

func TestToolsListAggregationAndPrefixing(t *testing.T) {
	gw := newTestGateway(t, testConfig(), defaultClients())
	resp := gw.handleRequest(context.Background(), &mcp.Request{JSONRPC: "2.0", ID: 1, Method: "tools/list"}, "test")
	if resp.Error != nil {
		t.Fatalf("tools/list: %v", resp.Error)
	}
	var out mcp.ToolsListResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tl := range out.Tools {
		names[tl.Name] = true
	}
	for _, want := range []string{"demo__echo", "demo__limited", "demo__fail", "other__ping"} {
		if !names[want] {
			t.Errorf("missing tool %q in %v", want, names)
		}
	}
}

func TestRoutingAndCache(t *testing.T) {
	clients := defaultClients()
	gw := newTestGateway(t, testConfig(), clients)
	ctx := context.Background()

	// First call: miss, hits upstream.
	resp := gw.handleRequest(ctx, callRequest(1, "demo__echo", map[string]any{"x": 1}), "test")
	if resp.Error != nil {
		t.Fatalf("call 1: %v", resp.Error)
	}
	if clients["demo"].calls != 1 {
		t.Fatalf("upstream calls = %d", clients["demo"].calls)
	}

	// Second identical call: cache hit, no new upstream call.
	resp = gw.handleRequest(ctx, callRequest(2, "demo__echo", map[string]any{"x": 1}), "test")
	if resp.Error != nil {
		t.Fatalf("call 2: %v", resp.Error)
	}
	if clients["demo"].calls != 1 {
		t.Errorf("cache miss: upstream calls = %d", clients["demo"].calls)
	}

	// Different args: miss again.
	resp = gw.handleRequest(ctx, callRequest(3, "demo__echo", map[string]any{"x": 2}), "test")
	if resp.Error != nil {
		t.Fatalf("call 3: %v", resp.Error)
	}
	if clients["demo"].calls != 2 {
		t.Errorf("expected 2 upstream calls, got %d", clients["demo"].calls)
	}

	// Uncached route always hits upstream.
	before := clients["other"].calls
	gw.handleRequest(ctx, callRequest(4, "other__ping", nil), "test")
	gw.handleRequest(ctx, callRequest(5, "other__ping", nil), "test")
	if clients["other"].calls != before+2 {
		t.Errorf("uncached route should hit upstream twice, got %d", clients["other"].calls-before)
	}
}

func TestUnknownTool(t *testing.T) {
	gw := newTestGateway(t, testConfig(), defaultClients())
	resp := gw.handleRequest(context.Background(), callRequest(1, "nope__x", nil), "test")
	if resp.Error == nil || resp.Error.Code != mcp.ErrInvalidParams {
		t.Errorf("expected invalid params, got %+v", resp.Error)
	}
}

func TestUpstreamError(t *testing.T) {
	gw := newTestGateway(t, testConfig(), defaultClients())
	resp := gw.handleRequest(context.Background(), callRequest(1, "demo__fail", nil), "test")
	if resp.Error == nil {
		t.Fatal("expected error")
	}
}

func TestRateLimit(t *testing.T) {
	gw := newTestGateway(t, testConfig(), defaultClients())
	ctx := context.Background()
	resp := gw.handleRequest(ctx, callRequest(1, "demo__limited", nil), "test")
	if resp.Error != nil {
		t.Fatalf("first call: %v", resp.Error)
	}
	resp = gw.handleRequest(ctx, callRequest(2, "demo__limited", nil), "test")
	if resp.Error == nil || resp.Error.Code != mcp.ErrRateLimited {
		t.Errorf("expected rate limited, got %+v", resp.Error)
	}
}

func TestInitializeAndPing(t *testing.T) {
	gw := newTestGateway(t, testConfig(), defaultClients())
	ctx := context.Background()
	resp := gw.handleRequest(ctx, &mcp.Request{JSONRPC: "2.0", ID: 1, Method: "initialize"}, "test")
	if resp.Error != nil {
		t.Fatalf("initialize: %v", resp.Error)
	}
	var init mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &init); err != nil {
		t.Fatal(err)
	}
	if init.ServerInfo.Name != "mcpporter" {
		t.Errorf("server name = %q", init.ServerInfo.Name)
	}
	resp = gw.handleRequest(ctx, &mcp.Request{JSONRPC: "2.0", ID: 2, Method: "ping"}, "test")
	if resp.Error != nil {
		t.Fatalf("ping: %v", resp.Error)
	}
	resp = gw.handleRequest(ctx, &mcp.Request{JSONRPC: "2.0", ID: 3, Method: "nope"}, "test")
	if resp.Error == nil || resp.Error.Code != mcp.ErrMethodNotFound {
		t.Errorf("expected method not found, got %+v", resp.Error)
	}
}

func TestRedactSecrets(t *testing.T) {
	in := `{"headers":{"Authorization":"Bearer abc123","X-Custom":"ok"},"args":{"api_key":"k"}}`
	out := redactSecrets(in)
	if strings.Contains(out, "abc123") || strings.Contains(out, `"api_key":"k"`) {
		t.Errorf("secrets not redacted: %s", out)
	}
	if !strings.Contains(out, "REDACTED") {
		t.Errorf("no redaction marker: %s", out)
	}
	if !strings.Contains(out, `"X-Custom":"ok"`) {
		t.Errorf("non-secret clobbered: %s", out)
	}
}

func TestNoPrefixMode(t *testing.T) {
	cfg := testConfig()
	f := false
	cfg.Gate.ToolPrefix = &f
	gw := newTestGateway(t, cfg, defaultClients())
	resp := gw.handleRequest(context.Background(), &mcp.Request{JSONRPC: "2.0", ID: 1, Method: "tools/list"}, "test")
	var out mcp.ToolsListResult
	_ = json.Unmarshal(resp.Result, &out)
	for _, tl := range out.Tools {
		if strings.Contains(tl.Name, "__") {
			t.Errorf("unexpected prefix in %q", tl.Name)
		}
	}
	// In no-prefix mode the "*" route sends everything to "other".
	resp = gw.handleRequest(context.Background(), callRequest(1, "echo", nil), "test")
	if resp.Error != nil {
		t.Fatalf("call: %v", resp.Error)
	}
}

func TestAuditLogWritten(t *testing.T) {
	// Use a real audit file to verify entries land on disk.
	path := t.TempDir() + "/audit.jsonl"
	auditLog, err := audit.New(path)
	if err != nil {
		t.Fatal(err)
	}
	gw := New(testConfig(), auditLog, "test")
	clients := defaultClients()
	for name, c := range clients {
		gw.clients[name] = c
	}
	if err := gw.RefreshTools(context.Background()); err != nil {
		t.Fatal(err)
	}
	gw.handleRequest(context.Background(), callRequest(1, "demo__echo", map[string]any{"secret": "s3cr3t"}), "test")
	_ = auditLog.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "s3cr3t") {
		t.Error("raw secret in audit log")
	}
	if !strings.Contains(string(data), "demo__echo") {
		t.Error("tool name missing from audit log")
	}
}
