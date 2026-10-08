package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/RuthvikBathala/mcpporter/internal/mcp"
)

// TestHelperProcess re-execs the test binary as a fake MCP server speaking
// JSON-RPC 2.0 over stdio.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	respond := func(id any, result any) {
		raw, _ := json.Marshal(result)
		resp, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: id, Result: raw})
		w.Write(append(resp, '\n'))
		w.Flush()
	}
	for sc.Scan() {
		var req mcp.Request
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			continue
		}
		if req.IsNotification() {
			continue
		}
		switch req.Method {
		case "initialize":
			respond(req.ID, mcp.InitializeResult{
				ProtocolVersion: mcp.ProtocolVersion,
				ServerInfo:      mcp.ServerInfo{Name: "fake", Version: "0.0.1"},
			})
		case "tools/list":
			respond(req.ID, mcp.ToolsListResult{Tools: []mcp.Tool{
				{Name: "echo", Description: "echoes input", InputSchema: map[string]any{"type": "object"}},
			}})
		case "tools/call":
			var p mcp.CallToolParams
			_ = json.Unmarshal(req.Params, &p)
			if p.Name == "boom" {
				raw, _ := json.Marshal(mcp.CallToolResult{})
				resp, _ := json.Marshal(mcp.Response{
					JSONRPC: "2.0", ID: req.ID,
					Error: &mcp.RPCError{Code: mcp.ErrInternal, Message: "boom"},
				})
				_ = raw
				w.Write(append(resp, '\n'))
				w.Flush()
				continue
			}
			respond(req.ID, mcp.TextResult(fmt.Sprintf("echo:%v", p.Arguments["text"])))
		}
	}
	os.Exit(0)
}

func newFakeStdio(t *testing.T) Client {
	t.Helper()
	c, err := NewStdio(os.Args[0], []string{"-test.run=TestHelperProcess"}, map[string]string{
		"GO_WANT_HELPER_PROCESS": "1",
	})
	if err != nil {
		t.Fatalf("NewStdio: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestStdioInitializeAndList(t *testing.T) {
	c := newFakeStdio(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	tools, err := c.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "echo" {
		t.Fatalf("tools = %+v", tools)
	}
	if !c.Healthy() {
		t.Error("expected healthy")
	}
}

func TestStdioCallTool(t *testing.T) {
	c := newFakeStdio(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	res, err := c.CallTool(ctx, "echo", map[string]any{"text": "hi"})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(res.Content) != 1 || res.Content[0].Text != "echo:hi" {
		t.Errorf("result = %+v", res)
	}
}

func TestStdioCallToolError(t *testing.T) {
	c := newFakeStdio(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := c.CallTool(ctx, "boom", nil); err == nil {
		t.Error("expected error from boom tool")
	}
}

// TestSSEClient exercises the SSE transport against a self-contained fake
// SSE server.
func TestSSEClient(t *testing.T) {
	// Build a self-contained SSE server with a response loop.
	responses := make(chan []byte, 16)
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: endpoint\ndata: %s/messages\n\n", srv.URL)
		w.(http.Flusher).Flush()
		for {
			select {
			case data := <-responses:
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	})
	mux.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad", 400)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		go func() {
			var result any
			switch req.Method {
			case "initialize":
				result = mcp.InitializeResult{ProtocolVersion: mcp.ProtocolVersion,
					ServerInfo: mcp.ServerInfo{Name: "fake-sse", Version: "0.0.1"}}
			case "tools/list":
				result = mcp.ToolsListResult{Tools: []mcp.Tool{{Name: "ping", InputSchema: map[string]any{}}}}
			case "tools/call":
				result = mcp.TextResult("pong")
			}
			raw, _ := json.Marshal(result)
			resp, _ := json.Marshal(mcp.Response{JSONRPC: "2.0", ID: req.ID, Result: raw})
			responses <- resp
		}()
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c, err := NewSSE(srv.URL+"/sse", nil)
	if err != nil {
		t.Fatalf("NewSSE: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
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
	if !strings.Contains(res.Content[0].Text, "pong") {
		t.Errorf("result = %+v", res)
	}
}
