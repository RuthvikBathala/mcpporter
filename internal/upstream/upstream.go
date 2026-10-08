// Package upstream implements MCP clients for the gateway's upstream
// servers: stdio (spawned subprocess, JSON-RPC over pipes) and SSE
// (server-sent events for responses, HTTP POST for requests).
package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RuthvikBathala/mcpporter/internal/mcp"
)

// Client is an MCP client connection to one upstream server.
type Client interface {
	// Initialize performs the MCP handshake.
	Initialize(ctx context.Context) (*mcp.InitializeResult, error)
	// ListTools returns the upstream's tool catalog.
	ListTools(ctx context.Context) ([]mcp.Tool, error)
	// CallTool invokes a tool by its upstream (unprefixed) name.
	CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error)
	// Close releases the connection.
	Close() error
	// Healthy reports whether the last interaction succeeded.
	Healthy() bool
}

// pending tracks in-flight requests awaiting responses.
type pending struct {
	mu      sync.Mutex
	next    int64
	waiters map[int64]chan *mcp.Response
}

func newPending() *pending {
	return &pending{waiters: map[int64]chan *mcp.Response{}}
}

func (p *pending) add() (int64, chan *mcp.Response) {
	id := atomic.AddInt64(&p.next, 1)
	ch := make(chan *mcp.Response, 1)
	p.mu.Lock()
	p.waiters[id] = ch
	p.mu.Unlock()
	return id, ch
}

func (p *pending) resolve(resp *mcp.Response) {
	var id int64
	switch v := resp.ID.(type) {
	case float64:
		id = int64(v)
	case int64:
		id = v
	case json.Number:
		id, _ = v.Int64()
	default:
		return
	}
	p.mu.Lock()
	ch, ok := p.waiters[id]
	if ok {
		delete(p.waiters, id)
	}
	p.mu.Unlock()
	if ok {
		ch <- resp
	}
}

func (p *pending) failAll(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, ch := range p.waiters {
		ch <- &mcp.Response{
			JSONRPC: "2.0",
			ID:      id,
			Error:   &mcp.RPCError{Code: mcp.ErrInternal, Message: err.Error()},
		}
		delete(p.waiters, id)
	}
}

// roundTrip sends req and waits for the matching response.
func roundTrip(ctx context.Context, p *pending, send func(*mcp.Request) error, method string, params any) (*mcp.Response, error) {
	id, ch := p.add()
	var rawParams json.RawMessage
	if params != nil {
		var err error
		rawParams, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	}
	req := &mcp.Request{JSONRPC: "2.0", ID: id, Method: method, Params: rawParams}
	if err := send(req); err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// stdio
// ---------------------------------------------------------------------------

// stdioClient speaks JSON-RPC 2.0 to a subprocess over its stdin/stdout.
type stdioClient struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	pending *pending
	writeMu sync.Mutex
	healthy atomic.Bool
	done    chan struct{}
}

// NewStdio spawns command with args/env and returns a client. The process's
// stderr is inherited so upstream crashes remain visible.
func NewStdio(command string, args []string, env map[string]string) (Client, error) {
	cmd := exec.Command(command, args...)
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdio stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdio stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("stdio start %q: %w", command, err)
	}
	c := &stdioClient{
		cmd:     cmd,
		stdin:   stdin,
		pending: newPending(),
		done:    make(chan struct{}),
	}
	c.healthy.Store(true)
	go c.readLoop(stdout)
	go c.waitLoop()
	return c, nil
}

func (c *stdioClient) readLoop(r io.Reader) {
	defer close(c.done)
	sc := bufio.NewScanner(r)
	// MCP messages can be large; allow up to 16MB per frame.
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var resp mcp.Response
		if err := json.Unmarshal(line, &resp); err != nil {
			continue
		}
		if resp.ID != nil {
			c.pending.resolve(&resp)
		}
		// Server->client notifications are ignored.
	}
	c.pending.failAll(fmt.Errorf("stdio transport closed"))
}

func (c *stdioClient) waitLoop() {
	_ = c.cmd.Wait()
	c.healthy.Store(false)
}

func (c *stdioClient) send(req *mcp.Request) error {
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	raw = append(raw, '\n')
	_, err = c.stdin.Write(raw)
	return err
}

func (c *stdioClient) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	resp, err := roundTrip(ctx, c.pending, c.send, "initialize", mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		ClientInfo:      mcp.ClientInfo{Name: "mcpporter", Version: "0.1.0"},
	})
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	// Fire-and-forget per the MCP spec.
	_ = c.send(&mcp.Request{JSONRPC: "2.0", Method: "notifications/initialized"})
	c.healthy.Store(true)
	return &out, nil
}

func (c *stdioClient) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	resp, err := roundTrip(ctx, c.pending, c.send, "tools/list", nil)
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.ToolsListResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	c.healthy.Store(true)
	return out.Tools, nil
}

func (c *stdioClient) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	resp, err := roundTrip(ctx, c.pending, c.send, "tools/call", mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.CallToolResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	c.healthy.Store(true)
	return &out, nil
}

func (c *stdioClient) Close() error {
	_ = c.stdin.Close()
	<-c.done
	return nil
}

func (c *stdioClient) Healthy() bool { return c.healthy.Load() }

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

// sseClient implements the MCP SSE transport: GET the SSE endpoint, read the
// "endpoint" event for the message URL, then POST JSON-RPC requests there.
type sseClient struct {
	endpoint string
	headers  map[string]string
	http     *http.Client
	pending  *pending
	healthy  atomic.Bool
	writeMu  sync.Mutex
	cancel   context.CancelFunc
}

// NewSSE connects to an SSE MCP endpoint.
func NewSSE(url string, headers map[string]string) (Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &sseClient{
		headers: headers,
		http:    &http.Client{Timeout: 0}, // streaming; per-request ctxs carry deadlines
		pending: newPending(),
		cancel:  cancel,
	}
	c.healthy.Store(true)
	if err := c.connect(ctx, url); err != nil {
		cancel()
		return nil, err
	}
	return c, nil
}

func (c *sseClient) connect(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("sse connect: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("sse connect: status %d", resp.StatusCode)
	}
	// Read the endpoint event synchronously; the rest streams in the background.
	ep, err := readEndpointEvent(resp.Body)
	if err != nil {
		resp.Body.Close()
		return err
	}
	c.endpoint = resolveURL(url, ep)
	go c.readLoop(resp.Body)
	return nil
}

// readEndpointEvent reads SSE events until the "endpoint" event arrives.
func readEndpointEvent(r io.Reader) (string, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var event string
	deadline := time.Now().Add(10 * time.Second)
	for sc.Scan() {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("sse: timed out waiting for endpoint event")
		}
		line := sc.Text()
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") && event == "endpoint" {
			return strings.TrimSpace(strings.TrimPrefix(line, "data:")), nil
		} else if line == "" {
			event = ""
		}
	}
	return "", fmt.Errorf("sse: stream closed before endpoint event")
}

func resolveURL(base, ref string) string {
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(ref, "/")
}

func (c *sseClient) readLoop(body io.ReadCloser) {
	defer body.Close()
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var event string
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") && event == "message" {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			var resp mcp.Response
			if err := json.Unmarshal([]byte(data), &resp); err == nil && resp.ID != nil {
				c.pending.resolve(&resp)
			}
		} else if line == "" {
			event = ""
		}
	}
	c.pending.failAll(fmt.Errorf("sse stream closed"))
	c.healthy.Store(false)
}

func (c *sseClient) send(req *mcp.Request) error {
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", "application/json")
	for k, v := range c.headers {
		hreq.Header.Set(k, v)
	}
	resp, err := c.http.Do(hreq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("sse post: status %d", resp.StatusCode)
	}
	return nil
}

func (c *sseClient) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	resp, err := roundTrip(ctx, c.pending, c.send, "initialize", mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		ClientInfo:      mcp.ClientInfo{Name: "mcpporter", Version: "0.1.0"},
	})
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	_ = c.send(&mcp.Request{JSONRPC: "2.0", Method: "notifications/initialized"})
	c.healthy.Store(true)
	return &out, nil
}

func (c *sseClient) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	resp, err := roundTrip(ctx, c.pending, c.send, "tools/list", nil)
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.ToolsListResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	c.healthy.Store(true)
	return out.Tools, nil
}

func (c *sseClient) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	resp, err := roundTrip(ctx, c.pending, c.send, "tools/call", mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.CallToolResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	c.healthy.Store(true)
	return &out, nil
}

func (c *sseClient) Close() error {
	c.cancel()
	return nil
}

func (c *sseClient) Healthy() bool { return c.healthy.Load() }
