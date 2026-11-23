// Package upstream implements MCP clients for the gateway's upstream
// servers: stdio (spawned subprocess, JSON-RPC over pipes), SSE (the
// legacy HTTP+SSE transport: server-sent events for responses, HTTP
// POST for requests), and Streamable HTTP (the transport that replaced
// HTTP+SSE in MCP 2025-03-26: a single HTTP endpoint where every
// message is a POST and the reply is plain JSON or an SSE stream).
package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

// ---------------------------------------------------------------------------
// Streamable HTTP

// ---------------------------------------------------------------------------
// Streamable HTTP
// ---------------------------------------------------------------------------

// upstreamEra is the protocol era a Streamable HTTP upstream speaks,
// detected once by Initialize and cached for the life of the client
// (the spec makes era a property of the server, not of a request).
type upstreamEra int

const (
	eraUnknown upstreamEra = iota
	// eraLegacy: handshake era (2025-11-25 and earlier) — initialize
	// handshake, optional Mcp-Session-Id session.
	eraLegacy
	// eraStateless: modern era (2026-07-28+) — no handshake, no
	// session; every request carries its version and capabilities in
	// params._meta (SEP-2575, SEP-2567).
	eraStateless
)

// rpcFailure is a failed Streamable HTTP call: either a non-2xx HTTP
// status or a JSON-RPC error, with the parsed RPC error preserved so
// callers can recognize spec-defined errors (notably
// UnsupportedProtocolVersionError, -32022) during era detection.
type rpcFailure struct {
	status int           // HTTP status, 0 if the call never got a response
	rpc    *mcp.RPCError // parsed JSON-RPC error, if the body carried one
	body   string        // raw body excerpt for non-RPC failures
}

func (e *rpcFailure) Error() string {
	if e.rpc != nil {
		return fmt.Sprintf("streamable-http post: status %d: %s", e.status, e.rpc.Error())
	}
	if e.body != "" {
		return fmt.Sprintf("streamable-http post: status %d: %s", e.status, e.body)
	}
	return fmt.Sprintf("streamable-http post: status %d", e.status)
}

// rpcErrorOf extracts the JSON-RPC error from an error returned by the
// Streamable HTTP transport, if there is one.
func rpcErrorOf(err error) *mcp.RPCError {
	var rf *rpcFailure
	if errors.As(err, &rf) {
		return rf.rpc
	}
	var re *mcp.RPCError
	if errors.As(err, &re) {
		return re
	}
	return nil
}

// StatelessInfo is optionally implemented by upstream clients that can
// speak the stateless (2026-07-28+) protocol era. It lets callers such
// as the proxy inspect the era negotiated with the upstream.
type StatelessInfo interface {
	// Stateless reports whether the upstream was detected as a
	// stateless-era server (server/discover, no handshake/session).
	Stateless() bool
	// DiscoverResult returns the upstream's server/discover result,
	// or nil if the upstream is legacy or Initialize has not run.
	DiscoverResult() *mcp.DiscoverResult
}

// streamableHTTPClient implements the MCP Streamable HTTP transport as
// a dual-era client:
//
//   - Legacy era (2025-03-26 and other handshake revisions): every
//     JSON-RPC message is an HTTP POST to a single endpoint, the reply
//     is plain JSON or an SSE stream, and if the server assigns a
//     session during initialize (Mcp-Session-Id response header) the
//     client echoes it on every later request and terminates it with
//     DELETE on Close.
//   - Stateless era (2026-07-28+): no initialize handshake and no
//     session. Initialize probes with server/discover; later requests
//     carry the protocol version and client capabilities in
//     params._meta, plus the SEP-2243 routing headers (MCP-Protocol-
//     Version, Mcp-Method, Mcp-Name).
//
// Era detection follows the spec's backward-compatibility rules for
// Streamable HTTP: attempt a modern request (server/discover) and
// inspect failures — a recognized modern error
// (UnsupportedProtocolVersionError) identifies a stateless server,
// anything else identifies a legacy server and the client falls back
// to the initialize handshake. The verdict is cached on the client.
type streamableHTTPClient struct {
	url     string
	headers map[string]string
	http    *http.Client
	healthy atomic.Bool
	seq     int64 // request id counter; use atomic.AddInt64

	mu       sync.Mutex
	session  string // Mcp-Session-Id, legacy era only
	era      upstreamEra
	version  string              // protocol version in use (per era)
	discover *mcp.DiscoverResult // stateless era only
}

// NewStreamableHTTP returns a client for a Streamable HTTP MCP endpoint,
// e.g. https://mcp.example.com/mcp. No connection is made here — the
// transport is request/response, so Initialize issues the first POST
// (a server/discover probe, falling back to the initialize handshake
// for legacy servers).
func NewStreamableHTTP(url string, headers map[string]string) (Client, error) {
	if url == "" {
		return nil, fmt.Errorf("streamable-http: url is required")
	}
	c := &streamableHTTPClient{
		url:     url,
		headers: headers,
		http:    &http.Client{Timeout: 0}, // per-request ctxs carry deadlines
		era:     eraUnknown,
		version: mcp.ProtocolVersion,
	}
	c.healthy.Store(true)
	return c, nil
}

func (c *streamableHTTPClient) sessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

func (c *streamableHTTPClient) setSession(id string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	// Stateless-era servers have no sessions; never adopt one.
	if c.era != eraStateless {
		c.session = id
	}
	c.mu.Unlock()
}

// mode returns the protocol version and era to speak on the next
// request: stateless once a stateless upstream has been detected,
// legacy otherwise (including before Initialize).
func (c *streamableHTTPClient) mode() (version string, stateless bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.era == eraStateless {
		return c.version, true
	}
	return mcp.ProtocolVersion, false
}

// Stateless implements StatelessInfo.
func (c *streamableHTTPClient) Stateless() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.era == eraStateless
}

// DiscoverResult implements StatelessInfo.
func (c *streamableHTTPClient) DiscoverResult() *mcp.DiscoverResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.discover
}

// newRequest builds a POST (or DELETE) request with the headers the
// transport requires. Legacy requests name the negotiated protocol
// version and echo the session, if any. Stateless requests also carry
// the SEP-2243 routing headers (Mcp-Method always, Mcp-Name when the
// method has a name parameter) and never a session header.
func (c *streamableHTTPClient) newRequest(ctx context.Context, httpMethod, body, version string, stateless bool, rpcMethod, name string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, httpMethod, c.url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", version)
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	if stateless {
		req.Header.Set("Mcp-Method", rpcMethod)
		if name != "" {
			req.Header.Set("Mcp-Name", name)
		}
	} else if sid := c.sessionID(); sid != "" {
		req.Header.Set("Mcp-Session-Id", sid)
	}
	return req, nil
}

// call performs one request/response round trip in the given era:
// POST the request, then read the matching response from either a
// JSON body or an SSE stream. In stateless mode the request params
// gain the per-request _meta envelope (protocol version, client
// capabilities, client info).
func (c *streamableHTTPClient) call(ctx context.Context, method, name string, params any, version string, stateless bool) (*mcp.Response, error) {
	id := atomic.AddInt64(&c.seq, 1)
	var rawParams json.RawMessage
	if stateless {
		var err error
		rawParams, err = mcp.WithClientMeta(params, version)
		if err != nil {
			return nil, err
		}
	} else if params != nil {
		var err error
		rawParams, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(&mcp.Request{JSONRPC: "2.0", ID: id, Method: method, Params: rawParams})
	if err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodPost, string(raw), version, stateless, method, name)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("streamable-http post: %w", err)
	}
	defer resp.Body.Close()
	c.setSession(resp.Header.Get("Mcp-Session-Id"))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		fail := &rpcFailure{status: resp.StatusCode, body: strings.TrimSpace(string(body))}
		var errResp mcp.Response
		if json.Unmarshal(body, &errResp) == nil && errResp.Error != nil {
			fail.rpc = errResp.Error
		}
		return nil, fail
	}
	var out *mcp.Response
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		out, err = readSSEResponse(resp.Body, id)
	} else {
		var decoded mcp.Response
		err = json.NewDecoder(resp.Body).Decode(&decoded)
		out = &decoded
	}
	if err != nil {
		return nil, fmt.Errorf("streamable-http response: %w", err)
	}
	if out.Error != nil {
		return nil, &rpcFailure{status: resp.StatusCode, rpc: out.Error}
	}
	return out, nil
}

// do performs a round trip in the client's currently detected era.
func (c *streamableHTTPClient) do(ctx context.Context, method, name string, params any) (*mcp.Response, error) {
	version, stateless := c.mode()
	return c.call(ctx, method, name, params, version, stateless)
}

// readSSEResponse scans an SSE stream for the response matching id.
// Other messages on the stream (e.g. notifications) are ignored.
func readSSEResponse(r io.Reader, id int64) (*mcp.Response, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var data []string
	flush := func() (*mcp.Response, bool) {
		if len(data) == 0 {
			return nil, false
		}
		joined := strings.Join(data, "\n")
		data = nil
		var resp mcp.Response
		if err := json.Unmarshal([]byte(joined), &resp); err != nil || resp.ID == nil {
			return nil, false
		}
		if n, ok := resp.ID.(float64); !ok || int64(n) != id {
			return nil, false
		}
		return &resp, true
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		case line == "":
			if resp, ok := flush(); ok {
				return resp, nil
			}
		}
	}
	if resp, ok := flush(); ok {
		return resp, nil
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("stream closed before response to request %d", id)
}

// notify sends a JSON-RPC notification (legacy era only — the
// stateless era has no notifications/initialized). The server answers
// 202 Accepted with no body; any 2xx is success.
func (c *streamableHTTPClient) notify(ctx context.Context, method string) error {
	raw, err := json.Marshal(&mcp.Request{JSONRPC: "2.0", Method: method})
	if err != nil {
		return err
	}
	req, err := c.newRequest(ctx, http.MethodPost, string(raw), mcp.ProtocolVersion, false, method, "")
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	c.setSession(resp.Header.Get("Mcp-Session-Id"))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("streamable-http notification: status %d", resp.StatusCode)
	}
	return nil
}

// Initialize detects the upstream's protocol era and performs whatever
// opening exchange that era requires: a server/discover probe for
// stateless (2026-07-28+) servers, or the classic initialize handshake
// for legacy servers. The returned InitializeResult is synthesized
// from the discover result in the stateless era, so existing callers
// (proxy, validate) work unchanged against both eras.
func (c *streamableHTTPClient) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	if res, handled, err := c.tryStateless(ctx); handled {
		if err != nil {
			c.healthy.Store(false)
			return nil, err
		}
		c.healthy.Store(true)
		return res, nil
	}
	return c.initializeLegacy(ctx)
}

// tryStateless probes the upstream with server/discover at the
// stateless protocol version. handled=false means the upstream is a
// legacy server and the caller should fall back to initialize.
func (c *streamableHTTPClient) tryStateless(ctx context.Context) (res *mcp.InitializeResult, handled bool, err error) {
	resp, err := c.call(ctx, "server/discover", "", nil, mcp.StatelessProtocolVersion, true)
	if err != nil {
		supported, recognized := mcp.ParseUnsupportedVersion(rpcErrorOf(err))
		if !recognized {
			// Per the spec's dual-era rules for Streamable HTTP, a
			// failure without a recognized modern error body
			// identifies a legacy server.
			return nil, false, nil
		}
		// A recognized modern error: the server is stateless-era but
		// does not speak our stateless version. Retry is only
		// possible if it also advertises a legacy version we speak.
		for _, v := range supported {
			if v == mcp.StatelessProtocolVersion {
				break
			}
			if !mcp.IsStatelessVersion(v) {
				return nil, false, nil
			}
		}
		return nil, true, fmt.Errorf("streamable-http: upstream supports protocol versions %v, none compatible with mcpporter (stateless %s, legacy %s)",
			supported, mcp.StatelessProtocolVersion, mcp.ProtocolVersion)
	}
	var disc mcp.DiscoverResult
	if err := json.Unmarshal(resp.Result, &disc); err != nil {
		// A success response we cannot parse is not a legacy signal;
		// surface it rather than masking it with a handshake attempt.
		return nil, true, fmt.Errorf("streamable-http: parse server/discover result: %w", err)
	}
	if len(disc.SupportedVersions) == 0 {
		// No version advertisement (e.g. a legacy server answering
		// unknown methods with an empty result): not evidence of a
		// stateless server, so fall back to the handshake.
		return nil, false, nil
	}
	if !disc.Supports(mcp.StatelessProtocolVersion) {
		if disc.Supports(mcp.ProtocolVersion) {
			// Dual-era server advertising our legacy version: let the
			// caller use the handshake.
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("streamable-http: upstream supports protocol versions %v, none compatible with mcpporter (stateless %s, legacy %s)",
			disc.SupportedVersions, mcp.StatelessProtocolVersion, mcp.ProtocolVersion)
	}
	c.mu.Lock()
	c.era = eraStateless
	c.version = mcp.StatelessProtocolVersion
	c.discover = &disc
	c.session = ""
	c.mu.Unlock()
	out := &mcp.InitializeResult{
		ProtocolVersion: mcp.StatelessProtocolVersion,
		Capabilities:    disc.Capabilities,
	}
	if si := disc.ServerInfo(); si != nil {
		out.ServerInfo = *si
	}
	return out, true, nil
}

// initializeLegacy performs the handshake-era opening exchange:
// initialize, then the notifications/initialized notification.
func (c *streamableHTTPClient) initializeLegacy(ctx context.Context) (*mcp.InitializeResult, error) {
	resp, err := c.call(ctx, "initialize", "", mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		ClientInfo:      mcp.ClientInfo{Name: "mcpporter", Version: "0.1.0"},
	}, mcp.ProtocolVersion, false)
	if err != nil {
		c.healthy.Store(false)
		return nil, err
	}
	var out mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.era = eraLegacy
	if out.ProtocolVersion != "" {
		c.version = out.ProtocolVersion
	}
	c.mu.Unlock()
	// Fire-and-forget per the MCP spec.
	_ = c.notify(ctx, "notifications/initialized")
	c.healthy.Store(true)
	return &out, nil
}

func (c *streamableHTTPClient) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	resp, err := c.do(ctx, "tools/list", "", nil)
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

func (c *streamableHTTPClient) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	resp, err := c.do(ctx, "tools/call", name, mcp.CallToolParams{Name: name, Arguments: args})
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

func (c *streamableHTTPClient) Close() error {
	// Legacy era only: if the server assigned a session, terminate it
	// explicitly, as the spec prescribes; failure is not fatal.
	// Stateless-era servers have no session to terminate.
	if c.sessionID() != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if req, err := c.newRequest(ctx, http.MethodDelete, "", mcp.ProtocolVersion, false, "", ""); err == nil {
			if resp, err := c.http.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}
	c.healthy.Store(false)
	return nil
}

func (c *streamableHTTPClient) Healthy() bool { return c.healthy.Load() }
