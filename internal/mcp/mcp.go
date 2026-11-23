// Package mcp implements the JSON-RPC 2.0 framing and the subset of the
// Model Context Protocol (MCP) types that mcpporter needs to act as both a
// server (facing MCP clients) and a client (facing upstream MCP servers).
//
// MCP is JSON-RPC 2.0 under the hood: requests carry an id, notifications
// don't, and responses echo the request id.
package mcp

import (
	"encoding/json"
	"fmt"
)

// JSON-RPC 2.0 error codes.
const (
	ErrParse          = -32700
	ErrInvalidRequest = -32600
	ErrMethodNotFound = -32601
	ErrInvalidParams  = -32602
	ErrInternal       = -32603
	// ErrRateLimited is a server-defined error code mcpporter returns when a
	// tool call is rejected by the rate limiter.
	ErrRateLimited = -32000
	// ErrHeaderMismatch (2026-07-28, SEP-2243): the Streamable HTTP
	// routing headers disagree with the JSON-RPC body.
	ErrHeaderMismatch = -32020
	// ErrUnsupportedProtocolVersion (2026-07-28, SEP-2575): the server
	// does not implement the requested protocol version. The error data
	// carries {"supported": [...], "requested": "..."}.
	ErrUnsupportedProtocolVersion = -32022
)

// ProtocolVersion is the MCP protocol version mcpporter advertises to
// legacy (handshake-era) servers and clients. 2025-03-26 is the revision
// that introduced the Streamable HTTP transport (replacing the legacy
// HTTP+SSE transport).
const ProtocolVersion = "2025-03-26"

// StatelessProtocolVersion is the first stateless ("modern era") MCP
// revision, 2026-07-28. It removes the initialize handshake and
// protocol-level sessions (SEP-2575, SEP-2567): every request carries
// its protocol version and client capabilities in params._meta, and
// servers advertise themselves via the server/discover RPC instead.
const StatelessProtocolVersion = "2026-07-28"

// Reserved params._meta keys for stateless (2026-07-28+) requests and
// results.
const (
	MetaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	MetaClientInfo         = "io.modelcontextprotocol/clientInfo"
	MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	MetaServerInfo         = "io.modelcontextprotocol/serverInfo"
)

// IsStatelessVersion reports whether v is a stateless-era revision
// (2026-07-28 or later). MCP versions are YYYY-MM-DD strings, so a
// lexicographic comparison is a chronological one.
func IsStatelessVersion(v string) bool { return v >= StatelessProtocolVersion }

// ClientCapabilities are the capabilities a client declares. In the
// stateless era they travel in every request's _meta instead of once
// in the initialize handshake.
type ClientCapabilities struct {
	Extensions map[string]any `json:"extensions,omitempty"`
}

// ClientMeta builds the _meta object a stateless request must carry:
// the protocol version and client capabilities (required), plus the
// client identity (SHOULD identify, per SEP-2575).
func ClientMeta(version string) map[string]any {
	return map[string]any{
		MetaProtocolVersion:    version,
		MetaClientCapabilities: ClientCapabilities{},
		MetaClientInfo:         ClientInfo{Name: "mcpporter", Version: "0.1.0"},
	}
}

// WithClientMeta returns params (a struct, map, or nil) re-marshaled
// with the stateless _meta object merged in under the "_meta" key.
func WithClientMeta(params any, version string) (json.RawMessage, error) {
	obj := map[string]any{}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("stateless params must be a JSON object: %w", err)
		}
	}
	obj["_meta"] = ClientMeta(version)
	return json.Marshal(obj)
}

// DiscoverResult is the result of the stateless-era server/discover
// RPC: the replacement for the initialize handshake's advertisement.
// The server identity lives in _meta, not a top-level field.
type DiscoverResult struct {
	ResultType        string       `json:"resultType,omitempty"`
	SupportedVersions []string     `json:"supportedVersions"`
	Capabilities      Capabilities `json:"capabilities"`
	Instructions      string       `json:"instructions,omitempty"`
	// CacheableResult fields (SEP-2549).
	TTLMs      int64  `json:"ttlMs,omitempty"`
	CacheScope string `json:"cacheScope,omitempty"`
	Meta       struct {
		ServerInfo *ServerInfo `json:"io.modelcontextprotocol/serverInfo,omitempty"`
	} `json:"_meta,omitempty"`
}

// ServerInfo returns the server identity from the discover result's
// _meta, or nil if the server did not include one.
func (d *DiscoverResult) ServerInfo() *ServerInfo { return d.Meta.ServerInfo }

// Supports reports whether the server advertises version v.
func (d *DiscoverResult) Supports(v string) bool {
	for _, s := range d.SupportedVersions {
		if s == v {
			return true
		}
	}
	return false
}

// UnsupportedVersionData is the data payload of an
// UnsupportedProtocolVersionError (-32022).
type UnsupportedVersionData struct {
	Supported []string `json:"supported"`
	Requested string   `json:"requested"`
}

// ParseUnsupportedVersion extracts the supported-version list from an
// RPC error, reporting whether it is an UnsupportedProtocolVersionError
// — the "recognized modern error" that identifies a stateless-era
// server during dual-era detection.
func ParseUnsupportedVersion(e *RPCError) ([]string, bool) {
	if e == nil || e.Code != ErrUnsupportedProtocolVersion {
		return nil, false
	}
	var data UnsupportedVersionData
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return nil, true // recognized code, unparsable data
	}
	return data.Supported, true
}

// Request is a JSON-RPC 2.0 request. When ID is nil the message is a
// notification and no response is expected.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the request carries no id.
func (r *Request) IsNotification() bool { return r.ID == nil }

// Response is a JSON-RPC 2.0 response.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError is a JSON-RPC 2.0 error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message)
}

// NewErrorResponse builds an error response for req.
func NewErrorResponse(id any, code int, msg string) *Response {
	return &Response{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &RPCError{Code: code, Message: msg},
	}
}

// NewResultResponse builds a success response for req, marshaling result.
func NewResultResponse(id any, result any) *Response {
	raw, err := json.Marshal(result)
	if err != nil {
		return NewErrorResponse(id, ErrInternal, "failed to marshal result: "+err.Error())
	}
	return &Response{JSONRPC: "2.0", ID: id, Result: raw}
}

// ClientInfo identifies an MCP client during initialization.
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// ServerInfo identifies an MCP server during initialization.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// InitializeParams are sent by the client as the first request.
type InitializeParams struct {
	ProtocolVersion string     `json:"protocolVersion"`
	ClientInfo      ClientInfo `json:"clientInfo"`
}

// InitializeResult is the server's answer to initialize.
type InitializeResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	ServerInfo      ServerInfo   `json:"serverInfo"`
	Capabilities    Capabilities `json:"capabilities"`
}

// Capabilities advertises what the server supports.
type Capabilities struct {
	Tools      map[string]any `json:"tools,omitempty"`
	Resources  map[string]any `json:"resources,omitempty"`
	Prompts    map[string]any `json:"prompts,omitempty"`
	Extensions map[string]any `json:"extensions,omitempty"`
}

// Tool is an MCP tool definition.
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ToolsListResult is the result of tools/list. ResultType, TTLMs and
// CacheScope are stateless-era (2026-07-28) fields; legacy servers omit
// them, and a missing ResultType means "complete".
type ToolsListResult struct {
	Tools      []Tool `json:"tools"`
	ResultType string `json:"resultType,omitempty"`
	TTLMs      int64  `json:"ttlMs,omitempty"`
	CacheScope string `json:"cacheScope,omitempty"`
}

// CallToolParams are the parameters of tools/call.
type CallToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

// ContentBlock is a single block of tool result content.
type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// CallToolResult is the result of tools/call.
type CallToolResult struct {
	Content []ContentBlock `json:"content"`
	IsError bool           `json:"isError,omitempty"`
	// ResultType is a stateless-era (2026-07-28) field: "complete" for
	// ordinary results; absent on legacy results, meaning "complete".
	ResultType string `json:"resultType,omitempty"`
}

// TextResult is a convenience constructor for a single text block result.
func TextResult(text string) *CallToolResult {
	return &CallToolResult{Content: []ContentBlock{{Type: "text", Text: text}}}
}

// ErrorResult is a convenience constructor for a failed tool result.
func ErrorResult(text string) *CallToolResult {
	return &CallToolResult{Content: []ContentBlock{{Type: "text", Text: text}}, IsError: true}
}
