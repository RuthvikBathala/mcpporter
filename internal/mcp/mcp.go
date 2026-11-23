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
)

// ProtocolVersion is the MCP protocol version mcpporter advertises.
// 2025-03-26 is the revision that introduced the Streamable HTTP
// transport (replacing the legacy HTTP+SSE transport).
const ProtocolVersion = "2025-03-26"

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
	Tools map[string]any `json:"tools,omitempty"`
}

// Tool is an MCP tool definition.
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ToolsListResult is the result of tools/list.
type ToolsListResult struct {
	Tools []Tool `json:"tools"`
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
}

// TextResult is a convenience constructor for a single text block result.
func TextResult(text string) *CallToolResult {
	return &CallToolResult{Content: []ContentBlock{{Type: "text", Text: text}}}
}

// ErrorResult is a convenience constructor for a failed tool result.
func ErrorResult(text string) *CallToolResult {
	return &CallToolResult{Content: []ContentBlock{{Type: "text", Text: text}}, IsError: true}
}
