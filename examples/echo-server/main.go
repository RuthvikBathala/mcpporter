// Command echo-server is a tiny demo MCP server used by mcpporter's
// quickstart. It exposes two tools over stdio: echo and time.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func respond(w *bufio.Writer, id any, result any) {
	raw, _ := json.Marshal(result)
	resp, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": json.RawMessage(raw)})
	w.Write(append(resp, '\n'))
	w.Flush()
}

func errResp(w *bufio.Writer, id any, code int, msg string) {
	resp, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": msg},
	})
	w.Write(append(resp, '\n'))
	w.Flush()
}

func main() {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	tools := []map[string]any{
		{
			"name":        "echo",
			"description": "Echoes the input text back.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"text": map[string]any{"type": "string"}},
				"required":   []string{"text"},
			},
		},
		{
			"name":        "time",
			"description": "Returns the current UTC time.",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}

	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		if req.ID == nil {
			continue // notification
		}
		switch req.Method {
		case "initialize":
			respond(w, req.ID, map[string]any{
				"protocolVersion": "2025-03-26",
				"serverInfo":      map[string]any{"name": "echo-server", "version": "0.1.0"},
				"capabilities":    map[string]any{},
			})
		case "tools/list":
			respond(w, req.ID, map[string]any{"tools": tools})
		case "tools/call":
			var p struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(req.Params, &p)
			switch p.Name {
			case "echo":
				respond(w, req.ID, map[string]any{
					"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("echo: %v", p.Arguments["text"])}},
				})
			case "time":
				respond(w, req.ID, map[string]any{
					"content": []map[string]any{{"type": "text", "text": time.Now().UTC().Format(time.RFC3339)}},
				})
			default:
				errResp(w, req.ID, -32602, "unknown tool: "+p.Name)
			}
		default:
			errResp(w, req.ID, -32601, "method not found: "+req.Method)
		}
	}
}
