package mcp

import (
	"encoding/json"
	"testing"
)

func TestIsStatelessVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"2026-07-28": true,
		"2027-01-01": true,
		"2025-11-25": false,
		"2025-03-26": false,
		"2024-11-05": false,
	} {
		if got := IsStatelessVersion(v); got != want {
			t.Errorf("IsStatelessVersion(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestWithClientMeta(t *testing.T) {
	raw, err := WithClientMeta(CallToolParams{Name: "ping", Arguments: map[string]any{"x": 1}}, StatelessProtocolVersion)
	if err != nil {
		t.Fatalf("WithClientMeta: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if obj["name"] != "ping" {
		t.Errorf("params lost: %v", obj)
	}
	meta, ok := obj["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("_meta = %v, want object", obj["_meta"])
	}
	if meta[MetaProtocolVersion] != StatelessProtocolVersion {
		t.Errorf("meta protocolVersion = %v", meta[MetaProtocolVersion])
	}
	if meta[MetaClientCapabilities] == nil {
		t.Error("meta missing clientCapabilities")
	}
	if meta[MetaClientInfo] == nil {
		t.Error("meta missing clientInfo")
	}

	// nil params still produce a params object carrying only _meta
	// (the server/discover shape).
	raw, err = WithClientMeta(nil, StatelessProtocolVersion)
	if err != nil {
		t.Fatalf("WithClientMeta(nil): %v", err)
	}
	obj = map[string]any{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal nil: %v", err)
	}
	if len(obj) != 1 || obj["_meta"] == nil {
		t.Errorf("nil params -> %v, want only _meta", obj)
	}
}

func TestParseUnsupportedVersion(t *testing.T) {
	data, _ := json.Marshal(UnsupportedVersionData{
		Supported: []string{"2026-07-28", "2025-11-25"}, Requested: "1900-01-01",
	})
	supported, ok := ParseUnsupportedVersion(&RPCError{
		Code: ErrUnsupportedProtocolVersion, Message: "Unsupported protocol version", Data: data,
	})
	if !ok {
		t.Fatal("ParseUnsupportedVersion: not recognized")
	}
	if len(supported) != 2 || supported[0] != "2026-07-28" {
		t.Errorf("supported = %v", supported)
	}
	if _, ok := ParseUnsupportedVersion(&RPCError{Code: ErrMethodNotFound}); ok {
		t.Error("method-not-found must not be recognized as unsupported-version")
	}
	if _, ok := ParseUnsupportedVersion(nil); ok {
		t.Error("nil error must not be recognized")
	}
}

func TestDiscoverResultParsing(t *testing.T) {
	// The exact shape from the 2026-07-28 server/discover spec page:
	// identity in _meta, cache hints as top-level fields.
	raw := []byte(`{
		"resultType": "complete",
		"supportedVersions": ["2026-07-28"],
		"capabilities": {"tools": {}, "resources": {}},
		"_meta": {"io.modelcontextprotocol/serverInfo": {"name": "ExampleServer", "version": "1.0.0"}},
		"instructions": "weather tools",
		"ttlMs": 3600000,
		"cacheScope": "public"
	}`)
	var d DiscoverResult
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !d.Supports("2026-07-28") || d.Supports("2025-03-26") {
		t.Errorf("Supports wrong for %v", d.SupportedVersions)
	}
	if si := d.ServerInfo(); si == nil || si.Name != "ExampleServer" {
		t.Errorf("ServerInfo = %+v", si)
	}
	if d.TTLMs != 3600000 || d.CacheScope != "public" {
		t.Errorf("cache hints = %d/%q", d.TTLMs, d.CacheScope)
	}
}
