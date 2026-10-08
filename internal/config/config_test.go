package config

import (
	"os"
	"path/filepath"
	"testing"
)

const validYAML = `
gate:
  name: test-gate
  tool_prefix: true
upstreams:
  - name: demo
    transport: stdio
    command: demo-server
    args: ["--stdio"]
    env:
      API_TOKEN: "${TEST_MCPPORTER_TOKEN}"
  - name: remote
    transport: sse
    url: https://example.com/sse
    headers:
      Authorization: "Bearer ${TEST_MCPPORTER_TOKEN}"
routes:
  - pattern: "demo__*"
    upstream: demo
    cache: true
    cache_ttl: 1m
    rate_limit:
      per_second: 10
      burst: 20
  - pattern: "*"
    upstream: remote
admin:
  listen: 127.0.0.1:8080
audit:
  path: /tmp/mcpporter-audit.jsonl
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gate.yaml")
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	t.Setenv("TEST_MCPPORTER_TOKEN", "secret-123")
	cfg, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Gate.Name != "test-gate" {
		t.Errorf("gate name = %q", cfg.Gate.Name)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("upstreams = %d", len(cfg.Upstreams))
	}
	// Env expansion must have resolved ${TEST_MCPPORTER_TOKEN}.
	if cfg.Upstreams[0].Env["API_TOKEN"] != "secret-123" {
		t.Errorf("env not expanded: %q", cfg.Upstreams[0].Env["API_TOKEN"])
	}
	if cfg.Upstreams[1].Headers["Authorization"] != "Bearer secret-123" {
		t.Errorf("header not expanded: %q", cfg.Upstreams[1].Headers["Authorization"])
	}
	if !cfg.Routes[0].Cache || cfg.Routes[0].CacheTTLDuration().String() != "1m0s" {
		t.Errorf("route cache misparsed: %+v", cfg.Routes[0])
	}
	if cfg.Routes[0].RateLimit.PerSecond != 10 || cfg.Routes[0].RateLimit.Burst != 20 {
		t.Errorf("rate limit misparsed: %+v", cfg.Routes[0].RateLimit)
	}
	if cfg.Admin.Listen != "127.0.0.1:8080" {
		t.Errorf("admin listen = %q", cfg.Admin.Listen)
	}
}

func TestLoadValidationErrors(t *testing.T) {
	cases := map[string]string{
		"no upstreams": `
upstreams: []
routes:
  - {pattern: "*", upstream: x}
`,
		"unknown upstream in route": `
upstreams:
  - {name: a, transport: stdio, command: x}
routes:
  - {pattern: "*", upstream: nope}
`,
		"bad transport": `
upstreams:
  - {name: a, transport: grpc, command: x}
routes:
  - {pattern: "*", upstream: a}
`,
		"bad glob": `
upstreams:
  - {name: a, transport: stdio, command: x}
routes:
  - {pattern: "[bad", upstream: a}
`,
		"bad rate limit": `
upstreams:
  - {name: a, transport: stdio, command: x}
routes:
  - {pattern: "*", upstream: a, rate_limit: {per_second: 0, burst: 5}}
`,
		"sse without url": `
upstreams:
  - {name: a, transport: sse}
routes:
  - {pattern: "*", upstream: a}
`,
	}
	for name, y := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTemp(t, y)); err == nil {
				t.Errorf("expected validation error, got nil")
			}
		})
	}
}

func TestDefaults(t *testing.T) {
	y := `
upstreams:
  - {name: a, transport: stdio, command: x}
routes:
  - {pattern: "*", upstream: a}
`
	cfg, err := Load(writeTemp(t, y))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Gate.PrefixEnabled() {
		t.Error("prefix should default to enabled")
	}
	if cfg.Gate.Separator() != "__" {
		t.Errorf("separator = %q", cfg.Gate.Separator())
	}
	if d := cfg.Upstreams[0].TimeoutDuration(); d.String() != "1m0s" {
		t.Errorf("default timeout = %v", d)
	}
}
