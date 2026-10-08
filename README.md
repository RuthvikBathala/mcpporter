# mcpporter

[![CI](https://github.com/RuthvikBathala/mcpporter/actions/workflows/ci.yml/badge.svg)](https://github.com/RuthvikBathala/mcpporter/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.21%2B-blue)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**The control plane for MCP servers. One gateway, every tool — with routing, secret injection, caching, audit logging, and rate limiting.**

## The problem

MCP adoption looks like this in practice: a dozen servers configured per client, API keys pasted into every client config, no idea which tools are actually called, and no way to share one setup across Claude Code, Claude Desktop, and Cursor. Every client holds secrets; nothing is observable.

mcpporter sits between your MCP clients and your upstream servers and fixes that:

```
┌─────────────┐      ┌──────────────────────────────────────────────┐      ┌──────────────┐
│ Claude Code │      │                  mcpporter                   │      │    github    │
│ Claude      ├─────►│  routing · auth injection · cache · audit ·   ├─────►│   postgres   │
│ Desktop     │ stdio│  rate limiting · admin API                   │ stdio│  filesystem  │
│ Cursor      │      └──────────────────────────────────────────────┘  SSE └──────────────┘
└─────────────┘                         │
                                        ▼
                                  /healthz /status
```

- **One endpoint** — clients connect once; the gateway aggregates `tools/list` across all upstreams (with collision-safe `server__tool` prefixing).
- **Secrets stay server-side** — upstream API keys are injected from the gateway's environment (`${GITHUB_TOKEN}`); clients never hold them.
- **Caching** — repeated tool calls are served from cache with per-route TTLs.
- **Audit log** — every call is logged as JSONL (arguments are hashed, never stored raw).
- **Rate limiting** — per-route token buckets return proper MCP errors when exceeded.
- **Admin API** — `/healthz` and `/status` with uptime, upstream health, and cache stats.

## Quickstart

```bash
# Build both binaries
go build -o /tmp/mcpporter ./cmd/mcpporter
go build -o /tmp/echo-server ./examples/echo-server

# Check your config and upstream connectivity
/tmp/mcpporter validate --config examples/gate.yaml
# ✓ demo                 2 tools

# Start the gateway (MCP over stdio, admin on :18080)
/tmp/mcpporter serve --config examples/gate.yaml
```

Point any MCP client at it (example `mcp.json` for Claude Code):

```json
{
  "mcpServers": {
    "porter": { "command": "/tmp/mcpporter", "args": ["serve", "--config", "/path/to/gate.yaml"] }
  }
}
```

Try it manually:

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","clientInfo":{"name":"t","version":"0"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"demo__echo","arguments":{"text":"hello"}}}' \
| /tmp/mcpporter serve --config examples/gate.yaml 2>/dev/null
```

## Configuration

`gate.yaml` reference:

```yaml
gate:
  name: my-gate
  tool_prefix: true        # demo__echo instead of echo (default true)
  prefix_separator: "__"   # default "__"

upstreams:
  - name: github
    transport: stdio
    command: npx
    args: ["-y", "@modelcontextprotocol/server-github"]
    env:
      GITHUB_PERSONAL_ACCESS_TOKEN: "${GITHUB_TOKEN}"  # injected from gateway env
  - name: remote
    transport: sse
    url: https://mcp.example.com/sse
    headers:
      Authorization: "Bearer ${MCP_TOKEN}"

routes:
  - pattern: "github__*"   # glob on the gateway-visible tool name; first match wins
    upstream: github
    cache: true
    cache_ttl: 5m
    rate_limit: { per_second: 10, burst: 30 }
  - pattern: "*"
    upstream: remote

admin:
  listen: 127.0.0.1:8080  # GET /healthz, GET /status (empty = disabled)

audit:
  path: /var/log/mcpporter.jsonl  # JSONL; args are SHA-256 hashed, never raw
```

CLI:

```
mcpporter serve --config gate.yaml [--debug]   # run the gateway (stdio)
mcpporter validate --config gate.yaml          # check config + upstream health
mcpporter version
```

`--debug` dumps JSON-RPC frames to stderr with secret-like values redacted.

## Admin API

- `GET /healthz` → `{"ok":true}`
- `GET /status` → version, uptime, tool count, per-upstream health, cache stats:

```json
{"version":"0.1.0","uptime":"2h3m4s","tools":42,
 "upstreams":{"github":true,"remote":false},
 "cache":{"hits":120,"misses":35,"size":28}}
```

## Architecture

```
cmd/mcpporter/      CLI (serve / validate / version)
internal/
  mcp/              JSON-RPC 2.0 framing + MCP types (initialize, tools/list, tools/call)
  config/           gate.yaml loading, ${ENV} expansion, validation
  upstream/         MCP clients: stdio (subprocess) and SSE transports
  proxy/            gateway core: aggregation, prefixing, routing, cache,
                    rate limiting, audit, debug redaction; stdio + HTTP serving
  cache/            TTL cache keyed by (tool, canonical args hash)
  audit/            append-only JSONL audit log (args hashed, never raw)
  ratelimit/        per-tool token buckets
  admin/            /healthz + /status HTTP endpoint
examples/
  gate.yaml         runnable example config
  echo-server/      tiny demo MCP server (Go, stdio)
```

The gateway speaks raw JSON-RPC 2.0 directly — no MCP SDK dependency — so the
entire dependency tree is `yaml.v3` + `cobra`.

## Roadmap

- Streamable HTTP upstream transport
- Hot-reload of `gate.yaml` on SIGHUP
- Per-client API keys on the gateway side
- Prometheus `/metrics`
- Tool approval policies (require confirmation for destructive tools)

## Contributing

Issues and PRs welcome. Run the full check before pushing:

```bash
go build ./... && go vet ./... && go test -race -count=1 ./...
```

## License

MIT — see [LICENSE](LICENSE).
