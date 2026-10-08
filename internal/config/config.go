// Package config loads and validates mcpporter gate.yaml files.
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root of a gate.yaml file.
type Config struct {
	Gate      GateConfig       `yaml:"gate"`
	Upstreams []UpstreamConfig `yaml:"upstreams"`
	Routes    []RouteConfig    `yaml:"routes"`
	Admin     AdminConfig      `yaml:"admin"`
	Audit     AuditConfig      `yaml:"audit"`
}

// GateConfig holds gateway-wide settings.
type GateConfig struct {
	Name string `yaml:"name"`
	// Debug dumps JSON-RPC frames to stderr (with secrets redacted).
	Debug bool `yaml:"debug"`
	// ToolPrefix controls whether upstream tool names are prefixed with the
	// upstream name to avoid collisions (e.g. github__create_issue).
	// Defaults to true.
	ToolPrefix *bool `yaml:"tool_prefix"`
	// PrefixSeparator separates the upstream name from the tool name.
	// Defaults to "__".
	PrefixSeparator string `yaml:"prefix_separator"`
}

// PrefixEnabled reports whether tool prefixing is on (default true).
func (g GateConfig) PrefixEnabled() bool {
	if g.ToolPrefix == nil {
		return true
	}
	return *g.ToolPrefix
}

// Separator returns the configured prefix separator (default "__").
func (g GateConfig) Separator() string {
	if g.PrefixSeparator == "" {
		return "__"
	}
	return g.PrefixSeparator
}

// UpstreamConfig describes one upstream MCP server.
type UpstreamConfig struct {
	Name string `yaml:"name"`
	// Transport is "stdio" or "sse".
	Transport string `yaml:"transport"`
	// Stdio fields.
	Command string            `yaml:"command"`
	Args    []string          `yaml:"args"`
	Env     map[string]string `yaml:"env"`
	// SSE fields.
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	// Timeout for a single request, e.g. "30s". Defaults to 60s.
	Timeout string `yaml:"timeout"`
}

// RouteConfig maps tool-name globs to an upstream.
type RouteConfig struct {
	// Pattern is a glob matched against the gateway-visible tool name,
	// e.g. "github__*" or "*". First match wins.
	Pattern string `yaml:"pattern"`
	// Upstream names the upstream server to route to.
	Upstream string `yaml:"upstream"`
	// Cache enables result caching for this route.
	Cache bool `yaml:"cache"`
	// CacheTTL is how long results are cached, e.g. "5m". Default 5m.
	CacheTTL string `yaml:"cache_ttl"`
	// RateLimit optionally rate-limits this route.
	RateLimit *RateLimitConfig `yaml:"rate_limit"`
}

// RateLimitConfig is a token bucket: PerSecond refill rate, Burst capacity.
type RateLimitConfig struct {
	PerSecond float64 `yaml:"per_second"`
	Burst     int     `yaml:"burst"`
}

// AdminConfig configures the HTTP admin endpoint. Empty Listen disables it.
type AdminConfig struct {
	Listen string `yaml:"listen"`
}

// AuditConfig configures audit logging.
type AuditConfig struct {
	Path string `yaml:"path"`
}

// Load reads, parses, expands environment variables in, and validates a
// gate.yaml file. ${VAR} and $VAR in upstream env values and headers are
// expanded from the gateway's own environment — this is how secrets reach
// upstreams without ever appearing in client configs.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	expandEnv(&c)
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &c, nil
}

// expandEnv resolves ${VAR} references in upstream env and headers.
func expandEnv(c *Config) {
	for i := range c.Upstreams {
		for k, v := range c.Upstreams[i].Env {
			c.Upstreams[i].Env[k] = os.ExpandEnv(v)
		}
		for k, v := range c.Upstreams[i].Headers {
			c.Upstreams[i].Headers[k] = os.ExpandEnv(v)
		}
	}
}

// Validate checks the config for consistency.
func (c *Config) Validate() error {
	if len(c.Upstreams) == 0 {
		return fmt.Errorf("at least one upstream is required")
	}
	seen := map[string]bool{}
	for i, u := range c.Upstreams {
		if u.Name == "" {
			return fmt.Errorf("upstreams[%d]: name is required", i)
		}
		if seen[u.Name] {
			return fmt.Errorf("upstreams[%d]: duplicate name %q", i, u.Name)
		}
		seen[u.Name] = true
		switch u.Transport {
		case "stdio":
			if u.Command == "" {
				return fmt.Errorf("upstream %q: command is required for stdio transport", u.Name)
			}
		case "sse":
			if u.URL == "" {
				return fmt.Errorf("upstream %q: url is required for sse transport", u.Name)
			}
		default:
			return fmt.Errorf("upstream %q: transport must be stdio or sse, got %q", u.Name, u.Transport)
		}
		if u.Timeout != "" {
			if _, err := time.ParseDuration(u.Timeout); err != nil {
				return fmt.Errorf("upstream %q: invalid timeout %q: %w", u.Name, u.Timeout, err)
			}
		}
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("at least one route is required")
	}
	for i, r := range c.Routes {
		if r.Pattern == "" {
			return fmt.Errorf("routes[%d]: pattern is required", i)
		}
		if _, err := filepath.Match(r.Pattern, ""); err != nil {
			return fmt.Errorf("routes[%d]: invalid glob pattern %q: %w", i, r.Pattern, err)
		}
		if !seen[r.Upstream] {
			return fmt.Errorf("routes[%d]: unknown upstream %q", i, r.Upstream)
		}
		if r.CacheTTL != "" {
			if _, err := time.ParseDuration(r.CacheTTL); err != nil {
				return fmt.Errorf("routes[%d]: invalid cache_ttl %q: %w", i, r.CacheTTL, err)
			}
		}
		if r.RateLimit != nil {
			if r.RateLimit.PerSecond <= 0 {
				return fmt.Errorf("routes[%d]: rate_limit.per_second must be positive", i)
			}
			if r.RateLimit.Burst <= 0 {
				return fmt.Errorf("routes[%d]: rate_limit.burst must be positive", i)
			}
		}
	}
	return nil
}

// Timeout returns the upstream request timeout (default 60s).
func (u UpstreamConfig) TimeoutDuration() time.Duration {
	if u.Timeout == "" {
		return 60 * time.Second
	}
	d, _ := time.ParseDuration(u.Timeout) // validated already
	return d
}

// CacheTTLDuration returns the route cache TTL (default 5m).
func (r RouteConfig) CacheTTLDuration() time.Duration {
	if r.CacheTTL == "" {
		return 5 * time.Minute
	}
	d, _ := time.ParseDuration(r.CacheTTL) // validated already
	return d
}
