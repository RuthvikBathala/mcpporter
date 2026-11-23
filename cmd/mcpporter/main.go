// Command mcpporter is the MCP control plane: a gateway that aggregates
// upstream MCP servers behind one MCP endpoint with routing, auth injection,
// caching, audit logging, and rate limiting.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/RuthvikBathala/mcpporter/internal/admin"
	"github.com/RuthvikBathala/mcpporter/internal/audit"
	"github.com/RuthvikBathala/mcpporter/internal/config"
	"github.com/RuthvikBathala/mcpporter/internal/proxy"
	"github.com/RuthvikBathala/mcpporter/internal/upstream"
)

// version is stamped at build time: -ldflags "-X main.version=0.1.0".
var version = "dev"

var (
	configPath string
	debugFlag  bool
)

func main() {
	root := &cobra.Command{
		Use:   "mcpporter",
		Short: "MCP control plane: one gateway for all your MCP servers",
		Long: `mcpporter sits between MCP clients (Claude Code, Claude Desktop, …)
and your upstream MCP servers, adding routing, secret injection, caching,
audit logging, and rate limiting — without clients ever holding secrets.`,
		SilenceUsage: true,
	}
	root.PersistentFlags().StringVar(&configPath, "config", "gate.yaml", "path to gate.yaml")
	root.PersistentFlags().BoolVar(&debugFlag, "debug", false, "dump JSON-RPC frames to stderr (secrets redacted)")

	root.AddCommand(serveCmd(), validateCmd(), versionCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadConfig() *config.Config {
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcpporter: %v\n", err)
		os.Exit(1)
	}
	if debugFlag {
		cfg.Gate.Debug = true
	}
	return cfg
}

func serveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the gateway (MCP over stdio)",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := loadConfig()
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			auditLog, err := audit.New(cfg.Audit.Path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "mcpporter: audit log: %v\n", err)
				os.Exit(1)
			}

			gw := proxy.New(cfg, auditLog, version)
			if err := gw.Connect(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "mcpporter: %v\n", err)
				os.Exit(1)
			}
			defer gw.Close()

			if cfg.Admin.Listen != "" {
				srv := admin.New(cfg.Admin.Listen, gw)
				go func() {
					fmt.Fprintf(os.Stderr, "mcpporter: admin on http://%s\n", cfg.Admin.Listen)
					if err := srv.Start(); err != nil {
						fmt.Fprintf(os.Stderr, "mcpporter: admin: %v\n", err)
					}
				}()
				defer srv.Shutdown()
			}

			fmt.Fprintf(os.Stderr, "mcpporter: serving %d tools on stdio\n", gw.ToolCount())
			if err := gw.ServeStdio(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "mcpporter: serve: %v\n", err)
				os.Exit(1)
			}
		},
	}
}

func validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate gate.yaml and check upstream connectivity",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := loadConfig()
			ctx := context.Background()
			ok := true
			for _, u := range cfg.Upstreams {
				var (
					c   upstream.Client
					err error
				)
				switch u.Transport {
				case "stdio":
					c, err = upstream.NewStdio(u.Command, u.Args, u.Env)
				case "sse":
					c, err = upstream.NewSSE(u.URL, u.Headers)
				case "streamable-http":
					c, err = upstream.NewStreamableHTTP(u.URL, u.Headers)
				}
				if err != nil {
					fmt.Printf("✗ %-20s dial: %v\n", u.Name, err)
					ok = false
					continue
				}
				if _, err := c.Initialize(ctx); err != nil {
					fmt.Printf("✗ %-20s initialize: %v\n", u.Name, err)
					_ = c.Close()
					ok = false
					continue
				}
				tools, err := c.ListTools(ctx)
				_ = c.Close()
				if err != nil {
					fmt.Printf("✗ %-20s tools/list: %v\n", u.Name, err)
					ok = false
					continue
				}
				fmt.Printf("✓ %-20s %d tools\n", u.Name, len(tools))
			}
			fmt.Printf("%d routes, %d upstreams\n", len(cfg.Routes), len(cfg.Upstreams))
			if !ok {
				os.Exit(1)
			}
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the mcpporter version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("mcpporter %s\n", version)
		},
	}
}
