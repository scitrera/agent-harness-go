// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// mcpRegistryNamePrefix is the fixed prefix for MCP tool registry names. The
// scheme is "mcp.<server>.<tool>" — identical to the names RegisterMCPTool is
// wired with, so switching a server from static registration to the dynamic
// MCPProvider leaves tool identities unchanged.
const mcpRegistryNamePrefix = "mcp."

// mcpRegistryName returns the registry name for a (server, tool) pair. It is the
// single source of truth for the MCP tool naming scheme shared by
// RegisterMCPTool callers and MCPProvider.
func mcpRegistryName(server, tool string) string {
	return mcpRegistryNamePrefix + server + "." + tool
}

// mcpParseRegistryName is the best-effort inverse of mcpRegistryName. It is only
// a fallback: server or tool names may themselves contain dots, so MCPProvider
// resolves invocations through a map built during Tools() and consults this
// parser only when a name isn't in that map. Splits on the first dot after the
// prefix (server, then the remainder as tool).
func mcpParseRegistryName(name string) (server, tool string, ok bool) {
	rest, found := strings.CutPrefix(name, mcpRegistryNamePrefix)
	if !found {
		return "", "", false
	}
	server, tool, found = strings.Cut(rest, ".")
	if !found || server == "" || tool == "" {
		return "", "", false
	}
	return server, tool, true
}

// mcpLister lists a server's tools. *mcp.Manager satisfies it (and MCPCaller);
// MCPProvider type-asserts its Caller to this so the config surface stays the
// existing MCPCaller.
type mcpLister interface {
	ListTools(ctx context.Context, server string) ([]mcp.Tool, error)
}

// MCPProviderConfig configures an MCPProvider. Caller is the same MCPCaller used
// by RegisterMCPTool (a *mcp.Manager also satisfies the internal lister used for
// discovery). Servers is the explicit set of MCP server names to expose.
type MCPProviderConfig struct {
	Caller  MCPCaller
	Servers []string
}

// MCPProvider exposes configured MCP servers' tools as a dynamic, per-turn tool
// provider (structurally a turn ToolProvider: ID/Tools/Invoke). It replaces the
// static per-tool RegisterMCPTool model while keeping identical tool names,
// schemas, and result shapes so the swap is behavior-identical. Trust is left at
// the descriptor zero value (TrustDefault): gating is the pipeline's job.
type MCPProvider struct {
	caller  MCPCaller
	servers []string

	mu    sync.Mutex
	route map[string]mcpRoute // registry name -> (server, tool), refreshed by Tools
}

type mcpRoute struct {
	server string
	tool   string
}

// NewMCPProvider builds an MCPProvider from cfg.
func NewMCPProvider(cfg MCPProviderConfig) *MCPProvider {
	return &MCPProvider{
		caller:  cfg.Caller,
		servers: append([]string(nil), cfg.Servers...),
		route:   map[string]mcpRoute{},
	}
}

// ID identifies the provider.
func (p *MCPProvider) ID() string { return "mcp" }

// mcpEffect maps a server's annotations to a portable effect class. The mapping
// is deliberately one-directional: an explicit readOnlyHint:true becomes read,
// and EVERYTHING else becomes execute — the conservative class — because
// annotations are the server's own claim about itself. A server that wants its
// tool auto-approved by the read-only tier has to say so explicitly; a server
// that says nothing does not get the benefit of the doubt.
func mcpEffect(t mcp.Tool) spec.ToolEffect {
	if t.Annotations.IsReadOnly() {
		return spec.ToolEffectRead
	}
	return spec.ToolEffectExecute
}

// Tools lists every configured server's tools and converts each mcp.Tool to a
// Descriptor using the mcpRegistryName scheme. A per-server ListTools error is
// logged and that server skipped (best-effort, mirroring the manager's own
// tolerance); other servers still contribute. It also refreshes the internal
// registry-name -> (server, tool) route map used by Invoke.
func (p *MCPProvider) Tools(ctx context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) ([]Descriptor, error) {
	lister, ok := p.caller.(mcpLister)
	if !ok {
		return nil, fmt.Errorf("%w: mcp caller does not support ListTools", ErrInvalidTool)
	}
	var descs []Descriptor
	route := map[string]mcpRoute{}
	for _, server := range p.servers {
		mcpTools, err := lister.ListTools(ctx, server)
		if err != nil {
			slog.Warn("mcp provider: list tools failed", "server", server, "err", err)
			continue
		}
		for _, t := range mcpTools {
			name := mcpRegistryName(server, t.Name)
			descs = append(descs, Descriptor{
				Name:        name,
				Description: t.Description,
				Parameters:  t.InputSchema,
				Effect:      mcpEffect(t),
			})
			route[name] = mcpRoute{server: server, tool: t.Name}
		}
	}
	p.mu.Lock()
	p.route = route
	p.mu.Unlock()
	return descs, nil
}

// Invoke resolves the registry name back to (server, tool) — via the route map
// built by Tools(), falling back to mcpParseRegistryName — calls CallTool, and
// converts the result identically to RegisterMCPTool's handler.
func (p *MCPProvider) Invoke(ctx context.Context, req Request) (Result, error) {
	server, tool, ok := p.resolve(req.Name)
	if !ok {
		return Result{}, fmt.Errorf("%w: %s", ErrUnknownTool, req.Name)
	}
	result, err := p.caller.CallTool(ctx, server, tool, req.Arguments)
	if err != nil {
		return Result{}, fmt.Errorf("invoke mcp tool: %w", err)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return Result{}, fmt.Errorf("marshal mcp result: %w", err)
	}
	return NewJSONResult(req.CallID, req.Name, payload)
}

func (p *MCPProvider) resolve(name string) (server, tool string, ok bool) {
	p.mu.Lock()
	r, found := p.route[name]
	p.mu.Unlock()
	if found {
		return r.server, r.tool, true
	}
	return mcpParseRegistryName(name)
}
