package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

// workspaceCatalogRuntime compiles one immutable skill/MCP surface per logical
// workspace. Successful entries live for the process lifetime; a failed cold
// load is not cached, so that workspace retries on its next turn. The fallback
// is always the local filesystem catalog and is never borrowed from another
// workspace.
type workspaceCatalogRuntime struct {
	provider      catalog.WorkspaceProvider
	workspaceRoot string
	localSkills   []catalog.SkillSpec
	localWarnings []string

	newToolProvider func([]catalog.MCPServerSpec) (turn.ToolProvider, error)

	mu       sync.Mutex
	entries  map[string]*workspaceCatalogSlot
	fallback *workspaceCatalogEntry
}

type workspaceCatalogSlot struct {
	mu    sync.Mutex
	entry *workspaceCatalogEntry
}

type workspaceCatalogEntry struct {
	registry *skills.Registry
	prompt   contextpack.SkillCatalog
	tools    turn.ToolProvider
}

type catalogTurnEntry struct {
	workspaceID string
	entry       *workspaceCatalogEntry
}

type catalogTurnEntryKey struct{}

func newWorkspaceCatalogRuntime(provider catalog.WorkspaceProvider, workspaceRoot string, localSkills []catalog.SkillSpec, localWarnings []string) *workspaceCatalogRuntime {
	runtime := &workspaceCatalogRuntime{
		provider:        provider,
		workspaceRoot:   workspaceRoot,
		localSkills:     catalog.MergeSkills(nil, localSkills),
		localWarnings:   append([]string(nil), localWarnings...),
		newToolProvider: newCatalogMCPProvider,
		entries:         map[string]*workspaceCatalogSlot{},
	}
	runtime.fallback, _ = runtime.compile(catalog.Catalog{Skills: runtime.localSkills})
	return runtime
}

func (r *workspaceCatalogRuntime) slot(workspaceID string) *workspaceCatalogSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	slot := r.entries[workspaceID]
	if slot == nil {
		slot = &workspaceCatalogSlot{}
		r.entries[workspaceID] = slot
	}
	return slot
}

func (r *workspaceCatalogRuntime) resolve(ctx context.Context, workspaceID string) (*workspaceCatalogEntry, error) {
	if r == nil {
		return nil, errors.New("workspace catalog runtime is nil")
	}
	if r.provider == nil {
		return r.fallback, nil
	}
	slot := r.slot(workspaceID)
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.entry != nil {
		return slot.entry, nil
	}
	remote, err := r.provider.LoadWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	remote.Skills = catalog.MergeSkills(remote.Skills, r.localSkills)
	entry, err := r.compile(remote)
	if err != nil {
		return nil, err
	}
	slot.entry = entry
	return entry, nil
}

func (r *workspaceCatalogRuntime) compile(resolved catalog.Catalog) (*workspaceCatalogEntry, error) {
	registry := skills.BuildRegistry(resolved.Skills, r.workspaceRoot)
	provider, err := r.newToolProvider(resolved.MCPServers)
	if err != nil {
		return nil, err
	}
	return &workspaceCatalogEntry{
		registry: registry,
		prompt: contextpack.SkillCatalog{
			Skills:       skillSummaries(resolved.Skills),
			Bodies:       registry.Body,
			LoadWarnings: append([]string(nil), r.localWarnings...),
		},
		tools: provider,
	}, nil
}

// decorate resolves the catalog after the runner has resolved the logical
// workspace. A cold provider failure is explicitly fail-open to local skills;
// the failure is not cached and never substitutes another workspace's entry.
func (r *workspaceCatalogRuntime) decorate(ctx context.Context, addr protocol.MessageAddress) context.Context {
	entry, err := r.resolve(ctx, addr.WorkspaceID)
	if err != nil {
		slog.WarnContext(ctx, "workspace catalog load failed; using local filesystem catalog",
			slog.String("workspace", addr.WorkspaceID), slog.Any("err", err))
		entry = r.fallback
	}
	ctx = context.WithValue(ctx, catalogTurnEntryKey{}, catalogTurnEntry{workspaceID: addr.WorkspaceID, entry: entry})
	ctx = skills.WithRegistry(ctx, entry.registry)
	return contextpack.WithSkillCatalog(ctx, entry.prompt)
}

func (r *workspaceCatalogRuntime) entryFor(ctx context.Context, workspaceID string) *workspaceCatalogEntry {
	if turnEntry, ok := ctx.Value(catalogTurnEntryKey{}).(catalogTurnEntry); ok && turnEntry.workspaceID == workspaceID && turnEntry.entry != nil {
		return turnEntry.entry
	}
	entry, err := r.resolve(ctx, workspaceID)
	if err != nil {
		slog.WarnContext(ctx, "workspace catalog tool resolution failed; omitting remote tools",
			slog.String("workspace", workspaceID), slog.Any("err", err))
		return r.fallback
	}
	return entry
}

type workspaceCatalogToolProvider struct{ runtime *workspaceCatalogRuntime }

func (p workspaceCatalogToolProvider) ID() string { return "workspace-catalog-mcp" }

func (p workspaceCatalogToolProvider) Tools(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) ([]tools.Descriptor, error) {
	entry := p.runtime.entryFor(ctx, addr.WorkspaceID)
	if entry == nil || entry.tools == nil {
		return nil, nil
	}
	return entry.tools.Tools(ctx, addr, user)
}

func (p workspaceCatalogToolProvider) Invoke(ctx context.Context, req tools.Request) (tools.Result, error) {
	entry := p.runtime.entryFor(ctx, req.Addr.WorkspaceID)
	if entry == nil || entry.tools == nil {
		return tools.Result{}, fmt.Errorf("%w: %s", tools.ErrUnknownTool, req.Name)
	}
	return entry.tools.Invoke(ctx, req)
}

func newCatalogMCPProvider(servers []catalog.MCPServerSpec) (turn.ToolProvider, error) {
	if len(servers) == 0 {
		return nil, nil
	}
	manager := mcp.NewManager(time.Now)
	names := make([]string, 0, len(servers))
	seen := make(map[string]struct{}, len(servers))
	for _, server := range servers {
		if _, duplicate := seen[server.Name]; duplicate {
			return nil, fmt.Errorf("duplicate MCP server %q", server.Name)
		}
		seen[server.Name] = struct{}{}
		if err := manager.Register(mcp.ServerConfig{
			Name: server.Name, Command: server.Command, Args: server.Args,
			Env: server.Env, IdleTTL: server.IdleTTL(),
		}); err != nil {
			return nil, fmt.Errorf("register catalog MCP server %q: %w", server.Name, err)
		}
		names = append(names, server.Name)
	}
	return tools.NewMCPProvider(tools.MCPProviderConfig{Caller: manager, Servers: names}), nil
}

func composeContextDecorators(first, second func(context.Context, protocol.MessageAddress) context.Context) func(context.Context, protocol.MessageAddress) context.Context {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return func(ctx context.Context, addr protocol.MessageAddress) context.Context {
		return second(first(ctx, addr), addr)
	}
}

var _ turn.ToolProvider = workspaceCatalogToolProvider{}
