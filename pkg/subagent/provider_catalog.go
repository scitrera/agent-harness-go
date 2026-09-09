// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package subagent

import (
	"context"
	"fmt"
	"sort"
)

// WorkspaceDefinitionProvider loads the authoritative agent definitions for one
// logical workspace. Implementations must not silently fall back to another
// authority when a load fails.
type WorkspaceDefinitionProvider interface {
	LoadWorkspace(ctx context.Context, workspaceID string) ([]Definition, error)
}

// WorkspaceCatalog is the additive multi-workspace catalog capability. Legacy
// Catalog callers use the configured default; turn-aware callers address the
// resolved logical workspace explicitly.
type WorkspaceCatalog interface {
	Catalog
	ListWorkspace(ctx context.Context, workspaceID string) ([]Definition, error)
	GetWorkspace(ctx context.Context, workspaceID string, typ AgentType) (Definition, error)
}

// ProviderCatalog adapts a workspace provider to the existing subagent Catalog.
// A selected logical workspace may be bound to a different backend workspace;
// explicitly addressed other workspaces pass through unchanged.
type ProviderCatalog struct {
	provider           WorkspaceDefinitionProvider
	logicalWorkspace   string
	backendWorkspaceID string
}

func NewProviderCatalog(provider WorkspaceDefinitionProvider, logicalWorkspace, backendWorkspaceID string) (*ProviderCatalog, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: workspace definition provider is required", ErrInvalidDefinition)
	}
	return &ProviderCatalog{provider: provider, logicalWorkspace: logicalWorkspace, backendWorkspaceID: backendWorkspaceID}, nil
}

func (c *ProviderCatalog) List(ctx context.Context) ([]Definition, error) {
	return c.ListWorkspace(ctx, c.logicalWorkspace)
}

func (c *ProviderCatalog) Get(ctx context.Context, typ AgentType) (Definition, error) {
	return c.GetWorkspace(ctx, c.logicalWorkspace, typ)
}

func (c *ProviderCatalog) ListWorkspace(ctx context.Context, workspaceID string) ([]Definition, error) {
	defs, err := c.provider.LoadWorkspace(ctx, c.backendWorkspace(workspaceID))
	if err != nil {
		return nil, err
	}
	defs = append([]Definition(nil), defs...)
	seen := make(map[AgentType]struct{}, len(defs))
	for _, def := range defs {
		if err := def.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := seen[def.Type]; duplicate {
			return nil, fmt.Errorf("%w: duplicate type %s", ErrInvalidDefinition, def.Type)
		}
		seen[def.Type] = struct{}{}
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Type < defs[j].Type })
	return defs, nil
}

func (c *ProviderCatalog) GetWorkspace(ctx context.Context, workspaceID string, typ AgentType) (Definition, error) {
	defs, err := c.ListWorkspace(ctx, workspaceID)
	if err != nil {
		return Definition{}, err
	}
	for _, def := range defs {
		if def.Type == typ {
			return def, nil
		}
	}
	return Definition{}, fmt.Errorf("%w: %s", ErrUnknownAgent, typ)
}

func (c *ProviderCatalog) backendWorkspace(workspaceID string) string {
	if workspaceID == "" || workspaceID == c.logicalWorkspace {
		return c.backendWorkspaceID
	}
	return workspaceID
}

var _ WorkspaceCatalog = (*ProviderCatalog)(nil)
