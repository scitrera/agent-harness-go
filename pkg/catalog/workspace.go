// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalog

import "context"

type revisionContextKey struct{}

// WithRevision binds the immutable catalog snapshot admitted for this session
// or task lineage. An empty revision is a no-op for legacy providers.
func WithRevision(ctx context.Context, revision string) context.Context {
	if revision == "" {
		return ctx
	}
	return context.WithValue(ctx, revisionContextKey{}, revision)
}

// RevisionFrom returns the catalog revision bound to the current execution.
func RevisionFrom(ctx context.Context) (string, bool) {
	revision, ok := ctx.Value(revisionContextKey{}).(string)
	return revision, ok && revision != ""
}

// WorkspaceProvider loads a catalog for one resolved logical workspace. Empty
// workspace identifiers select the provider's configured default.
type WorkspaceProvider interface {
	LoadWorkspace(ctx context.Context, workspaceID string) (Catalog, error)
}

// WorkspaceProviderFunc adapts a function to WorkspaceProvider.
type WorkspaceProviderFunc func(ctx context.Context, workspaceID string) (Catalog, error)

func (f WorkspaceProviderFunc) LoadWorkspace(ctx context.Context, workspaceID string) (Catalog, error) {
	return f(ctx, workspaceID)
}

// BoundWorkspaceProvider maps only the selected logical default workspace to a
// backend-specific default. Other explicit workspaces pass through unchanged,
// preventing one configured MemoryLayer workspace from becoming every logical
// workspace's catalog.
type BoundWorkspaceProvider struct {
	Provider       WorkspaceProvider
	LogicalDefault string
	BackendDefault string
}

func (p BoundWorkspaceProvider) LoadWorkspace(ctx context.Context, workspaceID string) (Catalog, error) {
	if workspaceID == "" || workspaceID == p.LogicalDefault {
		workspaceID = p.BackendDefault
	}
	return p.Provider.LoadWorkspace(ctx, workspaceID)
}

// BindWorkspaceProvider applies a logical-to-backend default mapping. A nil
// provider remains nil so filesystem-only hosts need no special case.
func BindWorkspaceProvider(provider WorkspaceProvider, logicalDefault, backendDefault string) WorkspaceProvider {
	if provider == nil {
		return nil
	}
	if logicalDefault == backendDefault && logicalDefault != "" {
		return provider
	}
	return BoundWorkspaceProvider{
		Provider:       provider,
		LogicalDefault: logicalDefault,
		BackendDefault: backendDefault,
	}
}

// MergeSkills combines a lower-precedence catalog with a higher-precedence
// catalog deterministically. The first occurrence fixes ordering; a later
// duplicate replaces its value. This lets workspace filesystem skills override
// MemoryLayer skills without making the prompt order depend on map iteration.
func MergeSkills(base, override []SkillSpec) []SkillSpec {
	byName := make(map[string]SkillSpec, len(base)+len(override))
	order := make([]string, 0, len(base)+len(override))
	add := func(skill SkillSpec) {
		if skill.Name == "" || !skill.Enabled {
			return
		}
		if _, exists := byName[skill.Name]; !exists {
			order = append(order, skill.Name)
		}
		byName[skill.Name] = cloneSkill(skill)
	}
	for _, skill := range base {
		add(skill)
	}
	for _, skill := range override {
		add(skill)
	}
	out := make([]SkillSpec, 0, len(order))
	for _, name := range order {
		out = append(out, byName[name])
	}
	return out
}

func cloneSkill(skill SkillSpec) SkillSpec {
	skill.AllowedTools = append([]string(nil), skill.AllowedTools...)
	skill.Prereqs = append([]string(nil), skill.Prereqs...)
	if skill.Source != nil {
		source := *skill.Source
		skill.Source = &source
	}
	return skill
}
