// Package catalog is the tool/skill/MCP discovery seam: where the set of
// available tools, skills, and MCP servers comes from. The core ships a fixed
// (Static) and filesystem-backed provider; pkg/memorylayer supplies a
// workspace-aware remote provider. The catalog types are protocol-neutral.
package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

// Catalog is the discovered set of tools, skills, and MCP servers.
type Catalog struct {
	// Revision is the deterministic content identity of the enabled catalog
	// snapshot. Providers may supply it; hosts recompute it after local overlays.
	Revision   string          `json:"revision,omitempty"`
	Tools      []ToolSpec      `json:"tools"`
	Skills     []SkillSpec     `json:"skills"`
	MCPServers []MCPServerSpec `json:"mcp_servers"`
}

// WithComputedRevision returns a copy carrying a deterministic revision over
// the protocol-neutral catalog content. The existing Revision is excluded.
func WithComputedRevision(value Catalog) (Catalog, error) {
	payload, err := json.Marshal(struct {
		Tools      []ToolSpec      `json:"tools"`
		Skills     []SkillSpec     `json:"skills"`
		MCPServers []MCPServerSpec `json:"mcp_servers"`
	}{Tools: value.Tools, Skills: value.Skills, MCPServers: value.MCPServers})
	if err != nil {
		return Catalog{}, fmt.Errorf("catalog: compute revision: %w", err)
	}
	value.Revision = fmt.Sprintf("sha256:%x", sha256.Sum256(payload))
	return value, nil
}

type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	Enabled     bool            `json:"enabled"`
}

// ResourceRevision identifies the authoritative catalog resource from which an
// executable descriptor was projected. The catalog digest includes these
// fields, so an ETag or artifact change rotates the admitted snapshot even when
// a projection happens to render the same prompt text.
type ResourceRevision struct {
	System         string `json:"system"`
	ID             string `json:"id"`
	Revision       int    `json:"revision,omitempty"`
	ETag           string `json:"etag,omitempty"`
	ManifestDigest string `json:"manifest_digest,omitempty"`
	BundleDigest   string `json:"bundle_digest,omitempty"`
}

type SkillSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Path        string `json:"path,omitempty"`
	Content     string `json:"content,omitempty"`
	Enabled     bool   `json:"enabled"`
	// AllowedTools is the skill's declared tool scope, parsed from an optional
	// `allowed-tools` SKILL.md frontmatter key (YAML list or comma-separated
	// string). Surfaced to the model only — enforcement is a turn-loop follow-up.
	AllowedTools []string `json:"allowed_tools,omitempty"`
	// Prereqs / PreferredModel carry metadata.scitrera.{prereq_skills,preferred_model}
	// when the SOURCE already has it structured (e.g. a MemoryLayer catalog, which
	// strips SKILL.md frontmatter into a metadata field and leaves Content
	// frontmatter-less). BuildRegistry prefers these; when empty it falls back to
	// parsing the body frontmatter (the on-disk SKILL.md path).
	Prereqs        []string          `json:"prereq_skills,omitempty"`
	PreferredModel string            `json:"preferred_model,omitempty"`
	Source         *ResourceRevision `json:"source,omitempty"`
}

type MCPServerSpec struct {
	Name           string            `json:"name"`
	Command        string            `json:"command"`
	Args           []string          `json:"args,omitempty"`
	Env            []string          `json:"env,omitempty"`
	IdleTTLSeconds int               `json:"idle_ttl_seconds,omitempty"`
	Tools          []MCPToolSpec     `json:"tools,omitempty"`
	Source         *ResourceRevision `json:"source,omitempty"`
}

type MCPToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	Enabled     bool            `json:"enabled"`
}

func (s MCPServerSpec) IdleTTL() time.Duration {
	if s.IdleTTLSeconds <= 0 {
		return 0
	}
	return time.Duration(s.IdleTTLSeconds) * time.Second
}

// Provider supplies the catalog. Implementations include Static, filesystem
// discovery, and pkg/memorylayer.CatalogProvider.
type Provider interface {
	Load(ctx context.Context) (Catalog, error)
}

// ProviderFunc adapts a function to Provider (e.g. a store's LoadCatalog).
type ProviderFunc func(ctx context.Context) (Catalog, error)

func (f ProviderFunc) Load(ctx context.Context) (Catalog, error) { return f(ctx) }

// Static returns a fixed catalog. The simple core default.
func Static(c Catalog) Provider {
	return ProviderFunc(func(context.Context) (Catalog, error) { return c, nil })
}
