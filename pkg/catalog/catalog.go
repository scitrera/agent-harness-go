// Package catalog is the tool/skill/MCP discovery seam: where the set of
// available tools, skills, and MCP servers comes from. The core ships a fixed
// (Static) and a filesystem-backed provider; the Scitrera distribution provides
// a MemoryLayer-backed provider. The catalog types are protocol-neutral.
package catalog

import (
	"context"
	"encoding/json"
	"time"
)

// Catalog is the discovered set of tools, skills, and MCP servers.
type Catalog struct {
	Tools      []ToolSpec      `json:"tools"`
	Skills     []SkillSpec     `json:"skills"`
	MCPServers []MCPServerSpec `json:"mcp_servers"`
}

type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	Enabled     bool            `json:"enabled"`
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
	Prereqs        []string `json:"prereq_skills,omitempty"`
	PreferredModel string   `json:"preferred_model,omitempty"`
}

type MCPServerSpec struct {
	Name           string        `json:"name"`
	Command        string        `json:"command"`
	Args           []string      `json:"args,omitempty"`
	Env            []string      `json:"env,omitempty"`
	IdleTTLSeconds int           `json:"idle_ttl_seconds,omitempty"`
	Tools          []MCPToolSpec `json:"tools,omitempty"`
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

// Provider supplies the catalog. Implementations: Static (core), FS (core,
// see catalog/fs), and a MemoryLayer-backed provider (distribution).
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
