package subagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileCatalogListsDefinitionsWhenJSONFileUsesPromptPath(t *testing.T) {
	// Given: a filesystem catalog with one JSON agent and a relative prompt file.
	dir := t.TempDir()
	promptDir := filepath.Join(dir, "prompts")
	if err := os.Mkdir(promptDir, 0o755); err != nil {
		t.Fatalf("mkdir prompt dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(promptDir, "reviewer.md"), []byte("review instructions"), 0o644); err != nil {
		t.Fatalf("write prompt: %v", err)
	}
	agentJSON := `{
		"reviewer": {
			"description": "Review code changes",
			"prompt_path": "prompts/reviewer.md",
			"tools": ["read_file"],
			"disallowedTools": ["shell"],
			"skills": ["go"],
			"mcp_servers": ["docs"],
			"model": "sonnet",
			"max_turns": 3,
			"permission_mode": "ask",
			"background": true
		}
	}`
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agentJSON), 0o644); err != nil {
		t.Fatalf("write agents: %v", err)
	}
	catalog := NewFileCatalog(dir)

	// When: the catalog is listed and a definition is fetched by type.
	defs, err := catalog.List(context.Background())
	if err != nil {
		t.Fatalf("list catalog: %v", err)
	}
	def, err := catalog.Get(context.Background(), AgentType("reviewer"))
	if err != nil {
		t.Fatalf("get reviewer: %v", err)
	}

	// Then: file-backed fields are normalized and tool restrictions are typed.
	if len(defs) != 1 {
		t.Fatalf("expected one definition, got %d", len(defs))
	}
	if def.Name != AgentName("reviewer") || def.Type != AgentType("reviewer") {
		t.Fatalf("definition identity not normalized: %+v", def)
	}
	if def.Prompt != "review instructions" || def.Model != "sonnet" || def.MaxTurns != 3 {
		t.Fatalf("definition content not loaded: %+v", def)
	}
	if def.PermissionMode != PermissionModeAsk || !def.Background {
		t.Fatalf("definition policy flags not loaded: %+v", def)
	}
	if err := def.AllowsTool("read_file"); err != nil {
		t.Fatalf("read_file should be allowed: %v", err)
	}
	if err := def.AllowsTool("shell"); !errors.Is(err, ErrToolDenied) {
		t.Fatalf("shell should be denied, got %v", err)
	}
}

func TestFileCatalogRejectsInvalidPermissionMode(t *testing.T) {
	// Given: a catalog definition with an unsupported permission mode.
	dir := t.TempDir()
	agentJSON := `{"name":"bad","type":"bad","description":"Bad agent","prompt":"x","permission_mode":"root"}`
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte(agentJSON), 0o644); err != nil {
		t.Fatalf("write bad agent: %v", err)
	}
	catalog := NewFileCatalog(dir)

	// When: the catalog is loaded.
	_, err := catalog.List(context.Background())

	// Then: validation fails with a typed permission-mode error.
	if !errors.Is(err, ErrInvalidPermissionMode) {
		t.Fatalf("expected ErrInvalidPermissionMode, got %v", err)
	}
}

func TestFileCatalogGetReturnsUnknownAgent(t *testing.T) {
	// Given: an empty filesystem catalog.
	catalog := NewFileCatalog(t.TempDir())

	// When: a missing agent type is requested.
	_, err := catalog.Get(context.Background(), AgentType("missing"))

	// Then: callers can branch on the typed unknown-agent error.
	if !errors.Is(err, ErrUnknownAgent) {
		t.Fatalf("expected ErrUnknownAgent, got %v", err)
	}
}
