// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package requirements

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func Test_Compose_layers_local_config_with_source_labels(t *testing.T) {
	repo := Source{Label: "repo requirements", Kind: "file", Path: "repo.toml"}
	workspace := Source{Label: "workspace requirements", Kind: "file", Path: "workspace.toml"}
	got, err := Compose(
		Layer{
			Source:   repo,
			Approval: &ApprovalRequirement{Mode: ApprovalOnRequest},
			ToolPolicy: &ToolPolicyRequirement{
				DefaultDecision: tools.DecisionRequiresApproval,
				Rules: []tools.CommandRule{{
					ID:       "allow-cat",
					Decision: tools.DecisionAllow,
					Reason:   "safe read",
					Pattern:  tools.CommandPattern{Tokens: []tools.CommandToken{tools.LiteralToken("cat")}},
					ReadOnly: true,
				}},
			},
			Hooks: &HookRequirement{
				Config: hooks.Config{DefaultTimeout: 5 * time.Second, EnvAllowlist: []string{"PATH"}},
				Hooks: []hooks.CommandHook{{
					Name:    "session",
					Event:   hooks.EventSessionStart,
					Command: []string{"session-hook"},
				}},
			},
			MCPServers:   []mcp.ServerConfig{{Name: "docs", Command: "mcp-docs", Args: []string{"--stdio"}}},
			Catalogs:     []CatalogSource{{Name: "builtin", Kind: CatalogPlugin, Path: ".codex/plugins"}},
			Models:       []ModelChoice{{Role: "default", Model: "gpt-5"}},
			FileStores:   []FileStorePath{{Name: "taskstate", Path: ".agent/tasks.json"}},
			FeatureGates: []FeatureGate{{Name: "local_hooks", Enabled: true}},
		},
		Layer{
			Source: workspace,
			ToolPolicy: &ToolPolicyRequirement{Rules: []tools.CommandRule{{
				ID:       "deny-rm",
				Decision: tools.DecisionDeny,
				Reason:   "destructive delete",
				Pattern:  tools.CommandPattern{Tokens: []tools.CommandToken{tools.LiteralToken("rm")}},
			}}},
			Hooks: &HookRequirement{Hooks: []hooks.CommandHook{{
				Name:    "audit",
				Event:   hooks.EventPreToolUse,
				Command: []string{"audit-hook"},
			}}},
			MCPServers: []mcp.ServerConfig{{Name: "local", Command: "mcp-local"}},
			Models:     []ModelChoice{{Role: "summary", Model: "gpt-5-mini"}},
		},
	)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}
	if got.Approval == nil || got.Approval.Source.Label != repo.Label {
		t.Fatalf("approval source = %#v, want %q", got.Approval, repo.Label)
	}
	if len(got.ToolPolicy.Rules) != 2 || got.ToolPolicy.Rules[0].Value.ID != "deny-rm" {
		t.Fatalf("tool policy rule order = %#v, want high-priority overlay first", got.ToolPolicy.Rules)
	}
	if got.ToolPolicy.Rules[0].Source.Label != workspace.Label {
		t.Fatalf("overlay rule source = %q, want %q", got.ToolPolicy.Rules[0].Source.Label, workspace.Label)
	}
	if len(got.Hooks.Hooks) != 2 || got.Hooks.Hooks[0].Value.Name != "audit" {
		t.Fatalf("hook order = %#v, want high-priority overlay first", got.Hooks.Hooks)
	}
	if got.MCPServers["docs"].Source.Label != repo.Label || got.MCPServers["local"].Source.Label != workspace.Label {
		t.Fatalf("mcp sources = %#v", got.MCPServers)
	}
	if got.Models["summary"].Value.Model != "gpt-5-mini" {
		t.Fatalf("summary model = %#v", got.Models["summary"])
	}
	if !got.FeatureGates["local_hooks"].Value.Enabled {
		t.Fatalf("local_hooks feature gate = %#v", got.FeatureGates["local_hooks"])
	}
	if _, err := tools.NewCommandPolicy(got.CommandPolicyConfig()); err != nil {
		t.Fatalf("CommandPolicyConfig() invalid: %v", err)
	}
	cfg, commandHooks := got.HookRuntimeConfig()
	if cfg.DefaultTimeout != 5*time.Second || len(commandHooks) != 2 {
		t.Fatalf("HookRuntimeConfig() = %#v, %#v", cfg, commandHooks)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal composed requirements: %v", err)
	}
	if !strings.Contains(string(raw), "workspace requirements") {
		t.Fatalf("json artifact missing source label: %s", string(raw))
	}
}

func Test_Compose_fails_closed_when_hook_conflicts(t *testing.T) {
	_, err := Compose(
		Layer{Source: NewSource("repo"), Hooks: &HookRequirement{Hooks: []hooks.CommandHook{{
			Name: "audit", Event: hooks.EventPreToolUse, Command: []string{"audit-v1"},
		}}}},
		Layer{Source: NewSource("workspace"), Hooks: &HookRequirement{Hooks: []hooks.CommandHook{{
			Name: "audit", Event: hooks.EventPreToolUse, Command: []string{"audit-v2"},
		}}}},
	)
	assertSourceConflict(t, err, "hooks.PreToolUse.audit", "repo", "workspace")
}

func Test_Compose_fails_closed_when_policy_conflicts(t *testing.T) {
	_, err := Compose(
		Layer{Source: NewSource("repo"), ToolPolicy: &ToolPolicyRequirement{Rules: []tools.CommandRule{{
			ID: "git-status", Decision: tools.DecisionAllow, Pattern: tools.CommandPattern{Tokens: []tools.CommandToken{tools.LiteralToken("git")}},
		}}}},
		Layer{Source: NewSource("workspace"), ToolPolicy: &ToolPolicyRequirement{Rules: []tools.CommandRule{{
			ID: "git-status", Decision: tools.DecisionDeny, Pattern: tools.CommandPattern{Tokens: []tools.CommandToken{tools.LiteralToken("git")}},
		}}}},
	)
	assertSourceConflict(t, err, "tool_policy.rules.git-status", "repo", "workspace")
}

func Test_Compose_rejects_malformed_layer(t *testing.T) {
	_, err := Compose(Layer{
		Source: NewSource("bad"),
		ToolPolicy: &ToolPolicyRequirement{Rules: []tools.CommandRule{{
			ID:       "empty",
			Decision: tools.DecisionAllow,
		}}},
	})
	if !errors.Is(err, ErrInvalidLayer) {
		t.Fatalf("Compose() error = %v, want ErrInvalidLayer", err)
	}
}

func Test_Compose_rejects_empty_source(t *testing.T) {
	_, err := Compose(Layer{})
	if !errors.Is(err, ErrInvalidLayer) {
		t.Fatalf("Compose() error = %v, want ErrInvalidLayer", err)
	}
	if err == nil || !strings.Contains(err.Error(), "source label required") {
		t.Fatalf("Compose() error = %v, want source label failure", err)
	}
}

func Test_Compose_rejects_invalid_approval_mode_when_source_is_valid(t *testing.T) {
	_, err := Compose(Layer{
		Source:   NewSource("workspace"),
		Approval: &ApprovalRequirement{Mode: ApprovalMode("sometimes")},
	})
	if !errors.Is(err, ErrInvalidLayer) {
		t.Fatalf("Compose() error = %v, want ErrInvalidLayer", err)
	}
	if err == nil || !strings.Contains(err.Error(), "workspace") || !strings.Contains(err.Error(), "approval mode") {
		t.Fatalf("Compose() error = %v, want source-aware approval failure", err)
	}
}

func Test_Compose_rejects_invalid_named_requirements(t *testing.T) {
	tests := []struct {
		name  string
		layer Layer
		want  string
	}{
		{
			name:  "mcp command",
			layer: Layer{MCPServers: []mcp.ServerConfig{{Name: "docs"}}},
			want:  "mcp server \"docs\" command required",
		},
		{
			name:  "catalog kind",
			layer: Layer{Catalogs: []CatalogSource{{Name: "main", Kind: CatalogKind("theme"), Path: ".codex/plugins"}}},
			want:  "catalog \"main\" has unsupported kind",
		},
		{
			name:  "catalog locator",
			layer: Layer{Catalogs: []CatalogSource{{Name: "main", Kind: CatalogPlugin}}},
			want:  "catalog \"main\" path or uri required",
		},
		{
			name:  "model id",
			layer: Layer{Models: []ModelChoice{{Role: "default"}}},
			want:  "model \"default\" model required",
		},
		{
			name:  "file store path",
			layer: Layer{FileStores: []FileStorePath{{Name: "taskstate"}}},
			want:  "file store \"taskstate\" path required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.layer.Source = NewSource("workspace")
			_, err := Compose(tt.layer)
			if !errors.Is(err, ErrInvalidLayer) {
				t.Fatalf("Compose() error = %v, want ErrInvalidLayer", err)
			}
			if err == nil || !strings.Contains(err.Error(), "workspace") || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Compose() error = %v, want source-aware %q", err, tt.want)
			}
		})
	}
}

func Test_CommandPolicyConfig_adapts_resolve_host_executables(t *testing.T) {
	resolve := true
	got, err := Compose(Layer{
		Source: NewSource("workspace"),
		ToolPolicy: &ToolPolicyRequirement{
			ResolveHostExecutables: &resolve,
			Rules: []tools.CommandRule{{
				ID:       "allow-go",
				Decision: tools.DecisionAllow,
				Pattern:  tools.CommandPattern{Tokens: []tools.CommandToken{tools.LiteralToken("go")}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}
	cfg := got.CommandPolicyConfig()
	if !cfg.ResolveHostExecutables {
		t.Fatalf("ResolveHostExecutables = false, want true")
	}
	if got.ToolPolicy.ResolveHostExecutables == nil || got.ToolPolicy.ResolveHostExecutables.Source.Label != "workspace" {
		t.Fatalf("sourced ResolveHostExecutables = %#v, want workspace source", got.ToolPolicy.ResolveHostExecutables)
	}
}

func assertSourceConflict(t *testing.T, err error, field string, sources ...string) {
	t.Helper()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Compose() error = %v, want ErrConflict", err)
	}
	var conflictErr *ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("Compose() error = %T, want *ConflictError", err)
	}
	if conflictErr.Field != field {
		t.Fatalf("conflict field = %q, want %q", conflictErr.Field, field)
	}
	message := err.Error()
	for _, source := range sources {
		if !strings.Contains(message, source) {
			t.Fatalf("conflict error %q missing source %q", message, source)
		}
	}
}
