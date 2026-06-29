package requirements

import (
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func Test_Compose_rejects_unknown_hook_event(t *testing.T) {
	_, err := Compose(Layer{
		Source: NewSource("workspace"),
		Hooks: &HookRequirement{Hooks: []hooks.CommandHook{{
			Name:    "audit",
			Event:   hooks.EventName("PreTooluse"),
			Command: []string{"audit-hook"},
		}}},
	})

	if !errors.Is(err, ErrInvalidLayer) {
		t.Fatalf("Compose() error = %v, want ErrInvalidLayer", err)
	}
	if err == nil || !strings.Contains(err.Error(), "workspace") || !strings.Contains(err.Error(), "PreTooluse") {
		t.Fatalf("Compose() error = %v, want source-aware hook event failure", err)
	}
}

func Test_Compose_preserves_same_layer_hook_order_with_overlay_precedence(t *testing.T) {
	got, err := Compose(
		Layer{
			Source: NewSource("repo"),
			Hooks: &HookRequirement{Hooks: []hooks.CommandHook{
				{Name: "repo-first", Event: hooks.EventPreToolUse, Command: []string{"repo-first"}},
				{Name: "repo-second", Event: hooks.EventPreToolUse, Command: []string{"repo-second"}},
			}},
		},
		Layer{
			Source: NewSource("workspace"),
			Hooks: &HookRequirement{Hooks: []hooks.CommandHook{
				{Name: "workspace-first", Event: hooks.EventPreToolUse, Command: []string{"workspace-first"}},
				{Name: "workspace-second", Event: hooks.EventPreToolUse, Command: []string{"workspace-second"}},
			}},
		},
	)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}

	want := []string{"workspace-first", "workspace-second", "repo-first", "repo-second"}
	if len(got.Hooks.Hooks) != len(want) {
		t.Fatalf("hook count = %d, want %d", len(got.Hooks.Hooks), len(want))
	}
	for i, name := range want {
		if got.Hooks.Hooks[i].Value.Name != name {
			t.Fatalf("hook order[%d] = %q, want %q", i, got.Hooks.Hooks[i].Value.Name, name)
		}
	}
}

func Test_Compose_preserves_same_layer_command_rule_order_with_overlay_precedence(t *testing.T) {
	got, err := Compose(
		Layer{
			Source: NewSource("repo"),
			ToolPolicy: &ToolPolicyRequirement{Rules: []tools.CommandRule{
				commandRule("repo-first"),
				commandRule("repo-second"),
			}},
		},
		Layer{
			Source: NewSource("workspace"),
			ToolPolicy: &ToolPolicyRequirement{Rules: []tools.CommandRule{
				commandRule("workspace-first"),
				commandRule("workspace-second"),
			}},
		},
	)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}

	want := []string{"workspace-first", "workspace-second", "repo-first", "repo-second"}
	cfg := got.CommandPolicyConfig()
	if len(cfg.Rules) != len(want) {
		t.Fatalf("rule count = %d, want %d", len(cfg.Rules), len(want))
	}
	for i, id := range want {
		if cfg.Rules[i].ID != id {
			t.Fatalf("rule order[%d] = %q, want %q", i, cfg.Rules[i].ID, id)
		}
	}
	policy, err := tools.NewCommandPolicy(cfg)
	if err != nil {
		t.Fatalf("NewCommandPolicy() error = %v", err)
	}
	decision := policy.DecideCommand(tools.CommandRequest{Argv: []string{"go"}})
	if len(decision.MatchedRules) != len(want) {
		t.Fatalf("matched rule count = %d, want %d", len(decision.MatchedRules), len(want))
	}
	for i, id := range want {
		if decision.MatchedRules[i].RuleID != id {
			t.Fatalf("matched rule order[%d] = %q, want %q", i, decision.MatchedRules[i].RuleID, id)
		}
	}
}

func commandRule(id string) tools.CommandRule {
	return tools.CommandRule{
		ID:       id,
		Decision: tools.DecisionAllow,
		Pattern:  tools.CommandPattern{Tokens: []tools.CommandToken{tools.LiteralToken("go")}},
	}
}
