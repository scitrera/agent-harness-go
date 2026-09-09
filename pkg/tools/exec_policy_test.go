// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

func Test_CommandPolicy_allows_read_only_prefix_and_records_audit_metadata(t *testing.T) {
	policy, err := NewCommandPolicy(CommandPolicyConfig{
		Rules: []CommandRule{
			{
				ID:           "go-test-package",
				Decision:     DecisionAllow,
				Reason:       "targeted package tests are read-only",
				AuditCode:    "exec.allow.go_test",
				Pattern:      CommandPattern{Tokens: []CommandToken{LiteralToken("go"), LiteralToken("test"), AnyToken("./pkg/tools", "./pkg/localtools")}},
				ReadOnly:     true,
				Match:        [][]string{{"go", "test", "./pkg/tools"}},
				NotMatch:     [][]string{{"go", "test", "./..."}},
				EnvAllowlist: []string{"GOFLAGS"},
			},
		},
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	decision := policy.DecideCommand(CommandRequest{
		Argv:          []string{"go", "test", "./pkg/localtools", "-run", "Test_Workspace"},
		WorkspaceRoot: "/workspace",
		CWD:           "oss",
		Env:           []string{"GOFLAGS=-count=1", "SECRET=redacted"},
	})

	if decision.Code != DecisionAllow {
		t.Fatalf("expected allow, got %#v", decision)
	}
	if decision.MatchedRuleID != "go-test-package" {
		t.Fatalf("expected matched rule id, got %#v", decision)
	}
	if got := decision.MatchedPrefix; len(got) != 3 || got[0] != "go" || got[1] != "test" || got[2] != "./pkg/localtools" {
		t.Fatalf("unexpected matched prefix: %#v", got)
	}
	if !decision.ReadOnly {
		t.Fatalf("expected read-only decision")
	}
	if decision.WorkspaceRoot != "/workspace" || decision.WorkingDirectory != "oss" {
		t.Fatalf("workspace metadata missing: %#v", decision)
	}
	if len(decision.EnvKeys) != 1 || decision.EnvKeys[0] != "GOFLAGS" {
		t.Fatalf("unexpected env audit keys: %#v", decision.EnvKeys)
	}
	if decision.Reason != "targeted package tests are read-only" || decision.AuditCode != "exec.allow.go_test" {
		t.Fatalf("unexpected audit reason/code: %#v", decision.Decision)
	}
}

func Test_CommandPolicy_denies_take_precedence_over_prompt_and_allow(t *testing.T) {
	policy, err := NewCommandPolicy(CommandPolicyConfig{
		Rules: []CommandRule{
			{
				ID:       "go-any",
				Decision: DecisionAllow,
				Reason:   "go commands allowed",
				Pattern:  CommandPattern{Tokens: []CommandToken{LiteralToken("go")}},
			},
			{
				ID:       "go-test-prompts",
				Decision: DecisionRequiresApproval,
				Reason:   "full test sweep needs approval",
				Pattern:  CommandPattern{Tokens: []CommandToken{LiteralToken("go"), LiteralToken("test")}},
			},
			{
				ID:       "go-test-all-denied",
				Decision: DecisionDeny,
				Reason:   "full workspace sweep is blocked",
				Pattern:  CommandPattern{Tokens: []CommandToken{LiteralToken("go"), LiteralToken("test"), LiteralToken("./...")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	decision := policy.DecideCommand(CommandRequest{Argv: []string{"go", "test", "./..."}})

	if decision.Code != DecisionDeny {
		t.Fatalf("expected deny precedence, got %#v", decision)
	}
	if decision.MatchedRuleID != "go-test-all-denied" || decision.Reason != "full workspace sweep is blocked" {
		t.Fatalf("expected deny rule metadata, got %#v", decision)
	}
	if len(decision.MatchedRules) != 3 {
		t.Fatalf("expected all three matching rules in audit metadata, got %#v", decision.MatchedRules)
	}
}

func Test_CommandPolicy_rejects_not_match_fixture_that_hits_rule(t *testing.T) {
	_, err := NewCommandPolicy(CommandPolicyConfig{
		Rules: []CommandRule{
			{
				ID:       "overbroad-go",
				Decision: DecisionAllow,
				Reason:   "overbroad",
				Pattern:  CommandPattern{Tokens: []CommandToken{LiteralToken("go")}},
				NotMatch: [][]string{{"go", "env"}},
			},
		},
	})

	if !errors.Is(err, ErrCommandPolicyFixture) {
		t.Fatalf("expected fixture error, got %v", err)
	}
}

func Test_CommandPolicy_host_executable_metadata_limits_absolute_basename_fallback(t *testing.T) {
	policy, err := NewCommandPolicy(CommandPolicyConfig{
		ResolveHostExecutables: true,
		HostExecutables: []HostExecutable{
			{Name: "git", Paths: []string{"/usr/bin/git"}},
		},
		Rules: []CommandRule{
			{
				ID:             "git-status",
				Decision:       DecisionAllow,
				Reason:         "git status is read-only",
				Pattern:        CommandPattern{Tokens: []CommandToken{LiteralToken("git"), LiteralToken("status")}},
				HostExecutable: "git",
				ReadOnly:       true,
			},
		},
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	allowed := policy.DecideCommand(CommandRequest{Argv: []string{"/usr/bin/git", "status"}})
	if allowed.Code != DecisionAllow || allowed.ResolvedProgram != "/usr/bin/git" || allowed.HostExecutable != "git" {
		t.Fatalf("expected host executable fallback allow, got %#v", allowed)
	}

	blocked := policy.DecideCommand(CommandRequest{Argv: []string{"/tmp/git", "status"}})
	if blocked.Code != DecisionRequiresApproval {
		t.Fatalf("expected unlisted host path to require approval, got %#v", blocked)
	}
}

func Test_LocalShell_enforces_command_policy_before_run_command(t *testing.T) {
	// Given
	ctx := context.Background()
	root := t.TempDir()
	ws, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	policy, err := NewCommandPolicy(CommandPolicyConfig{
		Rules: []CommandRule{
			{
				ID:       "deny-sh",
				Decision: DecisionDeny,
				Reason:   "shell writes are blocked",
				Pattern:  CommandPattern{Tokens: []CommandToken{LiteralToken("sh")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, CommandPolicy: policy}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}

	// When
	_, err = reg.Invoke(ctx, Request{
		CallID:    "shell-deny",
		Name:      "shell",
		Arguments: json.RawMessage(`{"command":"sh","args":["-c","printf denied > marker.txt"]}`),
	})

	// Then
	if !errors.Is(err, ErrToolRequiresApproval) {
		t.Fatalf("expected command policy error, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "marker.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("command executed despite denial, statErr=%v", statErr)
	}
}

func Test_LocalShell_surfaces_command_policy_requires_approval(t *testing.T) {
	// Given
	ctx := context.Background()
	root := t.TempDir()
	ws, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	policy, err := NewCommandPolicy(CommandPolicyConfig{})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, CommandPolicy: policy}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}

	// When
	_, err = reg.Invoke(ctx, Request{
		CallID:    "shell-approval",
		Name:      "shell",
		Arguments: json.RawMessage(`{"command":"sh","args":["-c","printf approval > marker.txt"]}`),
	})

	// Then
	if !errors.Is(err, ErrToolRequiresApproval) || !strings.Contains(err.Error(), "command requires approval") {
		t.Fatalf("expected approval-required command policy error, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "marker.txt")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("command executed despite approval requirement, statErr=%v", statErr)
	}
}

func Test_LocalShell_runs_when_command_policy_allows_prefix(t *testing.T) {
	// Given
	ctx := context.Background()
	root := t.TempDir()
	ws, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	policy, err := NewCommandPolicy(CommandPolicyConfig{
		Rules: []CommandRule{
			{
				ID:       "allow-sh-printf",
				Decision: DecisionAllow,
				Reason:   "printf smoke command is read-only",
				Pattern:  CommandPattern{Tokens: []CommandToken{LiteralToken("sh"), LiteralToken("-c"), LiteralToken("printf ok")}},
				ReadOnly: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, CommandPolicy: policy}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}

	// When
	result, err := reg.Invoke(ctx, Request{
		CallID:    "shell-allow",
		Name:      "shell",
		Arguments: json.RawMessage(`{"command":"sh","args":["-c","printf ok"]}`),
	})

	// Then
	if err != nil {
		t.Fatalf("invoke shell: %v", err)
	}
	if !strings.Contains(string(result.Payload), "ok") {
		t.Fatalf("expected shell output in payload, got %s", string(result.Payload))
	}
}
