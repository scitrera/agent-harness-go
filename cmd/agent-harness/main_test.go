package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingSubagentRunner struct {
	called       bool
	agentType    subagent.AgentType
	model        string
	maxTurns     int
	instructions string
	allowedTools []string
}

func (r *recordingSubagentRunner) RunSubagent(_ context.Context, req subagent.Request) (subagent.Result, error) {
	r.called = true
	r.agentType = req.AgentType
	r.model = req.Model
	r.maxTurns = req.MaxTurns
	r.instructions = req.Instructions
	r.allowedTools = append([]string(nil), req.AllowedTools...)
	return subagent.Result{Text: "catalog-backed response"}, nil
}

func TestAgentCatalogForWorkspaceUsesDefaultDirs(t *testing.T) {
	// Given: a workspace with the default hidden agent catalog directory.
	workspace := t.TempDir()
	catalogDir := filepath.Join(workspace, ".agent-harness-agents")
	if err := os.Mkdir(catalogDir, 0o755); err != nil {
		t.Fatalf("mkdir catalog: %v", err)
	}
	def := `{"name":"reviewer","type":"reviewer","description":"Review code","prompt":"review instructions"}`
	if err := os.WriteFile(filepath.Join(catalogDir, "reviewer.json"), []byte(def), 0o644); err != nil {
		t.Fatalf("write definition: %v", err)
	}

	// When: the reference runner resolves its local catalog.
	catalog, err := agentCatalogForWorkspace(workspace)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("expected catalog")
	}
	loaded, err := catalog.Get(context.Background(), subagent.AgentType("reviewer"))
	if err != nil {
		t.Fatalf("get reviewer: %v", err)
	}

	// Then: the file-backed definition is available to production registration.
	if loaded.Prompt != "review instructions" {
		t.Fatalf("definition prompt not loaded: %+v", loaded)
	}
}

func TestAgentCatalogForWorkspaceMissingDirsKeepsGenericSubagent(t *testing.T) {
	// Given: a workspace without local agent catalog directories.
	workspace := t.TempDir()

	// When: the reference runner resolves its local catalog.
	catalog, err := agentCatalogForWorkspace(workspace)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}

	// Then: no catalog is wired, preserving generic spawn_subagent behavior.
	if catalog != nil {
		t.Fatalf("expected nil catalog, got %T", catalog)
	}
}

func TestRegisterReferenceSubagentUsesCatalog(t *testing.T) {
	// Given: the same file-backed catalog shape the reference runner wires in production.
	workspace := t.TempDir()
	catalogDir := filepath.Join(workspace, "agents")
	if err := os.Mkdir(catalogDir, 0o755); err != nil {
		t.Fatalf("mkdir catalog: %v", err)
	}
	def := `{"name":"reviewer","type":"reviewer","description":"Review code","prompt":"review instructions","tools":["read_file"],"model":"agent-model","max_turns":2}`
	if err := os.WriteFile(filepath.Join(catalogDir, "reviewer.json"), []byte(def), 0o644); err != nil {
		t.Fatalf("write definition: %v", err)
	}
	catalog, err := agentCatalogForWorkspace(workspace)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	reg := tools.NewRegistry()
	runner := &recordingSubagentRunner{}
	if err := registerReferenceSubagent(reg, runner, catalog, false); err != nil {
		t.Fatalf("register subagent: %v", err)
	}

	// When: spawn_subagent selects the filesystem agent through the registered tool.
	res, err := reg.Invoke(context.Background(), tools.Request{
		CallID:    "call-1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"agent":"reviewer","task":"review diff","tools":["read_file"]}`),
	})
	if err != nil {
		t.Fatalf("invoke subagent: %v", err)
	}

	// Then: catalog fields reach the runner through the non-test registration helper.
	if res.IsError {
		t.Fatalf("unexpected error result: %s", res.Payload)
	}
	if !runner.called || runner.agentType != subagent.AgentType("reviewer") {
		t.Fatalf("catalog agent not selected: %+v", runner)
	}
	if runner.model != "agent-model" || runner.maxTurns != 2 || runner.instructions != "review instructions" {
		t.Fatalf("catalog runtime fields not applied: %+v", runner)
	}
	if len(runner.allowedTools) != 1 || runner.allowedTools[0] != "read_file" {
		t.Fatalf("allowed tools not applied: %+v", runner.allowedTools)
	}
	if !bytes.Contains(res.Payload, []byte("catalog-backed response")) {
		t.Fatalf("result missing subagent response: %s", res.Payload)
	}
}

func TestRegisterReferenceSubagentKeepsDepthLimit(t *testing.T) {
	// Given: production registration with the default max depth.
	reg := tools.NewRegistry()
	runner := &recordingSubagentRunner{}
	if err := registerReferenceSubagent(reg, runner, nil, false); err != nil {
		t.Fatalf("register subagent: %v", err)
	}

	// When: a nested call already at the production depth limit invokes spawn_subagent.
	ctx := subagent.WithDepth(context.Background(), referenceSubagentMaxDepth)
	res, err := reg.Invoke(ctx, tools.Request{
		CallID:    "call-1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"task":"nested"}`),
	})
	if err != nil {
		t.Fatalf("invoke subagent: %v", err)
	}

	// Then: it returns a model-visible error without calling the runner.
	if runner.called {
		t.Fatal("runner should not be called at max depth")
	}
	if !res.IsError || !bytes.Contains(res.Payload, []byte("depth limit")) {
		t.Fatalf("expected depth-limit result, got %s", res.Payload)
	}
}

func TestAgentCatalogForWorkspaceRejectsFileCatalogPath(t *testing.T) {
	// Given: a workspace where the default catalog path is a file, not a directory.
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "agents"), []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("write catalog path: %v", err)
	}

	// When: the reference runner resolves the catalog path.
	_, err := agentCatalogForWorkspace(workspace)

	// Then: startup fails closed instead of silently ignoring invalid local config.
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("not a directory")) {
		t.Fatalf("expected not-directory error, got %v", err)
	}
}
