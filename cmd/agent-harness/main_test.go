package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/refinement"
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

func TestSelectAppModeDefaultsToTUI(t *testing.T) {
	mode, err := selectAppMode(false, false, false, false, false, false)
	if err != nil {
		t.Fatalf("select default mode: %v", err)
	}
	if mode != appModeTUI {
		t.Fatalf("default mode = %q, want %q", mode, appModeTUI)
	}
}

func TestSelectAppModeSupportsExplicitInterfaces(t *testing.T) {
	tests := []struct {
		name                                  string
		cli, tui, acp, web, serve, standalone bool
		want                                  appMode
	}{
		{name: "cli", cli: true, want: appModeCLI},
		{name: "tui", tui: true, want: appModeTUI},
		{name: "acp", acp: true, want: appModeACP},
		{name: "web", web: true, want: appModeWeb},
		{name: "serve", serve: true, want: appModeServe},
		{name: "standalone", standalone: true, want: appModeStandalone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mode, err := selectAppMode(test.cli, test.tui, test.acp, test.web, test.serve, test.standalone)
			if err != nil {
				t.Fatalf("select mode: %v", err)
			}
			if mode != test.want {
				t.Fatalf("mode = %q, want %q", mode, test.want)
			}
		})
	}
}

func TestSelectAppModeRejectsConflictingInterfaces(t *testing.T) {
	if _, err := selectAppMode(true, true, false, false, false, false); err == nil {
		t.Fatal("expected conflicting interface modes to fail")
	}
}

func TestValidateExternalSubagentConfigRequiresWorkerAndSharedHistory(t *testing.T) {
	target := "ag::routing::agent-harness::executor"
	for _, test := range []struct {
		name     string
		mode     appMode
		target   string
		executor bool
		memory   string
		wantErr  string
	}{
		{name: "disabled local mode", mode: appModeTUI},
		{name: "targeted serve", mode: appModeServe, target: target, memory: "http://memorylayer"},
		{name: "executor standalone", mode: appModeStandalone, executor: true, memory: "http://memorylayer"},
		{name: "wrong mode", mode: appModeTUI, target: target, memory: "http://memorylayer", wantErr: "require --serve"},
		{name: "local history", mode: appModeServe, target: target, wantErr: "require --memorylayer"},
		{name: "short target", mode: appModeServe, target: "executor", memory: "http://memorylayer", wantErr: "full Aether agent topic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateExternalSubagentConfig(test.mode, test.target, test.executor, test.memory)
			if test.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestNormalizePromptNotesAuthorityIsExplicitAndRequiresMemoryLayer(t *testing.T) {
	for _, test := range []struct {
		value, memoryURL, want, wantErr string
	}{
		{value: "", want: promptNotesAuthorityLocal},
		{value: " OFF ", want: promptNotesAuthorityOff},
		{value: "LOCAL", want: promptNotesAuthorityLocal},
		{value: "memorylayer", memoryURL: "http://memorylayer", want: promptNotesAuthorityMemoryLayer},
		{value: "memorylayer", wantErr: "requires --memorylayer"},
		{value: "auto", wantErr: "invalid prompt-note authority"},
	} {
		got, err := normalizePromptNotesAuthority(test.value, test.memoryURL)
		if test.wantErr == "" && (err != nil || got != test.want) {
			t.Fatalf("normalize(%q) = %q, %v; want %q", test.value, got, err, test.want)
		}
		if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
			t.Fatalf("normalize(%q) error = %v, want %q", test.value, err, test.wantErr)
		}
	}
}

func TestNormalizeAgentSpecificationsAuthorityIsExplicitAndRequiresMemoryLayer(t *testing.T) {
	for _, test := range []struct {
		value, memoryURL, want, wantErr string
	}{
		{value: "", want: agentSpecificationsAuthorityLocal},
		{value: " OFF ", want: agentSpecificationsAuthorityOff},
		{value: "LOCAL", want: agentSpecificationsAuthorityLocal},
		{value: "memorylayer", memoryURL: "http://memorylayer", want: agentSpecificationsAuthorityMemoryLayer},
		{value: "memorylayer", wantErr: "requires --memorylayer"},
		{value: "auto", wantErr: "invalid agent-specification authority"},
	} {
		got, err := normalizeAgentSpecificationsAuthority(test.value, test.memoryURL)
		if test.wantErr == "" && (err != nil || got != test.want) {
			t.Fatalf("normalize(%q) = %q, %v; want %q", test.value, got, err, test.want)
		}
		if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
			t.Fatalf("normalize(%q) error = %v, want %q", test.value, err, test.wantErr)
		}
	}
}

func TestNormalizeRefinementAuthorityIsExplicitAndRequiresMemoryLayer(t *testing.T) {
	for _, test := range []struct {
		value, memoryURL, want, wantErr string
	}{
		{value: "", want: refinementAuthorityLocal},
		{value: " OFF ", want: refinementAuthorityOff},
		{value: "LOCAL", want: refinementAuthorityLocal},
		{value: "memorylayer", memoryURL: "http://memorylayer", want: refinementAuthorityMemoryLayer},
		{value: "memorylayer", wantErr: "requires --memorylayer"},
		{value: "dual", wantErr: "invalid refinement authority"},
	} {
		got, err := normalizeRefinementAuthority(test.value, test.memoryURL)
		if test.wantErr == "" && (err != nil || got != test.want) {
			t.Fatalf("normalize(%q) = %q, %v; want %q", test.value, got, err, test.want)
		}
		if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
			t.Fatalf("normalize(%q) error = %v, want %q", test.value, err, test.wantErr)
		}
	}
}

func TestOpenRefinementServiceSelectsAuditAndResourceAuthorities(t *testing.T) {
	local, err := openRefinementService(appConfig{stateDir: t.TempDir()})
	if err != nil || local == nil || local.Store == nil || local.Editors[refinement.ResourcePromptNote] == nil || local.Editors[refinement.ResourceAgentSpecification] != nil {
		t.Fatalf("local refinement service = %+v, %v", local, err)
	}
	off, err := openRefinementService(appConfig{stateDir: t.TempDir(), refinementAuthority: refinementAuthorityOff})
	if err != nil || off != nil {
		t.Fatalf("off refinement service = %+v, %v", off, err)
	}
	remoteResource, err := openRefinementService(appConfig{
		stateDir: t.TempDir(), memorylayerURL: "http://memorylayer", memorylayerWorkspace: "backend",
		promptNotesAuthority: promptNotesAuthorityMemoryLayer,
	})
	if err != nil || remoteResource == nil || remoteResource.Editors[refinement.ResourcePromptNote] == nil || remoteResource.Editors[refinement.ResourceAgentSpecification] != nil {
		t.Fatalf("mixed-authority refinement service = %+v, %v", remoteResource, err)
	}
}

func TestOpenAgentSpecificationCatalogSelectsOneAuthority(t *testing.T) {
	workspace := t.TempDir()
	local, err := openAgentSpecificationCatalog(appConfig{
		workspaceRoot: workspace, agentSpecificationsAuthority: agentSpecificationsAuthorityLocal,
	})
	if err != nil || local != nil {
		t.Fatalf("empty local catalog = %T, %v", local, err)
	}
	off, err := openAgentSpecificationCatalog(appConfig{agentSpecificationsAuthority: agentSpecificationsAuthorityOff})
	if err != nil || off != nil {
		t.Fatalf("off catalog = %T, %v", off, err)
	}
	remote, err := openAgentSpecificationCatalog(appConfig{
		agentSpecificationsAuthority: agentSpecificationsAuthorityMemoryLayer,
		memorylayerURL:               "http://memorylayer",
		workspaceID:                  "project",
		memorylayerWorkspace:         "ml-project",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := remote.(*subagent.ProviderCatalog); !ok {
		t.Fatalf("remote catalog = %T", remote)
	}
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
