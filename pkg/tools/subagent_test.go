// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type spySubagent struct {
	called          bool
	task            string
	depth           int
	agentType       subagent.AgentType
	model           string
	maxTurns        int
	allowedTools    []string
	deniedTools     []string
	instructions    string
	resumeThreadID  string
	parentMessageID string
	executionScope  *workspacepkg.ExecutionScope
	outputSchema    json.RawMessage
	// resultThreadID/resultSummary let a test control the returned handle+digest.
	resultThreadID string
	resultSummary  string
}

type workspaceDefinitionProvider struct {
	workspaces []string
}

func (p *workspaceDefinitionProvider) LoadWorkspace(_ context.Context, workspaceID string) ([]subagent.Definition, error) {
	p.workspaces = append(p.workspaces, workspaceID)
	return []subagent.Definition{{
		Name: subagent.AgentName(workspaceID), Type: "reviewer", Description: "Review", Prompt: "instructions for " + workspaceID,
	}}, nil
}

func (s *spySubagent) RunSubagent(_ context.Context, req subagent.Request) (subagent.Result, error) {
	s.called = true
	s.task = req.Task
	s.depth = req.Depth
	s.agentType = req.AgentType
	s.model = req.Model
	s.maxTurns = req.MaxTurns
	s.allowedTools = append([]string(nil), req.AllowedTools...)
	s.deniedTools = append([]string(nil), req.DeniedTools...)
	s.instructions = req.Instructions
	s.resumeThreadID = req.ResumeThreadID
	s.parentMessageID = req.ParentMessageID
	s.executionScope = req.ExecutionScope
	s.outputSchema = append(json.RawMessage(nil), req.OutputSchema...)
	result := subagent.Result{Text: "sub-agent answer", ThreadID: s.resultThreadID, Summary: s.resultSummary}
	if len(req.OutputSchema) > 0 {
		result.Text = `{"status":"ok"}`
		result.StructuredPayload = json.RawMessage(result.Text)
		result.StructuredDigest = subagent.StructuredDigest(result.StructuredPayload)
	}
	return result, nil
}

func TestRegisterSubagentInheritsExactExecutionScope(t *testing.T) {
	registry := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagent(registry, spy, 2); err != nil {
		t.Fatal(err)
	}
	binding := spec.NewExecutionBinding("project-a", "view-a", "window-a", spec.ExecutionSiteClient)
	scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := workspacepkg.WithExecutionScope(context.Background(), scope)
	_, err = registry.Invoke(ctx, Request{
		CallID: "call-1", Name: SubagentToolName, Addr: protocol.MessageAddress{WorkspaceID: "project-a"},
		Arguments: json.RawMessage(`{"task":"inspect the view"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !workspacepkg.ExecutionScopesEqual(&scope, spy.executionScope) {
		t.Fatalf("inherited scope = %+v", spy.executionScope)
	}
}

func TestRegisterSubagentInvokes(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Descriptor is surfaced to the model.
	found := false
	for _, d := range reg.Descriptors() {
		if d.Name == "spawn_subagent" {
			found = true
		}
	}
	if !found {
		t.Fatal("spawn_subagent descriptor not registered")
	}

	res, err := reg.Invoke(context.Background(), Request{CallID: "c1", Name: "spawn_subagent", Arguments: json.RawMessage(`{"task":"research X"}`)})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !spy.called || spy.task != "research X" || spy.depth != 1 {
		t.Fatalf("runner not invoked correctly: %+v", spy)
	}
	if !bytes.Contains(res.Payload, []byte("sub-agent answer")) {
		t.Fatalf("result missing sub-agent answer: %s", res.Payload)
	}
}

func TestRegisterSubagentForwardsOutputSchemaAndReturnsStructuredResult(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatal(err)
	}
	result, err := reg.Invoke(context.Background(), Request{
		CallID: "structured", Name: SubagentToolName,
		Arguments: json.RawMessage(`{"task":"review","output_schema":{"type":"object","required":["status"],"properties":{"status":{"type":"string"}}}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(spy.outputSchema) == 0 || !bytes.Contains(result.Payload, []byte(`"structured_result":{"status":"ok"}`)) || !bytes.Contains(result.Payload, []byte(`"structured_result_digest":"sha256:`)) {
		t.Fatalf("schema=%s payload=%s", spy.outputSchema, result.Payload)
	}
}

func TestRegisterSubagentThreadResumeAndHandlePayload(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{resultThreadID: "parent::sub::9", resultSummary: "did the thing"}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}

	// The optional "thread" arg maps to ResumeThreadID so a follow-up routes to
	// an existing sub-agent thread.
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"task":"continue","thread":"parent::sub::9"}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if spy.resumeThreadID != "parent::sub::9" {
		t.Fatalf("ResumeThreadID = %q, want parent::sub::9", spy.resumeThreadID)
	}
	// The success payload surfaces the re-addressable handle + summary.
	if !bytes.Contains(res.Payload, []byte(`"thread_id":"parent::sub::9"`)) {
		t.Fatalf("payload missing thread_id handle: %s", res.Payload)
	}
	if !bytes.Contains(res.Payload, []byte(`"summary":"did the thing"`)) {
		t.Fatalf("payload missing summary: %s", res.Payload)
	}
	if !bytes.Contains(res.Payload, []byte("sub-agent answer")) {
		t.Fatalf("payload missing result text: %s", res.Payload)
	}
}

func TestRegisterSubagentEmitsSubagentPart(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{resultThreadID: "parent::sub::4", resultSummary: "found it"}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		MessageID: "assistant-1",
		Arguments: json.RawMessage(`{"task":"research X"}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// The parent MESSAGE id flows into the subagent request's ParentMessageID.
	if spy.parentMessageID != "assistant-1" {
		t.Fatalf("ParentMessageID = %q, want assistant-1", spy.parentMessageID)
	}
	// The success result carries a subagent reference part (thread linkage +
	// summary + completed status) for the turn loop to co-locate on history.
	if len(res.Parts) != 1 {
		t.Fatalf("expected 1 extra part, got %d", len(res.Parts))
	}
	sp, ok := res.Parts[0].AsSubagent()
	if !ok {
		t.Fatalf("extra part is not a subagent part: %s", res.Parts[0].Type())
	}
	if sp.ThreadID != "parent::sub::4" {
		t.Fatalf("subagent part thread = %q, want parent::sub::4", sp.ThreadID)
	}
	if sp.Status != protocol.SubagentCompleted {
		t.Fatalf("subagent part status = %q, want completed", sp.Status)
	}
	if sp.Summary != "found it" {
		t.Fatalf("subagent part summary = %q, want found it", sp.Summary)
	}
	if sp.ID != "c1" {
		t.Fatalf("subagent part id = %q, want c1", sp.ID)
	}
}

// recordingSink is a WorldStateSink that captures RecordSubagent calls.
type recordingSink struct {
	subID, subName, subStatus, subSummary string
	recorded                              bool
}

func (r *recordingSink) RecordInvokedSkill(string, string) {}
func (r *recordingSink) RecordFile(string, string)         {}
func (r *recordingSink) RecordTodos([]spec.TodoItem)       {}
func (r *recordingSink) RecordSubagent(id, name, status, summary string) {
	r.recorded = true
	r.subID, r.subName, r.subStatus, r.subSummary = id, name, status, summary
}

func TestRegisterSubagentRecordsWorldStateHandle(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{resultThreadID: "parent::sub::7", resultSummary: "did the analysis"}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}
	sink := &recordingSink{}
	ctx := WithWorldStateSink(context.Background(), sink)
	_, err := reg.Invoke(ctx, Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"task":"analyze X"}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !sink.recorded {
		t.Fatal("expected the sub-agent handle to be recorded on the world-state sink")
	}
	if sink.subID != "parent::sub::7" {
		t.Fatalf("recorded id = %q, want res.ThreadID parent::sub::7", sink.subID)
	}
	if sink.subName != "subagent" {
		t.Fatalf("recorded name = %q, want subagent (no catalog selection)", sink.subName)
	}
	if sink.subStatus != "completed" {
		t.Fatalf("recorded status = %q, want completed", sink.subStatus)
	}
	if sink.subSummary != "did the analysis" {
		t.Fatalf("recorded summary = %q, want did the analysis", sink.subSummary)
	}
}

func TestRegisterSubagentDepthLimit(t *testing.T) {
	reg := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagent(reg, spy, 2); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Already at the depth limit -> denied without invoking the runner.
	ctx := subagent.WithDepth(context.Background(), 2)
	res, err := reg.Invoke(ctx, Request{CallID: "c1", Name: "spawn_subagent", Arguments: json.RawMessage(`{"task":"nested"}`)})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if spy.called {
		t.Fatal("runner should not be invoked at the depth limit")
	}
	if !res.IsError || !bytes.Contains(res.Payload, []byte("depth limit")) {
		t.Fatalf("expected depth-limit error result, got %s (isError=%v)", res.Payload, res.IsError)
	}
}

func TestRegisterSubagentWithConfigSelectsFilesystemAgent(t *testing.T) {
	// Given: a file-backed catalog definition with model, prompt, and tool limits.
	dir := t.TempDir()
	agentJSON := `{"name":"reviewer","type":"reviewer","description":"Review code","prompt":"review instructions","tools":["read_file"],"disallowedTools":["shell"],"model":"sonnet","max_turns":3,"permission_mode":"ask"}`
	if err := os.WriteFile(filepath.Join(dir, "reviewer.json"), []byte(agentJSON), 0o644); err != nil {
		t.Fatalf("write agent: %v", err)
	}
	reg := NewRegistry()
	spy := &spySubagent{}
	err := RegisterSubagentWithConfig(reg, SubagentConfig{
		Runner:   spy,
		MaxDepth: 2,
		Catalog:  subagent.NewFileCatalog(dir),
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// When: spawn_subagent selects that agent and requests an allowed tool.
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"agent":"reviewer","task":"check the diff","tools":["read_file"]}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Then: the runner receives the normalized definition fields.
	if res.IsError {
		t.Fatalf("expected success, got %s", res.Payload)
	}
	if !spy.called || spy.agentType != subagent.AgentType("reviewer") || spy.model != "sonnet" {
		t.Fatalf("agent definition not selected: %+v", spy)
	}
	if spy.maxTurns != 3 || spy.instructions != "review instructions" {
		t.Fatalf("agent limits not passed through: %+v", spy)
	}
	if len(spy.allowedTools) != 1 || spy.allowedTools[0] != "read_file" {
		t.Fatalf("allowed tools not passed through: %+v", spy.allowedTools)
	}
}

func TestRegisterSubagentSelectsAgentFromRequestWorkspace(t *testing.T) {
	provider := &workspaceDefinitionProvider{}
	catalog, err := subagent.NewProviderCatalog(provider, "default", "backend-default")
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	spy := &spySubagent{}
	if err := RegisterSubagentWithConfig(reg, SubagentConfig{Runner: spy, MaxDepth: 2, Catalog: catalog}); err != nil {
		t.Fatal(err)
	}
	result, err := reg.Invoke(context.Background(), Request{
		CallID: "c1", Name: "spawn_subagent", Addr: protocol.MessageAddress{WorkspaceID: "project-b"},
		Arguments: json.RawMessage(`{"agent":"reviewer","task":"review"}`),
	})
	if err != nil || result.IsError {
		t.Fatalf("invoke result=%s err=%v", result.Payload, err)
	}
	if len(provider.workspaces) != 1 || provider.workspaces[0] != "project-b" {
		t.Fatalf("workspaces = %#v", provider.workspaces)
	}
	if spy.instructions != "instructions for project-b" {
		t.Fatalf("instructions = %q", spy.instructions)
	}
}

func TestRegisterSubagentWithConfigRejectsDeniedTool(t *testing.T) {
	// Given: a file-backed catalog definition that denies shell access.
	dir := t.TempDir()
	agentJSON := `{"name":"reviewer","type":"reviewer","description":"Review code","prompt":"review instructions","disallowedTools":["shell"]}`
	if err := os.WriteFile(filepath.Join(dir, "reviewer.json"), []byte(agentJSON), 0o644); err != nil {
		t.Fatalf("write agent: %v", err)
	}
	reg := NewRegistry()
	spy := &spySubagent{}
	err := RegisterSubagentWithConfig(reg, SubagentConfig{
		Runner:   spy,
		MaxDepth: 2,
		Catalog:  subagent.NewFileCatalog(dir),
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// When: spawn_subagent asks that agent for a denied tool.
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"agent":"reviewer","task":"check the diff","tools":["shell"]}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Then: the call returns a typed tool error result without invoking the runner.
	if spy.called {
		t.Fatal("runner should not be invoked for denied tools")
	}
	if !res.IsError || !bytes.Contains(res.Payload, []byte("tool denied")) {
		t.Fatalf("expected denied-tool result, got %s (isError=%v)", res.Payload, res.IsError)
	}
}
