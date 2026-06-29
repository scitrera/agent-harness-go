package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

type spySubagent struct {
	called       bool
	task         string
	depth        int
	agentType    subagent.AgentType
	model        string
	maxTurns     int
	allowedTools []string
	deniedTools  []string
	instructions string
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
	return subagent.Result{Text: "sub-agent answer"}, nil
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
