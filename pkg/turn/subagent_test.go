package turn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func Test_Runner_RunSubagent_returns_final_text(t *testing.T) {
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	res, err := r.RunSubagent(context.Background(), subagent.Request{
		Task:   "summarize the design",
		Depth:  1,
		Parent: protocol.MessageAddress{ThreadID: "t1"},
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if res.Text != "assistant response" {
		t.Fatalf("expected sub-agent final text, got %q", res.Text)
	}
}

func Test_Runner_RunSubagent_applies_catalog_prompt_model_and_tool_surface(t *testing.T) {
	// Given: a production runner with two registered tools and a catalog-selected
	// subagent request that only allows read_file.
	registry := tools.NewRegistry()
	if err := registry.Register("read_file", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register read_file: %v", err)
	}
	if err := registry.Register("shell", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register shell: %v", err)
	}
	provider := &fakeProvider{}
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{files: []bootstrap.File{{Name: "AGENTS.md", Content: "base instructions"}}},
		Registry:  registry,
		Provider:  provider,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:     "default-model",
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: the in-process subagent runner executes the request.
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task:         "inspect the patch",
		Parent:       protocol.MessageAddress{ThreadID: "thread-1"},
		AgentType:    subagent.AgentType("reviewer"),
		Instructions: "reviewer-only instructions",
		AllowedTools: []string{"read_file"},
		DeniedTools:  []string{"shell"},
		Model:        "agent-model",
	})

	// Then: the provider sees the selected model, prompt instructions, and only
	// the allowed tool in the real chat request.
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if provider.request.Model != "agent-model" {
		t.Fatalf("model = %q, want agent-model", provider.request.Model)
	}
	if got := toolNames(provider.request.Tools); len(got) != 1 || got[0] != "read_file" {
		t.Fatalf("tools = %#v, want [read_file]", got)
	}
	if !requestTextContains(provider.request, "reviewer-only instructions") {
		t.Fatalf("subagent instructions missing from provider request: %#v", provider.request.Messages)
	}
}

func Test_Runner_RunSubagent_denies_hidden_or_unlisted_tool_calls(t *testing.T) {
	// Given: shell is registered, but the selected subagent definition denies it.
	calledShell := false
	registry := tools.NewRegistry()
	if err := registry.Register("shell", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		calledShell = true
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ran":true}`))
	})); err != nil {
		t.Fatalf("register shell: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("blocked shell")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: the model calls shell anyway, even though it was hidden from Tools.
	res, err := runner.RunSubagent(context.Background(), subagent.Request{
		Task:        "try a shell command",
		Parent:      protocol.MessageAddress{ThreadID: "thread-1"},
		DeniedTools: []string{"shell"},
	})

	// Then: the real tool handler is not invoked; the denial is returned as a
	// tool_result and the provider is reprompted to produce the final answer.
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if calledShell {
		t.Fatal("shell handler should not run for denied subagent tool")
	}
	if res.Text != "blocked shell" {
		t.Fatalf("result = %q, want blocked shell", res.Text)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(provider.requests))
	}
	if !requestTextContains(provider.requests[1], "tool denied") {
		t.Fatalf("denied tool_result missing from reprompt: %#v", provider.requests[1].Messages)
	}
}

func Test_Runner_RunSubagent_applies_max_turns_to_tool_loop(t *testing.T) {
	// Given: the runner normally allows several tool iterations, but the selected
	// subagent caps this run to one tool iteration.
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register record: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool-1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-tool-2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		MaxToolIterations: 4,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: the subagent spends its single allowed tool iteration and asks for
	// another tool call.
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task:     "loop tools",
		Parent:   protocol.MessageAddress{ThreadID: "thread-1"},
		MaxTurns: 1,
	})

	// Then: the production loop stops at the catalog limit, not the runner default.
	if !errors.Is(err, ErrToolLoopLimit) {
		t.Fatalf("expected ErrToolLoopLimit, got %v", err)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(provider.requests))
	}
}

func toolNames(specs []provider.ToolSpec) []string {
	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		out = append(out, spec.Name)
	}
	return out
}

func requestTextContains(req provider.ChatRequest, want string) bool {
	needle := []byte(want)
	for _, msg := range req.Messages {
		for _, part := range msg.Content {
			if text, ok := part.AsText(); ok && bytes.Contains([]byte(text.Text), needle) {
				return true
			}
			if result, ok := part.AsToolResult(); ok && bytes.Contains(result.Output, needle) {
				return true
			}
		}
	}
	return false
}
