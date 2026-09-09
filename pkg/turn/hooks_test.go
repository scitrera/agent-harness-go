// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// callToolThenText emits a tool_call to toolName on its first response, then
// plain text. It records how many times it was called.
type callToolThenText struct {
	toolName string
	calls    int
}

func (p *callToolThenText) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	p.calls++
	if p.calls == 1 {
		part, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: p.toolName})
		if err != nil {
			return provider.ChatResponse{}, err
		}
		return provider.ChatResponse{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}, nil
	}
	text, _ := protocol.NewTextPart("done")
	return provider.ChatResponse{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{text}}}, nil
}

type recordingObserver struct {
	mu       sync.Mutex
	started  []string
	finished []string
}

func (o *recordingObserver) ToolStarted(_ context.Context, call hooks.ToolCall) {
	o.mu.Lock()
	o.started = append(o.started, call.Name)
	o.mu.Unlock()
}

func (o *recordingObserver) ToolFinished(_ context.Context, call hooks.ToolCall, _ bool, _ error) {
	o.mu.Lock()
	o.finished = append(o.finished, call.Name)
	o.mu.Unlock()
}

func registryWithTool(t *testing.T, name string, invoked *int) *tools.Registry {
	t.Helper()
	reg := tools.NewRegistry()
	err := reg.Register(name, tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		*invoked++
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	}))
	if err != nil {
		t.Fatalf("register tool: %v", err)
	}
	return reg
}

func Test_Runner_Run_observers_see_tool_lifecycle(t *testing.T) {
	invoked := 0
	reg := registryWithTool(t, "calc", &invoked)
	obs := &recordingObserver{}
	prov := &callToolThenText{toolName: "calc"}
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  prov,
		Registry:  reg,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Observers: []hooks.ToolObserver{obs},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "compute")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if invoked != 1 {
		t.Fatalf("expected calc invoked once, got %d", invoked)
	}
	if len(obs.started) != 1 || obs.started[0] != "calc" || len(obs.finished) != 1 || obs.finished[0] != "calc" {
		t.Fatalf("observer did not see calc lifecycle: started=%v finished=%v", obs.started, obs.finished)
	}
}

func Test_Runner_Run_command_allowed_tools_denies_disallowed_call(t *testing.T) {
	invoked := 0
	// The tool the model will try to call is NOT registered as executable here;
	// if the gate failed open, InvokeTool would error on an unknown tool.
	reg := registryWithTool(t, "allowed_tool", &invoked)
	prov := &callToolThenText{toolName: "forbidden_tool"}
	cmds := commands.New([]commands.Command{{
		Name:         "restrict",
		Body:         "do the task: $ARGUMENTS",
		AllowedTools: []string{"allowed_tool"},
	}})
	store := &fakeStore{}
	r, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{},
		Provider:  prov,
		Registry:  reg,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 16}),
		Commands:  cmds,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/restrict go")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if invoked != 0 {
		t.Fatal("disallowed tool must not execute")
	}
	if prov.calls != 2 {
		t.Fatalf("expected denial result to feed back into a second model call, got %d calls", prov.calls)
	}
	// History should contain a tool_result error carrying the denial reason.
	foundDenial := false
	for _, m := range store.messages {
		for _, p := range m.Content {
			if tr, ok := p.AsToolResult(); ok && tr.IsError && strings.Contains(string(tr.Output), "allowed-tools") {
				foundDenial = true
			}
		}
	}
	if !foundDenial {
		t.Fatalf("expected a denial tool_result in history: %#v", store.messages)
	}
}
