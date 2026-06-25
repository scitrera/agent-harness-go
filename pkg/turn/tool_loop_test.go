package turn

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type scriptedProvider struct {
	responses []provider.ChatResponse
	requests  []provider.ChatRequest
}

func (p *scriptedProvider) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	if err := ctx.Err(); err != nil {
		return provider.ChatResponse{}, err
	}
	p.requests = append(p.requests, req)
	if len(p.responses) == 0 {
		return provider.ChatResponse{}, errors.New("no scripted response")
	}
	resp := p.responses[0]
	p.responses = p.responses[1:]
	return resp, nil
}

func Test_Runner_Run_invokes_tool_call_and_reprompts_provider(t *testing.T) {
	// Given
	ctx := context.Background()
	store := &fakeStore{}
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"recorded":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{"value":1}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:             store,
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         publisher,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("please record")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	// When
	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})

	// Then
	if err != nil {
		t.Fatalf("run turn: %v", err)
	}
	if assistant.ID != "assistant-final" {
		t.Fatalf("expected final assistant, got %#v", assistant)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("expected two provider requests, got %d", len(provider.requests))
	}
	if len(store.messages) != 4 {
		t.Fatalf("expected user, assistant tool call, tool result, final assistant; got %#v", store.messages)
	}
	if store.messages[2].Role != protocol.RoleToolResult {
		t.Fatalf("expected tool result message at index 2, got %#v", store.messages[2])
	}
	// Streamed lifecycle: started, tool_call part, tool_result part, final.
	if len(publisher.events) != 4 {
		t.Fatalf("expected started, tool_call, tool_result, final; got %#v", publisher.events)
	}
	if publisher.events[0].Type != "message_started" {
		t.Fatalf("event[0] should be message_started: %#v", publisher.events[0])
	}
	if publisher.events[1].Type != "part_appended" || publisher.events[1].Part == nil || publisher.events[1].Part.Type() != protocol.ContentToolCall {
		t.Fatalf("event[1] should be a tool_call part_appended: %#v", publisher.events[1])
	}
	if publisher.events[2].Type != "part_appended" || publisher.events[2].Part == nil || publisher.events[2].Part.Type() != protocol.ContentToolResult {
		t.Fatalf("event[2] should be a tool_result part_appended: %#v", publisher.events[2])
	}
	if publisher.events[3].Type != "message_final" {
		t.Fatalf("event[3] should be message_final: %#v", publisher.events[3])
	}
}

func Test_Runner_Run_stops_when_tool_loop_exceeds_limit(t *testing.T) {
	// Given
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
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
		Assembler:         contextpack.NewAssembler(contextpack.Config{}),
		MaxToolIterations: 1,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser})

	// Then
	if !errors.Is(err, ErrToolLoopLimit) {
		t.Fatalf("expected ErrToolLoopLimit, got %v", err)
	}
}

// A tool that fails to invoke (here a handler error standing in for the real
// "not pre-authorized" registry denial) must NOT abort the turn: the error is
// handed back to the model as a tool_result, and the model then produces a
// normal reply. Aborting would end the turn with no assistant message, which
// reads to the UI as a hang.
func Test_Runner_Run_failed_tool_call_is_returned_to_model_not_aborted(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("shell", tools.HandlerFunc(func(context.Context, tools.Request) (tools.Result, error) {
		return tools.Result{}, errors.New("approval required: shell: tool is not pre-authorized")
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("I can't run shell here.")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         publisher,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser})
	if err != nil {
		t.Fatalf("turn must complete, not abort on tool error: %v", err)
	}
	if assistant.ID != "assistant-final" {
		t.Fatalf("expected final model reply after tool error, got %#v", assistant)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("expected provider reprompted (2 requests) after the tool error, got %d", len(provider.requests))
	}
	sawToolResult := false
	for _, ev := range publisher.events {
		if ev.Type == "part_appended" && ev.Part != nil && ev.Part.Type() == protocol.ContentToolResult {
			sawToolResult = true
		}
	}
	if !sawToolResult {
		t.Fatal("expected a tool_result error part streamed to the UI")
	}
}
