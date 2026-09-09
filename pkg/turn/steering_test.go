// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/steering"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// parkingProvider parks a steering message during the FIRST provider call — the
// moment a real user would type while a tool is running — so the test exercises
// the actual mid-turn path rather than pre-seeding the inbox.
type parkingProvider struct {
	inner    *scriptedProvider
	inbox    *steering.Inbox
	key      string
	park     protocol.ChatMessage
	once     sync.Once
	accepted bool
}

func (p *parkingProvider) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	p.once.Do(func() { p.accepted = p.inbox.Park(p.key, p.park) })
	return p.inner.Chat(ctx, req)
}

// providerFunc adapts a func to provider.Chat so a test can act at the exact
// moment a call is in flight.
type providerFunc func(context.Context, provider.ChatRequest) (provider.ChatResponse, error)

func (f providerFunc) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	return f(ctx, req)
}

func steeringMessage(t *testing.T, id, text string) protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return steering.Mark(protocol.ChatMessage{
		ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{part},
	})
}

// requestTexts flattens every user-role text a provider call carried, so a test
// can assert what the model actually saw.
func requestTexts(req provider.ChatRequest) string {
	var b strings.Builder
	for _, msg := range req.Messages {
		if msg.Role != protocol.RoleUser {
			continue
		}
		for _, part := range msg.Content {
			if tp, ok := part.AsText(); ok {
				b.WriteString(tp.Text)
				b.WriteString("\n")
			}
		}
	}
	return b.String()
}

func Test_Steering_reaches_the_model_on_the_next_provider_call(t *testing.T) {
	// Given: a turn that makes one tool call, and a user who interjects while the
	// first provider call is in flight.
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "c1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`)),
	})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	scripted := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	addr := protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "t1"}
	inbox := steering.New()
	prov := &parkingProvider{
		inner: scripted, inbox: inbox,
		key:  steering.Key(addr.WorkspaceID, addr.ThreadID),
		park: steeringMessage(t, "steer-1", "actually, use yarn"),
	}
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  prov,
		Registry:  registry,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Steering:  inbox,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	// Open the lane the way the runtime loop does.
	closeSteering := inbox.Begin(steering.Key(addr.WorkspaceID, addr.ThreadID))

	// When
	if _, err := runner.Run(ctx, addr, userMessage(t, "build it")); err != nil {
		t.Fatalf("run: %v", err)
	}
	leftover := closeSteering()

	// Then: it was accepted mid-turn...
	if !prov.accepted {
		t.Fatal("Park was rejected while the turn was running")
	}
	// ...delivered without waiting for a whole new turn...
	if len(leftover) != 0 {
		t.Fatalf("steering was never delivered: %#v", leftover)
	}
	// ...and the SECOND provider call carried it, wrapped so the model reads it
	// as an interjection rather than a new task.
	if len(scripted.requests) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(scripted.requests))
	}
	second := requestTexts(scripted.requests[1])
	if !strings.Contains(second, "actually, use yarn") {
		t.Fatalf("second call did not carry the steering message:\n%s", second)
	}
	if !strings.Contains(second, steering.OpenTag) {
		t.Fatalf("steering message was not marked as steering:\n%s", second)
	}
	// The first call predates the interjection and must not contain it.
	if strings.Contains(requestTexts(scripted.requests[0]), "actually, use yarn") {
		t.Fatal("steering leaked into the call that was already in flight")
	}
}

// A user who fires off three quick corrections expects the model to see all
// three together, not to answer the first and rediscover the other two a round
// later. Park appends to one queue and Drain takes the whole queue, so a burst
// is delivered as a batch at the next boundary — in send order.
func Test_Steering_delivers_a_burst_as_one_batch_in_order(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, _ := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "c1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`)),
	})
	finalPart, _ := protocol.NewTextPart("done")
	scripted := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	addr := protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "t1"}
	inbox := steering.New()
	key := steering.Key(addr.WorkspaceID, addr.ThreadID)

	// Given: three sends landing while the first provider call is in flight.
	var once sync.Once
	prov := providerFunc(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		once.Do(func() {
			for _, text := range []string{"skip the tests", "and use yarn", "and bump the version"} {
				if !inbox.Park(key, steeringMessage(t, text, text)) {
					t.Errorf("park rejected %q", text)
				}
			}
		})
		return scripted.Chat(ctx, req)
	})
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: prov, Registry: registry,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Steering:  inbox,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	closeSteering := inbox.Begin(key)

	// When
	if _, err := runner.Run(ctx, addr, userMessage(t, "build it")); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then: all three rode the SAME next call — not one per round.
	if leftover := closeSteering(); len(leftover) != 0 {
		t.Fatalf("undelivered steering: %#v", leftover)
	}
	if len(scripted.requests) != 2 {
		t.Fatalf("provider calls = %d, want 2 — a burst must not cost a round each", len(scripted.requests))
	}
	second := requestTexts(scripted.requests[1])
	first := strings.Index(second, "skip the tests")
	middle := strings.Index(second, "and use yarn")
	last := strings.Index(second, "and bump the version")
	if first < 0 || middle < 0 || last < 0 {
		t.Fatalf("not every steering message was delivered:\n%s", second)
	}
	if first >= middle || middle >= last {
		t.Fatalf("steering burst arrived out of send order:\n%s", second)
	}
}

func Test_Steering_does_not_cancel_work_already_in_flight(t *testing.T) {
	// Given: a tool call issued before the user interjects. Steering must not
	// abandon it — cancel is the separate, explicit escape hatch.
	ctx := context.Background()
	var toolRuns int
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		toolRuns++
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, _ := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "c1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`)),
	})
	finalPart, _ := protocol.NewTextPart("done")
	scripted := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	addr := protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "t1"}
	inbox := steering.New()
	prov := &parkingProvider{
		inner: scripted, inbox: inbox,
		key:  steering.Key(addr.WorkspaceID, addr.ThreadID),
		park: steeringMessage(t, "steer-1", "stop using npm"),
	}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: prov, Registry: registry,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Steering:  inbox,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	defer inbox.Begin(steering.Key(addr.WorkspaceID, addr.ThreadID))()

	// When
	assistant, err := runner.Run(ctx, addr, userMessage(t, "build it"))

	// Then: the tool still ran and the turn still produced its answer.
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if toolRuns != 1 {
		t.Fatalf("tool runs = %d, want 1 — steering must not abandon issued work", toolRuns)
	}
	if assistant.ID != "a2" {
		t.Fatalf("assistant = %#v, want the turn to complete normally", assistant)
	}
}

func Test_Steering_is_inert_when_no_inbox_is_wired(t *testing.T) {
	// Given: a host that never wired steering — the turn must behave exactly as
	// it did before.
	ctx := context.Background()
	finalPart, _ := protocol.NewTextPart("done")
	scripted := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: scripted,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hi"))

	// Then
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if assistant.ID != "a1" {
		t.Fatalf("assistant = %#v", assistant)
	}
}
