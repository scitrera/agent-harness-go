package turn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

func Test_Runner_Run_publishes_tool_lifecycle_events_in_order(t *testing.T) {
	// Given
	invoked := 0
	reg := registryWithTool(t, "calc", &invoked)
	pub := &fakePublisher{}
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &callToolThenText{toolName: "calc"},
		Publisher: pub,
		Registry:  reg,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "w1"}, userMessage(t, "compute")); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then
	events := decodeToolEvents(t, pub.events)
	if len(events) != 3 {
		t.Fatalf("expected queued+started+finished events, got %#v", events)
	}
	statuses := []tools.ToolEventStatus{events[0].Status, events[1].Status, events[2].Status}
	want := []tools.ToolEventStatus{tools.ToolEventQueued, tools.ToolEventStarted, tools.ToolEventFinished}
	for i := range want {
		if statuses[i] != want[i] {
			t.Fatalf("status[%d] = %q, want %q (events=%#v)", i, statuses[i], want[i], events)
		}
		if events[i].CallID != "call-1" || events[i].ToolName != "calc" || events[i].Addr.WorkspaceID != "w1" {
			t.Fatalf("event identity/address missing: %#v", events[i])
		}
		if events[i].ArgumentsHash == "" {
			t.Fatalf("expected argument hash metadata: %#v", events[i])
		}
	}
	if events[2].DurationMS <= 0 || events[2].Result.PayloadBytes == 0 {
		t.Fatalf("finished event missing result metadata: %#v", events[2])
	}
}

func Test_Runner_Run_publishes_one_aborted_event_when_tool_context_is_cancelled(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	reg := tools.NewRegistry()
	if err := reg.Register("cancel_me", tools.HandlerFunc(func(ctx context.Context, req tools.Request) (tools.Result, error) {
		cancel()
		<-ctx.Done()
		return tools.Result{}, ctx.Err()
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	pub := &fakePublisher{}
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &callToolThenText{toolName: "cancel_me"},
		Publisher: pub,
		Registry:  reg,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = r.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "cancel"))

	// Then: a user/ctx cancellation surfaces as the turn-cancelled sentinel
	// (the turn finalizes its partial result rather than failing).
	if !errors.Is(err, turncancel.ErrTurnCancelled) {
		t.Fatalf("expected turn-cancelled sentinel, got %v", err)
	}
	events := decodeToolEvents(t, pub.events)
	if len(events) != 3 {
		t.Fatalf("expected queued+started+aborted events, got %#v", events)
	}
	aborted := events[2]
	if aborted.Status != tools.ToolEventAborted || !aborted.IsError {
		t.Fatalf("expected aborted error event, got %#v", aborted)
	}
	for _, event := range events {
		if event.Status == tools.ToolEventFinished {
			t.Fatalf("unexpected duplicate finished event after abort: %#v", events)
		}
	}
}

// On cancellation the turn must not be discarded: it finalizes the PARTIAL turn
// (the tool_call streamed before the abort), marks it cancelled in meta so it
// persists in history, and returns the turn-cancelled sentinel (so the task
// layer wraps up gracefully instead of failing the task).
func Test_Runner_Run_cancel_finalizes_partial_turn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := tools.NewRegistry()
	if err := reg.Register("cancel_me", tools.HandlerFunc(func(ctx context.Context, _ tools.Request) (tools.Result, error) {
		cancel()
		<-ctx.Done()
		return tools.Result{}, ctx.Err()
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	pub := &fakePublisher{}
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &callToolThenText{toolName: "cancel_me"},
		Publisher: pub,
		Registry:  reg,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	msg, err := r.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "cancel"))

	if !errors.Is(err, turncancel.ErrTurnCancelled) {
		t.Fatalf("expected turn-cancelled sentinel, got %v", err)
	}
	if string(msg.Meta[metaCancelledKey]) != "true" {
		t.Fatalf("expected meta.cancelled=true on returned message, got meta=%v", msg.Meta)
	}
	// The partial content streamed before the abort (the tool_call) is preserved.
	sawToolCall := false
	for _, p := range msg.Content {
		if p.Type() == protocol.ContentToolCall {
			sawToolCall = true
		}
	}
	if !sawToolCall {
		t.Fatalf("expected partial tool_call preserved in finalized message, got %#v", msg.Content)
	}
	// A message_final event was published carrying the cancelled marker (so the
	// frontend/history reflect the cancelled turn).
	var final *channel.Event
	for i := range pub.events {
		if pub.events[i].Type == channel.EventMessageFinal {
			final = &pub.events[i]
		}
	}
	if final == nil || final.Message == nil {
		t.Fatal("expected a message_final event for the cancelled turn")
	}
	if string(final.Message.Meta[metaCancelledKey]) != "true" {
		t.Fatalf("expected finalized event marked cancelled, got meta=%v", final.Message.Meta)
	}
}

func Test_Runner_Run_publishes_failed_shell_command_metadata(t *testing.T) {
	// Given
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	reg := tools.NewRegistry()
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws, MaxOutput: 64}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "call-1",
		Name:   "shell",
		Args:   protocol.RawToArgs(json.RawMessage(`{"command":"sh","args":["-c","printf turn-partial; exit 9"],"max_output":64}`)),
	})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	pub := &fakePublisher{}
	r, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Provider:          &scriptedProvider{responses: []provider.ChatResponse{{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}}, {Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}}}},
		Publisher:         pub,
		Registry:          reg,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "w1"}, userMessage(t, "run failing shell"))

	// Then
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	events := decodeToolEvents(t, pub.events)
	if len(events) != 3 {
		t.Fatalf("expected queued+started+finished events, got %#v", events)
	}
	failed := events[2]
	if failed.Status != tools.ToolEventFinished || !failed.IsError || failed.ErrorCode != "command_failed" {
		t.Fatalf("expected command failure event, got %#v", failed)
	}
	if failed.Result.ExitCode != 9 || failed.Result.PID <= 0 || failed.Result.OutputBytes != len("turn-partial") {
		t.Fatalf("failed event lost command metadata: %#v", failed)
	}
}

func Test_Runner_Run_sanitizes_valid_local_argument_in_failed_lifecycle_event(t *testing.T) {
	// Given
	ws, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	reg := tools.NewRegistry()
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	secretPath := "turn-valid-secret-token.txt"
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "call-1",
		Name:   "read_file",
		Args:   protocol.RawToArgs(json.RawMessage(`{"path":"` + secretPath + `"}`)),
	})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	pub := &fakePublisher{}
	r, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Provider:          &scriptedProvider{responses: []provider.ChatResponse{{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}}, {Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}}}},
		Publisher:         pub,
		Registry:          reg,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "w1"}, userMessage(t, "read missing secret path"))

	// Then
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	events := decodeToolEvents(t, pub.events)
	if len(events) != 3 {
		t.Fatalf("expected queued+started+finished events, got %#v", events)
	}
	failed := events[2]
	if failed.Status != tools.ToolEventFinished || !failed.IsError || failed.ErrorCode == "" || failed.ErrorMessage == "" {
		t.Fatalf("expected categorized failed event, got %#v", failed)
	}
	for _, event := range pub.events {
		if event.Type == channel.EventToolLifecycle && strings.Contains(string(event.Payload), secretPath) {
			t.Fatalf("tool lifecycle payload leaked valid local argument: %s", string(event.Payload))
		}
	}
}

func decodeToolEvents(t *testing.T, events []channel.Event) []tools.ToolEvent {
	t.Helper()
	out := make([]tools.ToolEvent, 0)
	for _, event := range events {
		if event.Type != channel.EventToolLifecycle {
			continue
		}
		var payload tools.ToolEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode tool event payload %s: %v", string(event.Payload), err)
		}
		out = append(out, payload)
	}
	return out
}
