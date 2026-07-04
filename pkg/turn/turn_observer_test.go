package turn

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingTurnObserver struct {
	events []hooks.TurnEvent
}

func (o *recordingTurnObserver) ObserveTurn(_ context.Context, ev hooks.TurnEvent) {
	o.events = append(o.events, ev)
}

// Test_Runner_Run_fires_turn_lifecycle_observers asserts the TurnObserver seam
// fires at every boundary in order for a turn that makes one tool call: start →
// prompt → (model call ×2, bracketing the tool call) → finish. This is the
// surface a checkpoint/sync or audit backend hooks into.
func Test_Runner_Run_fires_turn_lifecycle_observers(t *testing.T) {
	ctx := context.Background()
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "c1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	obs := &recordingTurnObserver{}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Publisher:         &fakePublisher{},
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		MaxToolIterations: 2,
		TurnObservers:     []hooks.TurnObserver{obs},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	userPart, err := protocol.NewTextPart("please record")
	if err != nil {
		t.Fatalf("user text: %v", err)
	}

	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1"},
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}}); err != nil {
		t.Fatalf("run: %v", err)
	}

	var phases []hooks.TurnPhase
	for _, e := range obs.events {
		phases = append(phases, e.Phase)
	}
	want := []hooks.TurnPhase{
		hooks.PhaseTurnStarted,
		hooks.PhaseUserPromptSubmit,
		hooks.PhaseModelCallStarted, hooks.PhaseModelCallFinished, // iteration 0 (tool call)
		hooks.PhaseModelCallStarted, hooks.PhaseModelCallFinished, // iteration 1 (final answer)
		hooks.PhaseTurnFinished,
	}
	if len(phases) != len(want) {
		t.Fatalf("phase sequence = %v, want %v", phases, want)
	}
	for i := range want {
		if phases[i] != want[i] {
			t.Fatalf("phase[%d] = %s, want %s (full: %v)", i, phases[i], want[i], phases)
		}
	}
	// The two model calls carry their tool-loop iteration (0 then 1).
	if obs.events[2].Iteration != 0 || obs.events[4].Iteration != 1 {
		t.Fatalf("model-call iterations = %d,%d; want 0,1", obs.events[2].Iteration, obs.events[4].Iteration)
	}
	// A clean turn: the terminal event carries no error, and all addresses resolved.
	last := obs.events[len(obs.events)-1]
	if last.Err != nil {
		t.Fatalf("turn_finished err = %v, want nil", last.Err)
	}
	if last.Addr.ThreadID != "t1" {
		t.Fatalf("turn_finished addr thread = %q, want t1", last.Addr.ThreadID)
	}
}

// Test_Runner_Run_no_turn_observers_is_noop guards the unconfigured path: a turn
// with no TurnObservers runs unchanged (no panic, no requirement).
func Test_Runner_Run_no_turn_observers_is_noop(t *testing.T) {
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: mustText(t, "hi")}},
	}}
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Registry:  tools.NewRegistry(),
		Provider:  provider,
		Publisher: &fakePublisher{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"},
		protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: mustText(t, "hello")}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func mustText(t *testing.T, s string) []protocol.ContentPart {
	t.Helper()
	p, err := protocol.NewTextPart(s)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return []protocol.ContentPart{p}
}
