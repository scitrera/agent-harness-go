package executionledger

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestServiceObservesLifecycleReferencesAndRendersCommand(t *testing.T) {
	store := deterministicStore(20)
	service := &Service{Store: store, Authority: "aether-kv"}
	addr := protocol.MessageAddress{WorkspaceID: "workspace-a", ThreadID: "session-a", TaskID: "task-a"}
	ctx := hooks.WithExecutionBranchID(context.Background(), "branch-a")
	service.ObserveTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseTurnStarted, Addr: addr})
	service.ObserveTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseModelCallStarted, Addr: addr, Model: "model-a"})
	service.ObserveTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseResourceReferenced, Addr: addr, OperationID: "ref-1", Reference: &tools.ResultReference{System: "refinement-store", Kind: "refinement_record", ID: "record-a"}})
	service.ObserveTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseResourceReferenced, Addr: addr, OperationID: "skill-1", Reference: &tools.ResultReference{System: "skill-catalog", Kind: "target", ID: "review-workflow"}})
	service.ObserveTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseTurnFinished, Addr: addr})

	text, err := service.RunExecutionLedgerCommand(ctx, addr, protocol.ChatMessage{}, "--reference record-a --limit 5")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"source: aether-kv authoritative", "refinement_referenced", "record-a", "branch=branch-a"} {
		if !strings.Contains(text, want) {
			t.Fatalf("command output missing %q:\n%s", want, text)
		}
	}
	page, err := store.Query(ctx, sessionRef(addr), Query{Types: []EventType{EventSkillReferenced}})
	if err != nil || len(page.Events) != 1 || page.Events[0].Reference == nil || page.Events[0].Reference.ID != "review-workflow" {
		t.Fatalf("skill usage events = %#v, %v", page.Events, err)
	}
}

func TestServiceSkipsEphemeralObservation(t *testing.T) {
	store := deterministicStore(20)
	service := &Service{Store: store}
	addr := protocol.MessageAddress{WorkspaceID: "workspace-a", ThreadID: "session-a", TaskID: "task-a"}
	service.ObserveTurn(hooks.WithoutExecutionLedger(context.Background()), hooks.TurnEvent{Phase: hooks.PhaseTurnStarted, Addr: addr})
	page, err := store.Query(context.Background(), sessionRef(addr), Query{})
	if err != nil || len(page.Events) != 0 {
		t.Fatalf("ephemeral events = %+v, %v", page.Events, err)
	}
}
