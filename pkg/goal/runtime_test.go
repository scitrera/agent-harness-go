// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingGoalEnqueuer struct {
	mu      sync.Mutex
	inbound []channel.Inbound
	err     error
}

type recordingContinuationBackend struct {
	mu         sync.Mutex
	admissions []ContinuationAdmission
	receipt    ContinuationReceipt
	err        error
	state      ContinuationState
}

func (b *recordingContinuationBackend) Admit(_ context.Context, admission ContinuationAdmission) (ContinuationReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.admissions = append(b.admissions, admission)
	return b.receipt, b.err
}

func (b *recordingContinuationBackend) Inspect(context.Context, ContinuationReceipt) (ContinuationState, error) {
	return b.state, b.err
}

func (b *recordingContinuationBackend) calls() []ContinuationAdmission {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ContinuationAdmission(nil), b.admissions...)
}

func (e *recordingGoalEnqueuer) Enqueue(_ context.Context, inbound channel.Inbound) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err != nil {
		return e.err
	}
	e.inbound = append(e.inbound, inbound)
	return nil
}

func (e *recordingGoalEnqueuer) messages() []channel.Inbound {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]channel.Inbound(nil), e.inbound...)
}

type fixedGoalVerifier struct {
	result VerificationResult
	err    error
}

func (v fixedGoalVerifier) VerifyGoal(context.Context, VerificationRequest) (VerificationResult, error) {
	return v.result, v.err
}

type runtimeFixture struct {
	service *Service
	ledger  *FileLedger
	runtime *Runtime
}

func newRuntimeFixture(t *testing.T, max uint32, enqueuer channel.Enqueuer, verifier Verifier) runtimeFixture {
	t.Helper()
	stateDir := t.TempDir()
	store, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.Date(2026, 8, 9, 15, 0, 0, 0, time.UTC) }
	service, err := NewService(ServiceConfig{
		Store: store, Now: now, NewID: func() (string, error) { return "goal-1", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewFileLedger(stateDir, now)
	if err != nil {
		t.Fatal(err)
	}
	var idMu sync.Mutex
	nextID := 0
	runtime, err := NewRuntime(RuntimeConfig{
		Service: service, Ledger: ledger,
		Policy: BoundedPolicy{MaxContinuations: max}, Enqueuer: enqueuer,
		Verifier: verifier, DefaultWorkspaceID: "default",
		NewID: func(prefix string) (string, error) {
			idMu.Lock()
			defer idMu.Unlock()
			nextID++
			return fmt.Sprintf("%s%02d", prefix, nextID), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeFixture{service: service, ledger: ledger, runtime: runtime}
}

func TestRuntimePlansBeforeEnqueueAccountsOnceAndStopsAtLimit(t *testing.T) {
	enqueuer := &recordingGoalEnqueuer{}
	fx := newRuntimeFixture(t, 1, enqueuer, nil)
	goalRecord, err := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{
		Objective: "finish <all> requirements",
	})
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1", TaskID: "task-initial"}
	ctx, err := fx.runtime.BeginTurn(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	assistant := assistantWithUsage("assistant-1", addr, 25)
	handled, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant)
	if err != nil || !handled {
		t.Fatalf("after turn handled=%v err=%v", handled, err)
	}
	queued := enqueuer.messages()
	if len(queued) != 1 {
		t.Fatalf("queued = %#v", queued)
	}
	text := messageText(queued[0].Message)
	if !strings.Contains(text, "attempt 1") || !strings.Contains(text, "finish &lt;all&gt; requirements") {
		t.Fatalf("continuation prompt = %q", text)
	}
	if queued[0].Message.Addr.WorkspaceID != "project-a" || queued[0].Message.Addr.TaskID == addr.TaskID {
		t.Fatalf("continuation address = %#v", queued[0].Message.Addr)
	}
	records, err := fx.ledger.List(context.Background(), "project-a", "session-1")
	if err != nil || len(records) != 2 || records[0].Action != DecisionContinuationPlanned || records[1].Action != DecisionContinuationEnqueued {
		t.Fatalf("decisions = %#v err=%v", records, err)
	}
	updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.TokenUsage != 25 {
		t.Fatalf("token usage = %d", updated.TokenUsage)
	}

	// Replaying the same finalized turn neither charges nor enqueues twice.
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}
	updated, _ = fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.TokenUsage != 25 || len(enqueuer.messages()) != 1 {
		t.Fatalf("duplicate turn usage=%d queued=%d", updated.TokenUsage, len(enqueuer.messages()))
	}

	// The admitted follow-up consumes the only continuation. Its natural stop
	// blocks before another enqueue.
	nextAddr := queued[0].Addr
	nextCtx, err := fx.runtime.BeginTurn(context.Background(), nextAddr)
	if err != nil {
		t.Fatal(err)
	}
	second := assistantWithUsage("assistant-2", nextAddr, 10)
	if _, err := fx.runtime.AfterTurn(nextCtx, nextAddr, []protocol.ChatMessage{second}, second); err != nil {
		t.Fatal(err)
	}
	updated, _ = fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.Status != spec.SessionGoalBlocked || !strings.Contains(updated.BlockedReason, "maximum automatic continuations") {
		t.Fatalf("limited goal = %#v", updated)
	}
	if len(enqueuer.messages()) != 1 {
		t.Fatalf("limit enqueued another turn: %d", len(enqueuer.messages()))
	}
}

func TestRuntimeAdmitsOneContinuationAcrossReplicasForSameTurn(t *testing.T) {
	blobs := newMemoryCASBlobs()
	firstStore, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	secondStore, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	firstService, _ := NewService(ServiceConfig{Store: firstStore})
	secondService, _ := NewService(ServiceConfig{Store: secondStore})
	if _, err := firstService.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "exactly once"}); err != nil {
		t.Fatal(err)
	}
	firstLedger, _ := NewCASLedger(CASLedgerConfig{Blobs: blobs, MaxRetries: 256})
	secondLedger, _ := NewCASLedger(CASLedgerConfig{Blobs: blobs, MaxRetries: 256})
	enqueuer := &recordingGoalEnqueuer{}
	firstRuntime, _ := NewRuntime(RuntimeConfig{
		Service: firstService, Ledger: firstLedger,
		Policy: BoundedPolicy{MaxContinuations: 3}, Enqueuer: enqueuer,
	})
	secondRuntime, _ := NewRuntime(RuntimeConfig{
		Service: secondService, Ledger: secondLedger,
		Policy: BoundedPolicy{MaxContinuations: 3}, Enqueuer: enqueuer,
	})
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
	firstContext, err := firstRuntime.BeginTurn(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	secondContext, err := secondRuntime.BeginTurn(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	assistant := assistantWithUsage("assistant-shared", addr, 5)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, call := range []struct {
		runtime *Runtime
		ctx     context.Context
	}{
		{runtime: firstRuntime, ctx: firstContext},
		{runtime: secondRuntime, ctx: secondContext},
	} {
		wg.Add(1)
		go func(call struct {
			runtime *Runtime
			ctx     context.Context
		}) {
			defer wg.Done()
			<-start
			_, err := call.runtime.AfterTurn(call.ctx, addr, []protocol.ChatMessage{assistant}, assistant)
			errs <- err
		}(call)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if queued := enqueuer.messages(); len(queued) != 1 {
		t.Fatalf("queued continuations = %d", len(queued))
	}
	records, err := firstLedger.List(context.Background(), "project-a", "session-1")
	if err != nil || len(records) != 2 || records[0].Action != DecisionContinuationPlanned || records[1].Action != DecisionContinuationEnqueued {
		t.Fatalf("records = %#v err=%v", records, err)
	}
	goals, err := secondService.ListGoals(context.Background(), "project-a", "session-1")
	if err != nil || len(goals) != 1 || goals[0].TokenUsage != 5 {
		t.Fatalf("goals = %#v err=%v", goals, err)
	}
}

// An exhausted budget grants exactly one wrap-up round and then blocks. The goal
// is not cut off at the instant the budget trips: that would end it mid-thought,
// leaving the operator a blocked goal and no statement of where it got to.
func TestRuntimeTokenBudgetGrantsOneWrapUpRoundThenBlocks(t *testing.T) {
	enqueuer := &recordingGoalEnqueuer{}
	fx := newRuntimeFixture(t, 3, enqueuer, nil)
	budget := uint64(20)
	goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{
		Objective: "bounded", TokenBudget: &budget,
	})
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}

	// First turn over budget: the wrap-up round is queued, goal still active.
	ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
	assistant := assistantWithUsage("assistant-budget", addr, 25)
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}
	updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.Status != spec.SessionGoalActive || updated.TokenUsage != 25 {
		t.Fatalf("after the budget tripped, goal = %#v; want still active for the wrap-up", updated)
	}
	queued := enqueuer.messages()
	if len(queued) != 1 {
		t.Fatalf("queued = %d, want the one wrap-up round", len(queued))
	}
	// The wrap-up must not read like an ordinary "keep going" round.
	text := messageText(queued[0].Message)
	if !strings.Contains(text, "FINAL round") {
		t.Fatalf("wrap-up round does not announce itself as final: %q", text)
	}
	if !strings.Contains(text, "Do NOT mark the goal completed") {
		t.Fatalf("wrap-up round does not forbid claiming completion: %q", text)
	}
	records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
	if len(records) != 2 || records[0].Reason != ReasonTokenBudgetWrapUp {
		t.Fatalf("wrap-up decisions = %#v", records)
	}

	// Second turn still over budget: no second wrap-up, the goal blocks.
	ctx2, _ := fx.runtime.BeginTurn(context.Background(), addr)
	assistant2 := assistantWithUsage("assistant-budget-2", addr, 5)
	if _, err := fx.runtime.AfterTurn(ctx2, addr, []protocol.ChatMessage{assistant2}, assistant2); err != nil {
		t.Fatal(err)
	}
	final, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if final.Status != spec.SessionGoalBlocked {
		t.Fatalf("goal = %#v, want blocked after the wrap-up was spent", final)
	}
	if len(enqueuer.messages()) != 1 {
		t.Fatalf("queued = %d, want no round after the wrap-up", len(enqueuer.messages()))
	}
	after, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
	if after[len(after)-1].Reason != ReasonTokenBudget {
		t.Fatalf("final decision = %#v, want a token_budget block", after[len(after)-1])
	}
}

// A host that disabled automatic follow-ups must not get a surprise extra turn
// from the wrap-up path.
func TestRuntimeTokenBudgetBlocksWithoutWrapUpWhenContinuationsDisabled(t *testing.T) {
	enqueuer := &recordingGoalEnqueuer{}
	fx := newRuntimeFixture(t, 0, enqueuer, nil)
	budget := uint64(20)
	goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{
		Objective: "bounded", TokenBudget: &budget,
	})
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
	ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
	assistant := assistantWithUsage("assistant-budget", addr, 25)
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}

	updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.Status != spec.SessionGoalBlocked || len(enqueuer.messages()) != 0 {
		t.Fatalf("goal = %#v queued=%d; want blocked with no wrap-up", updated, len(enqueuer.messages()))
	}
	records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
	if len(records) != 1 || records[0].Reason != ReasonTokenBudget {
		t.Fatalf("budget decisions = %#v", records)
	}
}

func TestRuntimeNoEnqueuerLeavesGoalActiveAndRecordsStop(t *testing.T) {
	fx := newRuntimeFixture(t, 3, nil, nil)
	goalRecord, _ := fx.service.CreateGoal(context.Background(), "default", "session-1", CreateInput{Objective: "manual"})
	addr := protocol.MessageAddress{ThreadID: "session-1"}
	ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
	assistant := assistantWithUsage("assistant-manual", addr, 5)
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}
	updated, _ := fx.service.Goal(context.Background(), "default", "session-1", goalRecord.ID)
	if updated.Status != spec.SessionGoalActive {
		t.Fatalf("manual goal status = %s", updated.Status)
	}
	records, _ := fx.ledger.List(context.Background(), "default", "session-1")
	if len(records) != 1 || records[0].Reason != ReasonEnqueueUnavailable {
		t.Fatalf("manual decisions = %#v", records)
	}
}

func TestRuntimeDisabledContinuationLeavesGoalActive(t *testing.T) {
	enqueuer := &recordingGoalEnqueuer{}
	fx := newRuntimeFixture(t, 0, enqueuer, nil)
	goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "manual only"})
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
	ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
	assistant := assistantWithUsage("assistant-disabled", addr, 5)
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}
	updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.Status != spec.SessionGoalActive || len(enqueuer.messages()) != 0 {
		t.Fatalf("disabled goal = %#v queued=%d", updated, len(enqueuer.messages()))
	}
	records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
	if len(records) != 1 || records[0].Reason != ReasonMaxContinuations || records[0].Action != DecisionStopped {
		t.Fatalf("disabled decisions = %#v", records)
	}
}

func TestRuntimeContinuationPreservesDelegatedAuthorityOrFailsClosed(t *testing.T) {
	authority := tools.MemoryAuthority{SubjectType: "user", SubjectID: "alice", GrantID: "grant-1"}

	t.Run("missing handoff", func(t *testing.T) {
		enqueuer := &recordingGoalEnqueuer{}
		fx := newRuntimeFixture(t, 3, enqueuer, nil)
		_, _ = fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "secure"})
		addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
		ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
		ctx = tools.WithMemoryAuthority(ctx, authority)
		assistant := assistantWithUsage("assistant-secure", addr, 5)
		if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err == nil || !strings.Contains(err.Error(), "authority handoff") {
			t.Fatalf("authority error = %v", err)
		}
		if len(enqueuer.messages()) != 0 {
			t.Fatal("continuation enqueued without authority handoff")
		}
		records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
		if len(records) != 0 {
			t.Fatalf("unsafe continuation was planned: %#v", records)
		}
	})

	t.Run("single use handoff", func(t *testing.T) {
		enqueuer := &recordingGoalEnqueuer{}
		fx := newRuntimeFixture(t, 3, enqueuer, nil)
		handoff := authhandoff.New()
		fx.runtime.authHandoff = handoff
		_, _ = fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "secure"})
		addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
		ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
		ctx = tools.WithMemoryAuthority(ctx, authority)
		assistant := assistantWithUsage("assistant-secure", addr, 5)
		if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
			t.Fatal(err)
		}
		queued := enqueuer.messages()
		if len(queued) != 1 {
			t.Fatalf("queued = %#v", queued)
		}
		resolved, ok := handoff.ResolveMessage(queued[0].Message)
		if !ok || resolved != authority {
			t.Fatalf("resolved authority = %#v ok=%v", resolved, ok)
		}
		if _, ok := handoff.ResolveMessage(queued[0].Message); ok {
			t.Fatal("authority handoff token was reusable")
		}
	})
}

func TestRuntimeUsesDurableContinuationBackendWithoutSerializingAuthority(t *testing.T) {
	backend := &recordingContinuationBackend{
		receipt: ContinuationReceipt{Backend: "aether", TaskID: "task-durable-1"}, state: ContinuationRunning,
	}
	fx := newRuntimeFixture(t, 3, nil, nil)
	fx.runtime.continuationBackend = backend
	_, _ = fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "recover safely"})
	addr := protocol.MessageAddress{
		WorkspaceID: "project-a", ThreadID: "session-1", TaskID: "task-parent",
	}
	ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
	authority := tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"}
	ctx = tools.WithMemoryAuthority(ctx, authority)
	assistant := assistantWithUsage("assistant-durable", addr, 7)
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}
	calls := backend.calls()
	if len(calls) != 1 || calls[0].Authority != authority || calls[0].ParentTaskID != "task-parent" {
		t.Fatalf("admissions = %#v", calls)
	}
	if calls[0].Inbound.Addr.TaskID != "" || calls[0].Inbound.Message.Addr.TaskID != "" {
		t.Fatalf("backend admission carried synthetic task id: %#v", calls[0].Inbound.Addr)
	}
	payload, err := MarshalContinuationEnvelope(calls[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "grant-1") || strings.Contains(string(payload), "alice") ||
		strings.Contains(string(payload), "authority_handoff") {
		t.Fatalf("authority leaked into continuation payload: %s", payload)
	}
	records, err := fx.ledger.List(context.Background(), "project-a", "session-1")
	if err != nil || len(records) != 2 || records[0].Action != DecisionContinuationPlanned ||
		records[1].Action != DecisionContinuationAdmitted || records[1].ContinuationTaskID != "task-durable-1" ||
		records[1].ContinuationBackend != "aether" || records[0].ContinuationTaskID != "" ||
		records[0].Continuation == nil || records[0].Continuation.Inbound.Message.ID != calls[0].Inbound.Message.ID {
		t.Fatalf("records = %#v err=%v", records, err)
	}
	ledgerJSON, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ledgerJSON), "grant-1") || strings.Contains(string(ledgerJSON), "alice") ||
		strings.Contains(string(ledgerJSON), "authority_handoff") {
		t.Fatalf("authority leaked into continuation ledger: %s", ledgerJSON)
	}
	inspected, err := fx.runtime.InspectDecision(context.Background(), records[1])
	if err != nil || inspected != ContinuationRunning {
		t.Fatalf("inspection = %q err=%v", inspected, err)
	}
}

func TestRuntimeAccountsExplicitCompletingTurnWithoutContinuing(t *testing.T) {
	enqueuer := &recordingGoalEnqueuer{}
	fx := newRuntimeFixture(t, 3, enqueuer, nil)
	goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "complete"})
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
	ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
	if _, err := fx.service.UpdateGoal(context.Background(), "project-a", "session-1", UpdateInput{
		ID: goalRecord.ID, Status: spec.SessionGoalCompleted, Evidence: []string{"artifact:done"},
	}); err != nil {
		t.Fatal(err)
	}
	assistant := assistantWithUsage("assistant-complete", addr, 40)
	if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
		t.Fatal(err)
	}
	updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if updated.Status != spec.SessionGoalCompleted || updated.TokenUsage != 40 || len(enqueuer.messages()) != 0 {
		t.Fatalf("completed goal = %#v queued=%d", updated, len(enqueuer.messages()))
	}
	records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
	if len(records) != 1 || records[0].Action != DecisionCompleted || records[0].Reason != ReasonGoalTerminal {
		t.Fatalf("completion decisions = %#v", records)
	}
}

func TestRuntimeVerifierSatisfiedCompletesAndFailureBlocks(t *testing.T) {
	t.Run("revision", func(t *testing.T) {
		enqueuer := &recordingGoalEnqueuer{}
		fx := newRuntimeFixture(t, 3, enqueuer, fixedGoalVerifier{result: VerificationResult{
			Feedback: "add missing proof", FailedCriteria: []string{"prove completion"}, Evidence: []string{"verifier:1"},
		}})
		_, _ = fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "verified"})
		addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
		ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
		assistant := assistantWithUsage("assistant-revision", addr, 10)
		if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
			t.Fatal(err)
		}
		queued := enqueuer.messages()
		if len(queued) != 1 || !strings.Contains(messageText(queued[0].Message), "add missing proof") {
			t.Fatalf("revision queue = %#v", queued)
		}
		records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
		if len(records) != 2 || records[0].Reason != ReasonVerifierRevision {
			t.Fatalf("revision decisions = %#v", records)
		}
	})

	t.Run("satisfied", func(t *testing.T) {
		enqueuer := &recordingGoalEnqueuer{}
		fx := newRuntimeFixture(t, 3, enqueuer, fixedGoalVerifier{result: VerificationResult{
			Satisfied: true, Feedback: "all criteria pass", Evidence: []string{"check:1"},
		}})
		goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "verified"})
		addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
		ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
		assistant := assistantWithUsage("assistant-verified", addr, 10)
		if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
			t.Fatal(err)
		}
		updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
		if updated.Status != spec.SessionGoalCompleted || !containsAll(updated.Evidence, "check:1", "verifier:satisfied") {
			t.Fatalf("verified goal = %#v", updated)
		}
		if len(enqueuer.messages()) != 0 {
			t.Fatal("satisfied verifier enqueued continuation")
		}
	})

	// A verifier error is fail-closed, but only once the fault has PERSISTED.
	// Blocking on the first error conflates "this goal cannot succeed" with "the
	// grader had a bad minute", permanently killing a goal that was progressing.
	t.Run("error is tolerated below the streak", func(t *testing.T) {
		enqueuer := &recordingGoalEnqueuer{}
		fx := newRuntimeFixture(t, 5, enqueuer, fixedGoalVerifier{err: errors.New("grader offline")})
		goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "verified"})
		addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
		ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
		assistant := assistantWithUsage("assistant-error", addr, 10)

		if _, err := fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant); err != nil {
			t.Fatalf("a single verifier error must not fail the turn: %v", err)
		}

		updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
		if updated.Status != spec.SessionGoalActive {
			t.Fatalf("goal = %#v, want still active after one verifier error", updated)
		}
		if len(enqueuer.messages()) != 1 {
			t.Fatalf("queued = %d, want the goal to continue and retry", len(enqueuer.messages()))
		}
		records, _ := fx.ledger.List(context.Background(), "project-a", "session-1")
		// Recorded as verifier_failed, not active_goal: otherwise the streak is
		// uncountable and the fault is invisible in the audit trail.
		if records[0].Reason != ReasonVerifierFailed {
			t.Fatalf("first decision = %#v, want reason verifier_failed", records[0])
		}
		if !strings.Contains(records[0].Detail, "grader offline") {
			t.Fatalf("decision detail loses the verifier error: %q", records[0].Detail)
		}
	})

	t.Run("error blocks once it persists", func(t *testing.T) {
		fx := newRuntimeFixture(t, 5, &recordingGoalEnqueuer{}, fixedGoalVerifier{err: errors.New("grader offline")})
		goalRecord, _ := fx.service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "verified"})
		addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}

		// Drive consecutive failing turns up to the streak.
		var lastErr error
		for i := 0; i < DefaultVerifierFailureStreak; i++ {
			ctx, _ := fx.runtime.BeginTurn(context.Background(), addr)
			assistant := assistantWithUsage(fmt.Sprintf("assistant-error-%d", i), addr, 10)
			_, lastErr = fx.runtime.AfterTurn(ctx, addr, []protocol.ChatMessage{assistant}, assistant)
		}

		if lastErr == nil {
			t.Fatal("a persistent verifier failure must surface the error")
		}
		updated, _ := fx.service.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
		if updated.Status != spec.SessionGoalBlocked || !strings.Contains(updated.BlockedReason, "grader offline") {
			t.Fatalf("failed verifier goal = %#v", updated)
		}
	})

	// One good verify in the middle must reset the run, or intermittent faults
	// accumulate into a block that never should have happened.
	t.Run("a successful verify resets the streak", func(t *testing.T) {
		records := []DecisionRecord{
			{GoalID: "g1", TurnMessageID: "t1", Reason: ReasonVerifierFailed},
			{GoalID: "g1", TurnMessageID: "t2", Reason: ReasonVerifierFailed},
			{GoalID: "g1", TurnMessageID: "t3", Reason: ReasonVerifierRevision},
			{GoalID: "g1", TurnMessageID: "t4", Reason: ReasonVerifierFailed},
		}
		if got := consecutiveVerifierFailures(records, "g1"); got != 1 {
			t.Fatalf("streak = %d, want 1 — only the unbroken tail counts", got)
		}
		if got := consecutiveVerifierFailures(records[:2], "g1"); got != 2 {
			t.Fatalf("streak = %d, want 2", got)
		}
		if got := consecutiveVerifierFailures(records, "other-goal"); got != 0 {
			t.Fatalf("streak = %d, want 0 for an unrelated goal", got)
		}
	})

	// One continued turn writes two records sharing a TurnMessageID. Counting
	// records instead of turns would make a tolerance of 3 block after 2.
	t.Run("the streak counts turns not ledger records", func(t *testing.T) {
		records := []DecisionRecord{
			{GoalID: "g1", TurnMessageID: "t1", Action: DecisionContinuationPlanned, Reason: ReasonVerifierFailed},
			{GoalID: "g1", TurnMessageID: "t1", Action: DecisionContinuationEnqueued, Reason: ReasonVerifierFailed},
			{GoalID: "g1", TurnMessageID: "t2", Action: DecisionContinuationPlanned, Reason: ReasonVerifierFailed},
			{GoalID: "g1", TurnMessageID: "t2", Action: DecisionContinuationEnqueued, Reason: ReasonVerifierFailed},
		}
		if got := consecutiveVerifierFailures(records, "g1"); got != 2 {
			t.Fatalf("streak = %d, want 2 turns (not 4 records)", got)
		}
	})
}

func assistantWithUsage(id string, addr protocol.MessageAddress, total uint64) protocol.ChatMessage {
	raw, _ := json.Marshal(map[string]uint64{"total_tokens": total})
	part, _ := protocol.NewTextPart("assistant result")
	return protocol.ChatMessage{
		ID: id, Role: protocol.RoleAssistant, Addr: addr,
		Content: []protocol.ContentPart{part},
		Meta:    map[string]json.RawMessage{compaction.MetaUsage: raw},
	}
}

func messageText(message protocol.ChatMessage) string {
	var out strings.Builder
	for _, part := range message.Content {
		if text, ok := part.AsText(); ok {
			out.WriteString(text.Text)
		}
	}
	return out.String()
}

func containsAll(values []string, wanted ...string) bool {
	for _, want := range wanted {
		found := false
		for _, value := range values {
			if value == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
