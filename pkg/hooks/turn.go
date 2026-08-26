package hooks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// TurnPhase identifies a point in a turn's lifecycle. It is the discriminator for
// TurnEvent, mirroring how EventName discriminates command-hook invocations.
type TurnPhase string

const (
	PhaseTurnStarted        TurnPhase = "turn_started"
	PhaseUserPromptSubmit   TurnPhase = "user_prompt_submit"
	PhaseModelCallStarted   TurnPhase = "model_call_started"
	PhaseModelCallFinished  TurnPhase = "model_call_finished"
	PhaseCompacted          TurnPhase = "compacted"
	PhaseResourceReferenced TurnPhase = "resource_referenced"
	PhaseRecoveryStarted    TurnPhase = "recovery_started"
	// PhaseProviderRetry fires when a provider call failed and the runner has
	// decided to try again — before the wait, so an observer can show the user
	// what is happening while it happens. Distinct from PhaseRecoveryStarted,
	// which is about resuming a turn after a process restart.
	PhaseProviderRetry TurnPhase = "provider_retry"
	PhaseTurnFinished  TurnPhase = "turn_finished"
)

// RetryStrategy names what the runner is about to do about a failed provider
// call. It is the discriminator a renderer needs to word the notice: a model
// switch is a different user-facing story from waiting out a rate limit.
type RetryStrategy string

const (
	// RetryTrimContext drops the oldest history and retries the same model.
	RetryTrimContext RetryStrategy = "trim_context"
	// RetryBackoff waits RetryIn and retries the same model.
	RetryBackoff RetryStrategy = "backoff"
	// RetryFallbackModel switches to a different model; RetryIn is zero.
	RetryFallbackModel RetryStrategy = "fallback_model"
)

// TurnEvent is a turn-lifecycle signal delivered to TurnObservers. It carries
// enough to CORRELATE and snapshot (addr, iteration, model); the message content
// itself lives in the history store (persisted per step via SaveHistory). Err is
// set on the *Finished phases when the step/turn errored.
type TurnEvent struct {
	Phase     TurnPhase
	Addr      protocol.MessageAddress
	Iteration int    // tool-loop round: 0 before the first model call
	Model     string // model in use for this step, when known
	Err       error  // set on model_call_finished / turn_finished on error
	MessageID string // stable transcript message reference, when known
	// OperationID distinguishes repeated reference/recovery events within one
	// branch. Reference points at an authority-owned record without copying it.
	OperationID string
	Reference   *tools.ResultReference

	// Attempt is the 1-based ordinal of the retry about to be made, within its
	// own strategy's budget. Set on PhaseProviderRetry.
	Attempt int
	// RetryIn is the wait the runner will observe before retrying. Zero for
	// strategies that retry immediately. Set on PhaseProviderRetry.
	RetryIn time.Duration
	// Strategy is what the runner is about to do about the failure. Set on
	// PhaseProviderRetry.
	Strategy RetryStrategy
	// FailureKind is the classified provider failure ("rate_limit",
	// "context_overflow", …), empty when the error was not a classified
	// ProviderError. Set on PhaseProviderRetry.
	FailureKind string
	// NextModel is the model the runner is switching to. Set only for
	// RetryFallbackModel.
	NextModel string
}

type executionBranchKey struct{}
type executionLedgerDisabledKey struct{}

// WithExecutionBranchID installs the stable branch selected for one turn.
func WithExecutionBranchID(ctx context.Context, branchID string) context.Context {
	if branchID == "" {
		return ctx
	}
	return context.WithValue(ctx, executionBranchKey{}, branchID)
}

// ExecutionBranchIDFrom returns the turn branch carried on ctx.
func ExecutionBranchIDFrom(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(executionBranchKey{}).(string)
	return value, ok && value != ""
}

// WithoutExecutionLedger marks an ephemeral turn whose observer signals must
// not be written to durable operational history.
func WithoutExecutionLedger(ctx context.Context) context.Context {
	return context.WithValue(ctx, executionLedgerDisabledKey{}, true)
}

func ExecutionLedgerDisabled(ctx context.Context) bool {
	disabled, _ := ctx.Value(executionLedgerDisabledKey{}).(bool)
	return disabled
}

// TurnObserver observes the turn lifecycle without a veto — audit, telemetry,
// checkpoint/sync. Best-effort: it must not block the turn, and a nil observer or
// empty slice is a no-op. Peer to ToolObserver (which covers the tool
// sub-lifecycle). A single method keyed by Phase keeps it cheap to implement (a
// consumer switches on the phases it cares about) and lets new phases be added
// without breaking implementers — the same reason the messaging spec discriminates
// stream events by a "phase"/"event" field.
type TurnObserver interface {
	ObserveTurn(ctx context.Context, ev TurnEvent)
}

// ToolResultObserver is an optional richer companion to ToolObserver. The turn
// loop calls it only after an admitted tool actually ran, allowing evidence and
// provenance consumers to inspect bounded result metadata without changing the
// existing lifecycle interface or receiving tool payload content.
type ToolResultObserver interface {
	ToolResult(ctx context.Context, call ToolCall, result tools.Result, err error)
}

// NotifyTurn fans an event out to every observer, best-effort (nil-safe).
func NotifyTurn(ctx context.Context, ev TurnEvent, observers []TurnObserver) {
	for _, o := range observers {
		if o != nil {
			o.ObserveTurn(ctx, ev)
		}
	}
}

// ObserveTurn makes a command-hook Runtime a TurnObserver: it bridges the turn
// phases that HAVE a command-hook event to that event's external hooks. Phases
// with no corresponding event (the model-call boundaries) are dropped — the
// in-process TurnObserver seam still delivers them. This is what "populates" the
// previously-unwired UserPromptSubmit + PostCompact command-hook events.
func (r *Runtime) ObserveTurn(ctx context.Context, ev TurnEvent) {
	if r == nil || recursionGuarded(ctx) {
		return
	}
	event, ok := turnPhaseToEvent(ev.Phase)
	if !ok {
		return
	}
	_, _ = r.Dispatch(ctx, Invocation{Event: event, Input: turnEventInput(ev)})
}

// turnPhaseToEvent maps a turn phase to its command-hook event, when one exists.
// Only phases with a clean, already-defined event + fire site are mapped;
// SessionStart/SessionEnd and PreCompact are intentionally NOT mapped here (the
// per-turn runner has no session-open/close boundary, and compaction is only
// detectable AFTER it happens — a pre-compact signal needs assembler cooperation).
func turnPhaseToEvent(p TurnPhase) (EventName, bool) {
	switch p {
	case PhaseUserPromptSubmit:
		return EventUserPromptSubmit, true
	case PhaseCompacted:
		return EventPostCompact, true
	default:
		return "", false
	}
}

func turnEventInput(ev TurnEvent) json.RawMessage {
	env := map[string]any{
		"phase":      string(ev.Phase),
		"addr":       ev.Addr,
		"iteration":  ev.Iteration,
		"model":      ev.Model,
		"message_id": ev.MessageID,
	}
	if ev.Err != nil {
		env["error"] = ev.Err.Error()
	}
	if ev.Phase == PhaseProviderRetry {
		env["attempt"] = ev.Attempt
		env["retry_in_ms"] = ev.RetryIn.Milliseconds()
		env["strategy"] = string(ev.Strategy)
		env["failure_kind"] = ev.FailureKind
		if ev.NextModel != "" {
			env["next_model"] = ev.NextModel
		}
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return nil
	}
	return raw
}
