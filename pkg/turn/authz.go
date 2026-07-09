package turn

import (
	"context"
	"log/slog"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// AuthzOutcome is a tool authorizer's verdict on a call.
type AuthzOutcome int

const (
	// Abstain: this authorizer has no opinion; the pipeline moves on.
	Abstain AuthzOutcome = iota
	// Allow: the call is authorized (subject to a later Deny/escalation).
	Allow
	// Deny: the call is refused; short-circuits the pipeline.
	Deny
	// Prompt: the call needs human approval before it may run.
	Prompt
)

// AuthzDecision is an authorizer's verdict plus optional metadata (a reason for
// logs/telemetry, and the grant Scope the user chose when a Prompt is resolved).
type AuthzDecision struct {
	Outcome AuthzOutcome
	Reason  string
	Scope   string
}

// AuthzInput is the per-call context handed to each authorizer.
type AuthzInput struct {
	Call       protocol.ToolInvokeEnvelope
	Addr       protocol.MessageAddress
	Trust      tools.TrustLevel
	ProviderID string // "" = local registry tool
}

// ToolAuthorizer decides whether a tool call may proceed. Authorizers compose in
// a pipeline (see resolveAuthz): a plug-in safety classifier, the grant
// short-circuit, and the interactive prompt are all ToolAuthorizers.
type ToolAuthorizer interface {
	Authorize(ctx context.Context, in AuthzInput) AuthzDecision
}

// noopAuthorizer always abstains — the default safety slot.
type noopAuthorizer struct{}

func (noopAuthorizer) Authorize(context.Context, AuthzInput) AuthzDecision {
	return AuthzDecision{Outcome: Abstain}
}

// resolveAuthz runs authorizers in order, folding their verdicts against seed:
// a Deny short-circuits; a Prompt outranks an Allow (a stronger requirement
// wins) but an Allow never downgrades a Prompt; an Abstain passes. When no
// authorizer decides, the seed stands (seed Allow with none deciding → Allow).
// It is order-independent for Allow-vs-Prompt (Prompt always wins), which is the
// property the resolver unit test pins.
func resolveAuthz(ctx context.Context, in AuthzInput, seed AuthzOutcome, authorizers ...ToolAuthorizer) AuthzDecision {
	best := AuthzDecision{Outcome: seed}
	for _, a := range authorizers {
		if a == nil {
			continue
		}
		d := a.Authorize(ctx, in)
		switch d.Outcome {
		case Deny:
			return d
		case Prompt:
			best = d
		case Allow:
			if best.Outcome != Prompt {
				best = d
			}
		case Abstain:
			// no opinion
		}
	}
	return best
}

// trustBase maps a tool's Trust hint to the pipeline's base requirement: a
// pre-authorized/default tool starts Allow, a requires-approval tool starts
// Prompt. Local tools reach the pipeline only via the runtime policy's
// requires-approval trigger, so their base is Prompt regardless of Trust.
func trustBase(trust tools.TrustLevel) AuthzOutcome {
	if trust == tools.TrustRequiresApproval {
		return Prompt
	}
	return Allow
}

// safety returns the configured safety authorizer, or a no-op that abstains.
func (r *Runner) safety() ToolAuthorizer {
	if r.safetyAuthorizer != nil {
		return r.safetyAuthorizer
	}
	return noopAuthorizer{}
}

// authorizeTool composes the uniform authorization pipeline for a single call.
// base is the tool's intrinsic requirement (Prompt for a policy-gated local
// tool / a requires-approval provider tool; Allow otherwise). The grant
// authorizer may authorize a base Prompt away (a pre-authorized trust, or a
// durable/always grant — recording a session grant); the safety authorizer runs
// for EVERY tool (including pre-authorized) and may Deny or escalate an Allow to
// a Prompt; the interactive authorizer resolves a remaining Prompt. The returned
// decision is Allow (run the tool), Deny (refuse), or — on turn cancellation
// during a prompt — Deny with ctx.Err() set (the caller aborts the turn).
func (r *Runner) authorizeTool(ctx context.Context, in AuthzInput, base AuthzOutcome) AuthzDecision {
	// grant short-circuit: pre-authorized trust or a durable grant → Allow,
	// clearing a base Prompt. Deny is defensive (grant never denies today).
	grant := (grantAuthorizer{r: r}).Authorize(ctx, in)
	switch grant.Outcome {
	case Deny:
		return grant
	case Allow:
		base = Allow
	}
	// safety runs for every tool; it may deny or escalate Allow→Prompt.
	switch safety := r.safety().Authorize(ctx, in); safety.Outcome {
	case Deny:
		return safety
	case Prompt:
		base = Prompt
	case Allow:
		if base != Prompt {
			base = Allow
		}
	}
	if base == Prompt {
		// A remaining prompt needs an approval channel; with none wired the tool
		// cannot be authorized (local tools always have one — the caller gates on
		// r.approvals before reaching here).
		if r.approvals == nil {
			return AuthzDecision{Outcome: Deny, Reason: "approval required but no approval channel"}
		}
		return (interactiveAuthorizer{r: r}).Authorize(ctx, in)
	}
	return AuthzDecision{Outcome: base}
}

// grantAuthorizer authorizes a call ahead of any prompt: a pre-authorized Trust,
// or a durable "always" grant already recorded for the workspace (in which case
// it records a session grant so the rest of the turn skips the gate too, exactly
// as the pre-pipeline slow-path did). Otherwise it abstains.
type grantAuthorizer struct{ r *Runner }

func (g grantAuthorizer) Authorize(ctx context.Context, in AuthzInput) AuthzDecision {
	r := g.r
	if in.Trust == tools.TrustPreAuthorized {
		return AuthzDecision{Outcome: Allow, Reason: "pre-authorized"}
	}
	if r.grantStore == nil {
		return AuthzDecision{Outcome: Abstain}
	}
	granted, err := r.grantStore.IsGranted(ctx, in.Addr.WorkspaceID, in.Call.Name)
	if err != nil {
		slog.WarnContext(ctx, "durable grant lookup failed", slog.String("tool", in.Call.Name), slog.Any("err", err))
		return AuthzDecision{Outcome: Abstain}
	}
	if !granted {
		return AuthzDecision{Outcome: Abstain}
	}
	if r.approvalGranter != nil {
		r.approvalGranter.GrantSession(in.Addr.WorkspaceID, in.Call.Name)
	}
	return AuthzDecision{Outcome: Allow, Reason: "durable grant"}
}

// interactiveAuthorizer resolves a Prompt via the human-in-the-loop flow: it
// emits an approval_request (pending), blocks on the broker under
// ApprovalTimeout, records the session/always grant on approval, and emits the
// resolved status (approved/denied/expired). It reproduces the tail of the
// pre-pipeline invokeWithApproval verbatim. On turn cancellation it returns Deny
// WITHOUT emitting a resolved part (ctx.Err() is set; the caller aborts).
type interactiveAuthorizer struct{ r *Runner }

func (ia interactiveAuthorizer) Authorize(ctx context.Context, in AuthzInput) AuthzDecision {
	r := ia.r
	call := in.Call
	addr := in.Addr
	reqID := call.CallID
	emitter, _ := tools.PartEmitterFrom(ctx)
	scopes := r.approvalScopes
	if len(scopes) == 0 {
		scopes = []string{"once", "session", "always"}
	}
	r.emitApproval(ctx, emitter, reqID, call, scopes, spec.ApprovalPending, "tool is not pre-authorized")
	slog.InfoContext(ctx, "tool approval requested; awaiting user decision",
		slog.String("tool", call.Name),
		slog.String("call_id", reqID),
		slog.String("task", addr.TaskID),
		slog.String("workspace", addr.WorkspaceID),
	)

	awaitCtx := ctx
	if r.approvalTimeout > 0 {
		var cancel context.CancelFunc
		awaitCtx, cancel = context.WithTimeout(ctx, r.approvalTimeout)
		defer cancel()
	}
	decision, awaitErr := r.approvals.Await(awaitCtx, addr.TaskID, reqID)
	if ctx.Err() != nil {
		// The turn itself was cancelled (not just the approval timeout) — abort
		// without emitting a resolved part; the caller returns ctx.Err().
		return AuthzDecision{Outcome: Deny, Reason: "turn cancelled"}
	}
	if awaitErr != nil || !decision.Granted {
		status := spec.ApprovalDenied
		if awaitErr != nil {
			status = spec.ApprovalExpired // timed out waiting for a response
		}
		r.emitApproval(ctx, emitter, reqID, call, scopes, status, "")
		slog.InfoContext(ctx, "tool approval not granted",
			slog.String("tool", call.Name),
			slog.String("call_id", reqID),
			slog.String("status", string(status)),
		)
		return AuthzDecision{Outcome: Deny, Reason: string(status)}
	}

	// Granted: record a grant so future calls in scope don't re-prompt.
	if r.approvalGranter != nil {
		switch decision.Scope {
		case "session":
			r.approvalGranter.GrantSession(addr.WorkspaceID, call.Name)
		case "always":
			if gerr := r.approvalGranter.GrantAlways(ctx, addr.WorkspaceID, call.Name); gerr != nil {
				slog.WarnContext(ctx, "persist always-grant failed", slog.String("tool", call.Name), slog.Any("err", gerr))
			}
		}
	}
	r.emitApproval(ctx, emitter, reqID, call, scopes, spec.ApprovalApproved, "")
	slog.InfoContext(ctx, "tool approval granted",
		slog.String("tool", call.Name),
		slog.String("call_id", reqID),
		slog.String("scope", decision.Scope),
	)
	return AuthzDecision{Outcome: Allow, Scope: decision.Scope}
}
