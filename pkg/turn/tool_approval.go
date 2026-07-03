package turn

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// approveToolCall consults the per-turn approvers (e.g. command allowed-tools)
// first, then the runner's configured approvers. First denial wins.
func (r *Runner) approveToolCall(ctx context.Context, call hooks.ToolCall, perTurn []hooks.ToolApprover) hooks.Decision {
	if d := hooks.Approve(ctx, call, perTurn); !d.Allow {
		return d
	}
	return hooks.Approve(ctx, call, r.approvers)
}

func (r *Runner) notifyToolStarted(ctx context.Context, call hooks.ToolCall) {
	for _, o := range r.observers {
		if o != nil {
			o.ToolStarted(ctx, call)
		}
	}
}

func (r *Runner) notifyToolFinished(ctx context.Context, call hooks.ToolCall, isError bool, err error) {
	for _, o := range r.observers {
		if o != nil {
			o.ToolFinished(ctx, call, isError, err)
		}
	}
}

// toolErrorOutput is the tool_result output payload recorded when a tool call is
// denied by an approver or fails to invoke (e.g. registry policy denial,
// execution error) — the error is handed back to the model, not raised.
func toolErrorOutput(reason string) json.RawMessage {
	out, err := json.Marshal(map[string]string{"error": reason})
	if err != nil {
		return json.RawMessage(`{"error":"tool failed"}`)
	}
	return out
}

func isContextOverflow(err error) bool {
	var pe *provider.ProviderError
	return errors.As(err, &pe) && pe.Kind == provider.FailureContextOverflow
}

func toolCallsFromMessage(msg protocol.ChatMessage) ([]protocol.ToolInvokeEnvelope, error) {
	calls := make([]protocol.ToolInvokeEnvelope, 0)
	for _, part := range msg.Content {
		if call, ok := protocol.ToolCallFromPart(part); ok {
			calls = append(calls, call)
		}
	}
	return calls, nil
}

// invokeTool dispatches a tool call: a dynamically-discovered tool (one surfaced
// by the DynamicToolProvider this turn, not in the static registry) routes to the
// provider; everything else goes through the static registry's approval-aware
// path. The hooks approval gate (approveToolCall) has already run for both in the
// loop; the registry's requires-approval flow applies only to static tools.
func (r *Runner) invokeTool(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, call protocol.ToolInvokeEnvelope, tt turnTools) (tools.Result, error) {
	if _, dynamic := tt.dynamicNames[call.Name]; dynamic {
		return r.invokeDynamic(ctx, call)
	}
	return r.invokeWithApproval(ctx, session, addr, call)
}

// invokeDynamic invokes a discovered tool via the DynamicToolProvider, forwarding
// the per-turn OBO authority (on ctx and on the request) so the remote side can
// resolve the acting user.
func (r *Runner) invokeDynamic(ctx context.Context, call protocol.ToolInvokeEnvelope) (tools.Result, error) {
	req := tools.RequestFromEnvelope(call)
	if auth, ok := tools.MemoryAuthorityFrom(ctx); ok {
		req.Authority = auth
	}
	if id, ok := tools.MessageIDFrom(ctx); ok {
		req.MessageID = id
	}
	return r.dynamicTools.Invoke(ctx, req)
}

// invokeWithApproval invokes a tool; if the policy gates it as "requires
// approval" and an approval channel is wired, it emits an approval_request part,
// blocks for the user's approve/deny control (bounded by ApprovalTimeout), and
// on approval re-invokes (bypassing the gate) plus records any session/always
// grant. On deny/expire it returns the original requires-approval error so the
// caller's error-feedback path records it for the model. With no approval
// channel wired it behaves exactly as a plain invoke.
func (r *Runner) invokeWithApproval(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, call protocol.ToolInvokeEnvelope) (tools.Result, error) {
	result, err := session.InvokeTool(ctx, call)
	if err == nil || r.approvals == nil || !errors.Is(err, tools.ErrToolRequiresApproval) {
		return result, err
	}

	// Durable "always" grants: before prompting, consult the durable grant store
	// (when wired). A prior "always" grant — persisted under the user's OBO —
	// short-circuits the prompt: record a session grant so subsequent calls in
	// this turn skip the gate too, then run the tool approved. A store error is
	// non-fatal: log and fall through to the normal prompt flow.
	if r.grantStore != nil {
		granted, gerr := r.grantStore.IsGranted(ctx, addr.WorkspaceID, call.Name)
		if gerr != nil {
			slog.WarnContext(ctx, "durable grant lookup failed", slog.String("tool", call.Name), slog.Any("err", gerr))
		} else if granted {
			if r.approvalGranter != nil {
				r.approvalGranter.GrantSession(addr.WorkspaceID, call.Name)
			}
			return session.InvokeToolApproved(ctx, call)
		}
	}

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
		// The turn itself was cancelled (not just the approval timeout) — abort.
		return tools.Result{}, ctx.Err()
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
		return tools.Result{}, err // original requires-approval error → fed back to the model
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
	return session.InvokeToolApproved(ctx, call)
}

// emitApproval upserts an approval_request part (pending first, then the
// resolved status) on the current message stream so the user can answer and the
// resolved prompt persists. No-op without an emitter.
func (r *Runner) emitApproval(ctx context.Context, emitter tools.PartEmitter, reqID string, call protocol.ToolInvokeEnvelope, scopes []string, status spec.ApprovalStatus, reason string) {
	if emitter == nil {
		return
	}
	part := spec.NewApprovalRequestPart(spec.ApprovalRequestPart{
		ID:      reqID,
		Tool:    call.Name,
		Summary: "Use the " + call.Name + " tool",
		Args:    protocol.ArgsToRaw(call.Args),
		Options: scopes,
		Status:  status,
		Reason:  reason,
	})
	if err := emitter.UpsertPart(ctx, part); err != nil {
		slog.WarnContext(ctx, "emit approval_request failed", slog.Any("err", err))
	}
}
