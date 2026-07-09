package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
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

// invokeTool dispatches a tool call: a provider-surfaced tool (one a ToolProvider
// discovered this turn, not in the static registry) routes to that provider;
// everything else goes through the static registry's approval-aware path. The
// hooks approval gate (approveToolCall) has already run for both in the loop; the
// registry's requires-approval flow applies only to static tools.
func (r *Runner) invokeTool(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, call protocol.ToolInvokeEnvelope, tt turnTools) (tools.Result, error) {
	trust := tt.trustByTool[call.Name]
	if p, ok := tt.providerByTool[call.Name]; ok {
		return r.invokeToolProvider(ctx, p, addr, call, trust)
	}
	return r.invokeWithApproval(ctx, session, addr, call, trust)
}

// invokeToolProvider invokes a provider-surfaced tool via its ToolProvider,
// forwarding the per-turn OBO authority + enclosing message id (on ctx and on
// the request) so the remote side can resolve the acting user. The call is
// wrapped in a StartTool span for uniform provider telemetry. It first runs the
// uniform authorization pipeline (grant + safety, plus the interactive prompt
// only when the outcome is a Prompt): with the default no-op safety and a
// TrustDefault tool this resolves to Allow and the provider is invoked directly
// (today's non-prompting bypass), while a stronger Trust or a non-trivial safety
// authorizer can Deny or force a prompt.
func (r *Runner) invokeToolProvider(ctx context.Context, p ToolProvider, addr protocol.MessageAddress, call protocol.ToolInvokeEnvelope, trust tools.TrustLevel) (result tools.Result, err error) {
	in := AuthzInput{Call: call, Addr: addr, Trust: trust, ProviderID: p.ID()}
	if d := r.authorizeTool(ctx, in, trustBase(trust)); d.Outcome != Allow {
		if ctx.Err() != nil {
			return tools.Result{}, ctx.Err() // turn cancelled during a prompt
		}
		return tools.Result{}, policyErrorFor(call.Name) // denied/expired → fed back to the model
	}
	req := tools.RequestFromEnvelope(call)
	if auth, ok := tools.MemoryAuthorityFrom(ctx); ok {
		req.Authority = auth
	}
	if id, ok := tools.MessageIDFrom(ctx); ok {
		req.MessageID = id
	}
	ctx, span := telemetry.StartTool(ctx, call.Name)
	defer func() { telemetry.FinishErr(span, err) }()
	return p.Invoke(ctx, req)
}

// invokeWithApproval invokes a local (static-registry) tool; the registry policy
// runs inside session.InvokeTool and returns ErrToolRequiresApproval BEFORE the
// tool body executes when the tool is gated. On that trigger — and with an
// approval channel wired — it runs the uniform authorization pipeline seeded with
// a Prompt: the grant authorizer may short-circuit to Allow (a durable/always
// grant, recording a session grant), the safety authorizer may Deny or escalate,
// and the interactive authorizer resolves the remaining Prompt (emitting the
// approval_request, blocking under ApprovalTimeout, recording session/always
// grants). On Allow it re-invokes bypassing the gate; on Deny/expire it returns
// the original requires-approval error so the caller feeds it back to the model.
// With no approval channel wired it behaves exactly as a plain invoke.
func (r *Runner) invokeWithApproval(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, call protocol.ToolInvokeEnvelope, trust tools.TrustLevel) (tools.Result, error) {
	result, err := session.InvokeTool(ctx, call)
	if err == nil || r.approvals == nil || !errors.Is(err, tools.ErrToolRequiresApproval) {
		return result, err
	}
	in := AuthzInput{Call: call, Addr: addr, Trust: trust}
	if d := r.authorizeTool(ctx, in, Prompt); d.Outcome != Allow {
		if ctx.Err() != nil {
			return tools.Result{}, ctx.Err() // turn cancelled during the prompt
		}
		return tools.Result{}, err // denied/expired → original requires-approval error, fed back to the model
	}
	return session.InvokeToolApproved(ctx, call)
}

// policyErrorFor is the requires-approval error surfaced when the authorization
// pipeline refuses a provider tool (a local tool reuses the policy's original
// error instead). Wrapping tools.ErrToolRequiresApproval keeps the caller's
// error-feedback path uniform.
func policyErrorFor(name string) error {
	return fmt.Errorf("%w: %s", tools.ErrToolRequiresApproval, name)
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
