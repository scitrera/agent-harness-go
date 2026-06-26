package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	defaultMaxToolIterations = 4
	// maxOverflowRetries bounds reactive context-overflow recovery: on a
	// classified context_overflow error, the loop drops the oldest history and
	// rebuilds, up to this many times before surfacing the error.
	maxOverflowRetries = 2
)

func (r *Runner) runProviderLoop(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, bootstrap []bootstrap.File, streamer *turnStreamer, injected []protocol.ChatMessage, model string, perTurnApprovers []hooks.ToolApprover, tt turnTools) (protocol.ChatMessage, error) {
	toolIterations := 0
	for {
		response, err := r.callWithOverflowRecovery(ctx, session.History(), bootstrap, injected, model, streamer, tt)
		if err != nil {
			return protocol.ChatMessage{}, err
		}
		assistant := response.Message
		if assistant.Addr.ThreadID == "" {
			assistant.Addr = addr
		}
		if err := session.Append(ctx, assistant); err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("append assistant message: %w", err)
		}
		calls, err := toolCallsFromMessage(assistant)
		if err != nil {
			return protocol.ChatMessage{}, err
		}
		if len(calls) == 0 {
			return assistant, nil
		}
		if toolIterations >= r.maxToolIterations {
			return protocol.ChatMessage{}, ErrToolLoopLimit
		}
		toolIterations++

		// Stream the tool_call parts so the UI surfaces tool activity in real time.
		for _, part := range assistant.Content {
			if part.Type() == protocol.ContentToolCall {
				if _, err := streamer.appendPart(ctx, part); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool call: %w", err)
				}
			}
		}
		for _, call := range calls {
			if call.Addr.ThreadID == "" {
				call.Addr = addr
			}
			hc := hooks.ToolCall{CallID: call.CallID, Name: call.Name, Args: protocol.ArgsToRaw(call.Args), Addr: call.Addr}

			// Gate: per-turn approvers (e.g. command allowed-tools) then the
			// runner's configured approvers. A denial records a tool_result error
			// so the model can adapt, without executing the tool.
			if d := r.approveToolCall(ctx, hc, perTurnApprovers); !d.Allow {
				part, err := protocol.NewToolResultPart(call.CallID, call.Name, toolErrorOutput(d.Reason), true)
				if err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("denied tool result part: %w", err)
				}
				if err := session.AppendToolResult(ctx, call.CallID, part); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("record tool denial: %w", err)
				}
				if _, err := streamer.appendPart(ctx, part); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool denial: %w", err)
				}
				continue
			}

			toolCtx, toolSpan := telemetry.StartTool(ctx, call.Name)
			r.notifyToolStarted(toolCtx, hc)
			result, err := r.invokeTool(toolCtx, session, addr, call, tt)
			r.notifyToolFinished(toolCtx, hc, err != nil, err)
			telemetry.FinishErr(toolSpan, err)
			if err != nil {
				// A cancelled context aborts the turn; any other tool error (registry
				// policy denial, execution failure, …) is recorded as a tool_result
				// error and handed back to the model so it can adapt and respond.
				// Aborting the turn here would end it with no assistant reply, which
				// reads to the UI as a hang.
				if ctx.Err() != nil {
					return protocol.ChatMessage{}, fmt.Errorf("invoke tool %s: %w", call.Name, err)
				}
				slog.WarnContext(ctx, "tool call failed; returning error to model",
					slog.String("tool", call.Name), slog.Any("err", err))
				errPart, perr := protocol.NewToolResultPart(call.CallID, call.Name, toolErrorOutput(err.Error()), true)
				if perr != nil {
					return protocol.ChatMessage{}, fmt.Errorf("tool error result part: %w", perr)
				}
				if perr := session.AppendToolResult(ctx, call.CallID, errPart); perr != nil {
					return protocol.ChatMessage{}, fmt.Errorf("record tool error: %w", perr)
				}
				if _, perr := streamer.appendPart(ctx, errPart); perr != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool error: %w", perr)
				}
				continue
			}
			part, err := result.ContentPart()
			if err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("tool result part: %w", err)
			}
			if _, err := streamer.appendPart(ctx, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("stream tool result: %w", err)
			}
		}
	}
}

// callWithOverflowRecovery builds the context and invokes the provider. On a
// classified context_overflow error it drops the oldest history and retries
// (the request was rejected before any tokens streamed, so no partial output is
// duplicated), up to maxOverflowRetries.
func (r *Runner) callWithOverflowRecovery(ctx context.Context, history []protocol.ChatMessage, bootstrap []bootstrap.File, injected []protocol.ChatMessage, model string, streamer *turnStreamer, tt turnTools) (provider.ChatResponse, error) {
	for attempt := 0; ; attempt++ {
		contextMessages, err := r.ctxMgr.Build(ctx, bootstrap, trimOldest(history, attempt))
		if err != nil {
			return provider.ChatResponse{}, fmt.Errorf("assemble context: %w", err)
		}
		req := provider.ChatRequest{Model: model, Messages: mergeInjected(contextMessages, injected), Tools: tt.specs}
		response, err := r.invokeProvider(ctx, req, streamer)
		if err == nil {
			return response, nil
		}
		if isContextOverflow(err) && attempt < maxOverflowRetries && len(history) > 1 {
			slog.WarnContext(ctx, "context overflow; compacting and retrying", slog.Int("attempt", attempt+1))
			continue
		}
		return provider.ChatResponse{}, fmt.Errorf("provider chat: %w", err)
	}
}

// invokeProvider issues one provider call, streaming tokens via the streamer
// when the provider supports it and streaming is enabled.
func (r *Runner) invokeProvider(ctx context.Context, req provider.ChatRequest, streamer *turnStreamer) (resp provider.ChatResponse, err error) {
	ctx, span := telemetry.StartLLM(ctx, req.Model)
	defer telemetry.Finish(span, &err)
	// Log every provider call + its outcome. The LLM request is otherwise opaque
	// in the harness logs, so a failed call (network/auth/timeout) leaves no
	// trace beyond the task fail reason — which is exactly when we most need to
	// know the model, transport, and error.
	slog.InfoContext(ctx, "llm: provider call",
		slog.String("model", req.Model),
		slog.Int("messages", len(req.Messages)),
		slog.Int("tools", len(req.Tools)),
	)
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "llm: provider call failed",
				slog.String("model", req.Model),
				slog.Any("err", err),
			)
		}
	}()
	if sp, ok := r.provider.(StreamingProvider); ok && r.streaming {
		textIndex := -1
		onDelta := func(text string) error {
			if textIndex < 0 {
				idx, derr := streamer.appendTextStream(ctx)
				if derr != nil {
					return derr
				}
				textIndex = idx
			}
			return streamer.tokenDelta(ctx, textIndex, text)
		}
		resp, err = sp.ChatStream(ctx, req, onDelta)
		return resp, err
	}
	resp, err = r.provider.Chat(ctx, req)
	return resp, err
}

// mergeInjected inserts injected context (daily notes, recalled memories) right
// after the system prompt.
func mergeInjected(contextMessages, injected []protocol.ChatMessage) []protocol.ChatMessage {
	if len(injected) == 0 || len(contextMessages) == 0 {
		return contextMessages
	}
	merged := make([]protocol.ChatMessage, 0, len(contextMessages)+len(injected))
	merged = append(merged, contextMessages[0])
	merged = append(merged, injected...)
	merged = append(merged, contextMessages[1:]...)
	return merged
}

// trimOldest drops the oldest fraction of history for overflow retry attempt N
// (attempt 0 = no trim). The most recent message is always retained; pairing is
// repaired by the provider's transcript sanitizer.
func trimOldest(history []protocol.ChatMessage, attempt int) []protocol.ChatMessage {
	if attempt <= 0 || len(history) <= 1 {
		return history
	}
	drop := len(history) * attempt / (maxOverflowRetries + 1)
	if drop >= len(history) {
		drop = len(history) - 1
	}
	return history[drop:]
}

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
