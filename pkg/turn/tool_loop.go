package turn

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const defaultMaxToolIterations = 4

type toolIterationLimitKey struct{}

func withToolIterationLimit(ctx context.Context, limit int) context.Context {
	if limit <= 0 {
		return ctx
	}
	return context.WithValue(ctx, toolIterationLimitKey{}, limit)
}

func toolIterationLimit(ctx context.Context, fallback int) int {
	limit, ok := ctx.Value(toolIterationLimitKey{}).(int)
	if !ok || limit <= 0 {
		return fallback
	}
	return limit
}

func (r *Runner) runProviderLoop(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, user protocol.ChatMessage, bootstrap []bootstrap.File, streamer *turnStreamer, injected []protocol.ChatMessage, model string, perTurnApprovers []hooks.ToolApprover, tt turnTools) (protocol.ChatMessage, error) {
	required := requiredCapabilities(user)
	// One-time pre-turn vision escalation: the loaded history (or injected messages)
	// may ALREADY carry images — e.g. a persistent sub-agent thread that inspected an
	// image on a prior turn, or a continued/reloaded thread — while the current user
	// message is text-only. requiredCapabilities only inspects the user message, and
	// the reactive escalation in the loop only fires on a NEW inject_image, so without
	// this the first provider call would ship those history-carried images to a
	// non-vision model (400 "does not support image inputs"). Checked once here; images
	// that arrive DURING the turn are handled by the in-loop escalation below.
	if !required.Vision && (messagesHaveImage(session.History()) || messagesHaveImage(injected)) {
		required.Vision = true
		model = r.escalateModel(ctx, addr, user, required, model)
	}
	maxToolIterations := toolIterationLimit(ctx, r.maxToolIterations)
	toolIterations := 0
	for {
		// A tool earlier THIS turn may have pinned a model — load_skill honoring a
		// skill's preferred_model, or /model. Apply it to the rest of this turn's
		// provider calls: the turn's model was resolved once up front (before any
		// tool ran), and in a per-turn harness the "effective next turn" in-memory
		// pin is gone by the next turn, so the pin would otherwise never take effect.
		// Only adopt it when it can satisfy the current required capabilities so a
		// vision escalation (below / pre-turn) isn't undone by a text-only pin.
		if r.modelRegistry != nil {
			if sticky := r.stickyModel(addr.ThreadID); sticky != "" && sticky != model {
				if m, ok := r.modelRegistry.Get(sticky); ok && m.Capabilities.Satisfies(required) {
					slog.InfoContext(ctx, "turn: adopting pinned model mid-turn",
						slog.String("model", sticky), slog.String("was", model))
					model = sticky
				}
			}
		}
		// Turn-lifecycle observers see each model-call boundary (and any compaction
		// that ran while building this call's context). callWithRecovery may compact
		// inside its Build; a counter delta detects it without threading the pointer.
		beforeComp := compaction.CompactionCount(ctx)
		r.notifyTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseModelCallStarted, Addr: addr, Iteration: toolIterations, Model: model})
		response, usedModel, err := r.callWithRecovery(ctx, addr, user, required, session.History(), bootstrap, injected, model, streamer, tt)
		r.notifyTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseModelCallFinished, Addr: addr, Iteration: toolIterations, Model: usedModel, Err: err})
		if compaction.CompactionCount(ctx) > beforeComp {
			r.notifyTurn(ctx, hooks.TurnEvent{Phase: hooks.PhaseCompacted, Addr: addr, Iteration: toolIterations, Model: usedModel})
		}
		if err != nil {
			return protocol.ChatMessage{}, err
		}
		// Stick with the (possibly escalated) model for the rest of the turn:
		// reverting after a fallback would likely re-hit the original failure.
		model = usedModel
		assistant := response.Message
		if assistant.Addr.ThreadID == "" {
			assistant.Addr = addr
		}
		// Stamp the per-turn world-state (turn counter + invoked skills) onto the
		// assistant message so it persists in history and survives compaction. The
		// final assistant of the turn carries the complete skill set (tools, incl.
		// load_skill, ran before it).
		stampTurnWorldState(ctx, &assistant)
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
		if toolIterations >= maxToolIterations {
			return protocol.ChatMessage{}, ErrToolLoopLimit
		}
		toolIterations++

		// Carry the spawning assistant message id on ctx so tools that create
		// cross-thread back-refs (spawn_subagent) can record the parent MESSAGE id
		// (Request.MessageID); the tool CallID alone doesn't identify it.
		ctx = tools.WithMessageID(ctx, assistant.ID)

		// Surface this iteration's assistant content on the stream in content
		// order. Non-tool-call parts (preamble text, reasoning) that weren't
		// already streamed live are appended here so they render inline at their
		// real position instead of being dropped (non-streaming providers emit no
		// token deltas, so their preamble text would otherwise never reach the UI
		// and the turn's only text would collapse to the finalize tail). On the
		// streaming path these are already in the reconstruction, so appending is
		// a no-op. The tool_call parts themselves stream per-call below — right
		// before their approval + result — so an approval_request renders adjacent
		// to its own call instead of after the whole batch.
		for _, part := range assistant.Content {
			if part.Type() == protocol.ContentToolCall {
				continue
			}
			if _, err := streamer.appendUnstreamed(ctx, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("stream assistant part: %w", err)
			}
		}
		// Index the iteration's tool_call parts by call id so each can be streamed
		// immediately before its approval/execution (interleaved), keeping the
		// approval_request adjacent to the call it gates.
		toolCallParts := make(map[string]protocol.ContentPart, len(calls))
		for _, part := range assistant.Content {
			if part.Type() != protocol.ContentToolCall {
				continue
			}
			if env, ok := protocol.ToolCallFromPart(part); ok {
				toolCallParts[env.CallID] = part
			}
		}
		// Parts a tool asks to inject into the model's OWN context as a NEW
		// user-role message (inject_image's QC image). Collected across this
		// iteration's calls in order and appended AFTER every tool_call is answered
		// (tool results first, then the user message — the ordering the provider
		// requires), so the next iteration sees the image in context.
		var injectUser []protocol.ContentPart
		for _, call := range calls {
			if call.Addr.ThreadID == "" {
				call.Addr = addr
			}
			// Stream this call's tool_call part now (just before its approval +
			// execution) so it renders immediately above its approval_request /
			// result rather than being batched ahead of every approval.
			if tcPart, ok := toolCallParts[call.CallID]; ok {
				if _, err := streamer.appendUnstreamed(ctx, tcPart); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool call: %w", err)
				}
			}
			hc := hooks.ToolCall{CallID: call.CallID, Name: call.Name, Args: protocol.ArgsToRaw(call.Args), Addr: call.Addr}
			r.publishToolEvent(ctx, toolEventFromCall(tools.ToolEventQueued, call))

			// Gate: per-turn approvers (e.g. command allowed-tools) then the
			// runner's configured approvers. A denial records a tool_result error
			// so the model can adapt, without executing the tool.
			decision := r.approveToolCall(ctx, hc, perTurnApprovers)
			if !decision.Allow {
				errorOutput := toolErrorOutput(decision.Reason)
				finished := applyHookDecision(toolEventFromCall(tools.ToolEventFinished, call), decision)
				finished.Result.PayloadBytes = len(errorOutput)
				r.publishToolEvent(ctx, finished)
				if err := r.recordToolError(ctx, session, streamer, call.CallID, call.Name, errorOutput); err != nil {
					return protocol.ChatMessage{}, err
				}
				continue
			}

			_, dynamicCall := tt.providerByTool[call.Name]
			toolStart := time.Now()
			toolCtx, toolSpan := telemetry.StartTool(ctx, call.Name, protocol.ArgsToRaw(call.Args))
			r.publishToolEvent(toolCtx, applyHookDecision(toolEventFromCall(tools.ToolEventStarted, call), decision))
			r.notifyToolStarted(toolCtx, hc)
			result, err := r.invokeTool(toolCtx, session, addr, call, tt)
			r.notifyToolFinished(toolCtx, hc, err != nil, err)
			telemetry.AnnotateToolResult(toolSpan, result.Payload, result.IsError || err != nil)
			telemetry.FinishErr(toolSpan, err)
			r.publishToolEvent(toolCtx, finishToolEvent(call, toolStart, result, err))
			// Canonical per-call log covering every tool — local/static, MCP,
			// memory, subagent, and dynamic (bridge). The Go-error path below
			// adds its own warn with the failure detail.
			if err == nil {
				slog.InfoContext(toolCtx, "tool call",
					slog.String("tool", call.Name),
					slog.Bool("dynamic", dynamicCall),
					slog.Bool("is_error", result.IsError),
					slog.Duration("elapsed", time.Since(toolStart)),
				)
				// Record files the tool wrote/edited into the durable, aged
				// world-state (RecentFiles), so the model recalls what it produced
				// even after the tool result is compacted out of context.
				recordToolFiles(toolCtx, result)
				// At DEBUG, also surface the (truncated) arguments — e.g. which
				// file a read_file/edit_file touched, or the command a shell ran.
				// Kept off the INFO line so normal logs don't carry arg payloads.
				slog.DebugContext(toolCtx, "tool call args",
					slog.String("tool", call.Name),
					slog.String("args", truncForLog(string(protocol.ArgsToRaw(call.Args)), 512)),
				)
			}
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
				if perr := r.recordToolError(ctx, session, streamer, call.CallID, call.Name, toolErrorOutput(err.Error())); perr != nil {
					return protocol.ChatMessage{}, perr
				}
				continue
			}
			part, err := result.ContentPart()
			if err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("tool result part: %w", err)
			}
			// Persist the result to history (single append site for every tool —
			// static, dynamic, denied, errored — so it can't diverge per path).
			// Without this the model never sees the result and re-calls the tool
			// in a loop. When the tool returned extra parts (e.g. a subagent
			// reference part), co-locate them ON THE SAME tool-result message as the
			// tool_result part so the provider message stays non-empty.
			if len(result.Parts) > 0 {
				parts := append([]protocol.ContentPart{part}, result.Parts...)
				if err := session.AppendToolResultParts(ctx, call.CallID, parts...); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("record tool result parts: %w", err)
				}
			} else if err := session.AppendToolResult(ctx, call.CallID, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("record tool result: %w", err)
			}
			if _, err := streamer.appendPart(ctx, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("stream tool result: %w", err)
			}
			// Stream the co-located extra parts too (e.g. spawn_subagent's
			// SubagentPart carrying the child thread_id). They were persisted to the
			// tool-result message above, but the tool-result message is NOT what the
			// end-of-turn memory commit writes — that commits the finalized assistant,
			// reconstructed from the STREAM. Without streaming them, the subagent
			// linkage never reaches MemoryLayer (parent→child would survive only via
			// the "::sub::" thread-name convention). Streaming folds them into the
			// finalized assistant so the committed turn carries the reference.
			for _, extra := range result.Parts {
				if _, err := streamer.appendPart(ctx, extra); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool result extra part: %w", err)
				}
			}
			// A tool (inject_image) may ask to place parts into the model's OWN
			// context as a user message. Collect them in call order; append after
			// every tool_call is answered (below), never on the tool_result message
			// (a tool-role message is text-only, so an image on it would be dropped).
			if len(result.InjectUserParts) > 0 {
				injectUser = append(injectUser, result.InjectUserParts...)
			}
		}
		// After every tool_call is answered, inject the collected parts as ONE new
		// user-role message so the next provider iteration sees them in context.
		if len(injectUser) > 0 {
			if err := session.Append(ctx, protocol.ChatMessage{Role: protocol.RoleUser, Addr: addr, Content: injectUser}); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("append injected user message: %w", err)
			}
			// When an image entered context and this turn isn't already routed to a
			// vision model, flip the required capability and escalate for the next
			// iteration so the model can actually see it.
			if !required.Vision && containsImage(injectUser) {
				required.Vision = true
				model = r.escalateModel(ctx, addr, user, required, model)
			}
		}
	}
}

// recordToolError builds an error tool_result part, persists it to history, and
// streams it — the shared denied/errored path. Both hand the model an error
// tool_result (rather than aborting the turn) so it can adapt and respond.
func (r *Runner) recordToolError(ctx context.Context, session *harness.Session, streamer *turnStreamer, callID, name string, output json.RawMessage) error {
	part, err := protocol.NewToolResultPart(callID, name, output, true)
	if err != nil {
		return fmt.Errorf("tool error result part: %w", err)
	}
	if err := session.AppendToolResult(ctx, callID, part); err != nil {
		return fmt.Errorf("record tool error: %w", err)
	}
	if _, err := streamer.appendPart(ctx, part); err != nil {
		return fmt.Errorf("stream tool error: %w", err)
	}
	return nil
}

// containsImage reports whether any part is an image content part.
func containsImage(parts []protocol.ContentPart) bool {
	for _, p := range parts {
		if p.Type() == protocol.ContentImage {
			return true
		}
	}
	return false
}

// messagesHaveImage reports whether any message carries an image content part —
// used for the one-time pre-turn vision check (history/injected images that a
// text-only user message wouldn't reveal).
func messagesHaveImage(msgs []protocol.ChatMessage) bool {
	for _, m := range msgs {
		if containsImage(m.Content) {
			return true
		}
	}
	return false
}

// truncForLog returns s clamped to at most n bytes (rune-safe-ish, byte-capped),
// with an ellipsis marker when clamped. Used to keep arg payloads out of the way
// in DEBUG logs without dumping large tool inputs (edit_file bodies, etc.).
func truncForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(truncated)"
}
