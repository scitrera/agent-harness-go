package turn

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
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
	maxToolIterations := toolIterationLimit(ctx, r.maxToolIterations)
	toolIterations := 0
	for {
		response, usedModel, err := r.callWithRecovery(ctx, addr, user, required, session.History(), bootstrap, injected, model, streamer, tt)
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

		// Stream the tool_call parts so the UI surfaces tool activity in real time.
		for _, part := range assistant.Content {
			if part.Type() == protocol.ContentToolCall {
				if _, err := streamer.appendPart(ctx, part); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool call: %w", err)
				}
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
				part, err := protocol.NewToolResultPart(call.CallID, call.Name, errorOutput, true)
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

			_, dynamicCall := tt.dynamicNames[call.Name]
			toolStart := time.Now()
			toolCtx, toolSpan := telemetry.StartTool(ctx, call.Name)
			r.publishToolEvent(toolCtx, applyHookDecision(toolEventFromCall(tools.ToolEventStarted, call), decision))
			r.notifyToolStarted(toolCtx, hc)
			result, err := r.invokeTool(toolCtx, session, addr, call, tt)
			r.notifyToolFinished(toolCtx, hc, err != nil, err)
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

// containsImage reports whether any part is an image content part.
func containsImage(parts []protocol.ContentPart) bool {
	for _, p := range parts {
		if p.Type() == protocol.ContentImage {
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
