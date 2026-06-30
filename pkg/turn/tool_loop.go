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
			// in a loop.
			if err := session.AppendToolResult(ctx, call.CallID, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("record tool result: %w", err)
			}
			if _, err := streamer.appendPart(ctx, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("stream tool result: %w", err)
			}
		}
	}
}
