package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
)

const (
	defaultMaxToolIterations = 4
	// maxOverflowRetries bounds reactive context-overflow recovery: on a
	// classified context_overflow error, the loop drops the oldest history and
	// rebuilds, up to this many times before surfacing the error.
	maxOverflowRetries = 2
)

func (r *Runner) runProviderLoop(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, bootstrap []bootstrap.File, streamer *turnStreamer, injected []protocol.ChatMessage, model string, perTurnApprovers []hooks.ToolApprover) (protocol.ChatMessage, error) {
	toolIterations := 0
	for {
		response, err := r.callWithOverflowRecovery(ctx, session.History(), bootstrap, injected, model, streamer)
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
				if err := streamer.appendPart(ctx, part); err != nil {
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
				part, err := protocol.NewToolResultPart(call.CallID, call.Name, denialOutput(d.Reason), true)
				if err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("denied tool result part: %w", err)
				}
				if err := session.AppendToolResult(ctx, call.CallID, part); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("record tool denial: %w", err)
				}
				if err := streamer.appendPart(ctx, part); err != nil {
					return protocol.ChatMessage{}, fmt.Errorf("stream tool denial: %w", err)
				}
				continue
			}

			toolCtx, toolSpan := telemetry.StartTool(ctx, call.Name)
			r.notifyToolStarted(toolCtx, hc)
			result, err := session.InvokeTool(toolCtx, call)
			r.notifyToolFinished(toolCtx, hc, err != nil, err)
			telemetry.FinishErr(toolSpan, err)
			if err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("invoke tool %s: %w", call.Name, err)
			}
			part, err := result.ContentPart()
			if err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("tool result part: %w", err)
			}
			if err := streamer.appendPart(ctx, part); err != nil {
				return protocol.ChatMessage{}, fmt.Errorf("stream tool result: %w", err)
			}
		}
	}
}

// callWithOverflowRecovery builds the context and invokes the provider. On a
// classified context_overflow error it drops the oldest history and retries
// (the request was rejected before any tokens streamed, so no partial output is
// duplicated), up to maxOverflowRetries.
func (r *Runner) callWithOverflowRecovery(ctx context.Context, history []protocol.ChatMessage, bootstrap []bootstrap.File, injected []protocol.ChatMessage, model string, streamer *turnStreamer) (provider.ChatResponse, error) {
	for attempt := 0; ; attempt++ {
		contextMessages, err := r.ctxMgr.Build(ctx, bootstrap, trimOldest(history, attempt))
		if err != nil {
			return provider.ChatResponse{}, fmt.Errorf("assemble context: %w", err)
		}
		req := provider.ChatRequest{Model: model, Messages: mergeInjected(contextMessages, injected), Tools: r.toolSpecs}
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

// denialOutput is the tool_result output payload recorded when a tool call is
// denied by an approver.
func denialOutput(reason string) json.RawMessage {
	out, err := json.Marshal(map[string]string{"error": reason})
	if err != nil {
		return json.RawMessage(`{"error":"tool denied"}`)
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
