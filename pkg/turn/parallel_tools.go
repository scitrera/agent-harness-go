package turn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

// toolBatchParallelSafe requires an affirmative contract from every sibling
// call. One unclassified or explicitly sequential tool keeps the complete model
// batch on the existing source-ordered path.
func toolBatchParallelSafe(calls []protocol.ToolInvokeEnvelope, tt turnTools) bool {
	if len(calls) < 2 {
		return false
	}
	for i := range calls {
		if tt.concurrencyByTool[calls[i].Name] != tools.ConcurrencyParallelSafe {
			return false
		}
	}
	return true
}

type preparedParallelTool struct {
	call         protocol.ToolInvokeEnvelope
	hookCall     hooks.ToolCall
	decision     hooks.Decision
	hookDenied   bool
	invoke       func(context.Context) (tools.Result, error)
	admissionErr error
	admittedAt   time.Time
	result       tools.Result
	err          error
	started      time.Time
	finished     time.Time
	toolCtx      context.Context
	finishTrace  func(tools.Result, error)
	dynamic      bool
}

// prepareParallelTool performs every provider/static authorization stage but
// does not execute the body. It mirrors invokeTool's two dispatch paths while
// returning a closure that can run only after the entire batch is admitted.
func (r *Runner) prepareParallelTool(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, call protocol.ToolInvokeEnvelope, tt turnTools) (func(context.Context) (tools.Result, error), error) {
	if toolExcluded(ctx, call.Name) {
		return nil, fmt.Errorf("%s is not available in this context", call.Name)
	}
	bound, err := bindCatalogToolCall(call, tt)
	if err != nil {
		return nil, err
	}
	call = bound
	trust := tt.trustByTool[call.Name]
	if provider, ok := tt.providerByTool[call.Name]; ok {
		entry, err := r.resolveCatalogInvocation(ctx, call, tt)
		if err != nil {
			return nil, err
		}
		in := AuthzInput{Call: call, Addr: addr, Trust: trust, ProviderID: provider.ID()}
		if decision := r.authorizeToolProvider(ctx, in, trustBase(trust)); decision.Outcome != Allow {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, policyErrorFor(call.Name)
		}
		req := tools.RequestFromEnvelope(call)
		req.CatalogMeta = cloneCatalogMeta(entry.Descriptor.Meta)
		if authority, ok := tools.MemoryAuthorityFrom(ctx); ok {
			req.Authority = authority
		}
		if messageID, ok := tools.MessageIDFrom(ctx); ok {
			req.MessageID = messageID
		}
		return func(invokeCtx context.Context) (tools.Result, error) {
			return provider.Invoke(invokeCtx, req)
		}, nil
	}

	in := AuthzInput{Call: call, Addr: addr, Trust: trust}
	switch decision := r.safety().Authorize(ctx, in); decision.Outcome {
	case Deny:
		return nil, policyErrorFor(call.Name)
	case Prompt:
		if r.approvals == nil {
			return nil, policyErrorFor(call.Name)
		}
		if grant := (interactiveAuthorizer{r: r}).Authorize(ctx, in); grant.Outcome != Allow {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, policyErrorFor(call.Name)
		}
		prepared, err := session.PrepareToolApproved(ctx, call)
		if err != nil {
			return nil, err
		}
		return prepared.Invoke, nil
	}

	prepared, err := session.PrepareTool(ctx, call)
	if err == nil {
		return prepared.Invoke, nil
	}
	if r.approvals == nil || !errors.Is(err, tools.ErrToolRequiresApproval) {
		return nil, err
	}
	if decision := r.authorizeTool(ctx, in, Prompt); decision.Outcome != Allow {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	prepared, err = session.PrepareToolApproved(ctx, call)
	if err != nil {
		return nil, err
	}
	return prepared.Invoke, nil
}

// runParallelToolBatch has three deliberately non-overlapping phases:
// source-ordered durable checkpoint + authorization, concurrent invocation, and
// source-ordered persistence/streaming. No approval prompt can race a call body.
func (r *Runner) runParallelToolBatch(
	ctx context.Context,
	session *harness.Session,
	addr protocol.MessageAddress,
	calls []protocol.ToolInvokeEnvelope,
	toolCallParts map[string]protocol.ContentPart,
	streamer *turnStreamer,
	perTurnApprovers []hooks.ToolApprover,
	tt turnTools,
	execution *turnExecution,
	assistantRef turnjournal.HistoryMessageRef,
	toolIterations int,
) ([]protocol.ContentPart, error) {
	prepared := make([]preparedParallelTool, len(calls))
	checkpointCalls := append([]protocol.ToolInvokeEnvelope(nil), calls...)
	for i := range checkpointCalls {
		if checkpointCalls[i].Addr.ThreadID == "" {
			checkpointCalls[i].Addr = addr
		}
	}
	if err := execution.toolsPending(ctx, checkpointCalls, assistantRef, toolIterations); err != nil {
		return nil, err
	}

	// Ordered preflight: stream each call and settle hooks/policy/safety/approval
	// before the next call is considered. Denials are buffered as ordinary error
	// results and therefore commit in assistant source order too.
	for i := range checkpointCalls {
		call := checkpointCalls[i]
		if part, ok := toolCallParts[call.CallID]; ok {
			if _, err := streamer.appendUnstreamed(ctx, part); err != nil {
				return nil, fmt.Errorf("stream tool call: %w", err)
			}
		}
		hookCall := hooks.ToolCall{CallID: call.CallID, Name: call.Name, Args: protocol.ArgsToRaw(call.Args), Addr: call.Addr}
		r.publishToolEvent(ctx, toolEventFromCall(tools.ToolEventQueued, call))
		decision := r.approveToolCall(ctx, hookCall, perTurnApprovers)
		prepared[i] = preparedParallelTool{call: call, hookCall: hookCall, decision: decision}
		if !decision.Allow {
			prepared[i].hookDenied = true
			prepared[i].admissionErr = errors.New(decision.Reason)
			continue
		}
		prepared[i].admittedAt = time.Now()
		invoke, err := r.prepareParallelTool(ctx, session, addr, call, tt)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("authorize tool %s: %w", call.Name, err)
			}
			prepared[i].admissionErr = err
			continue
		}
		prepared[i].invoke = invoke
		_, prepared[i].dynamic = tt.providerByTool[call.Name]
	}

	var wait sync.WaitGroup
	for i := range prepared {
		item := &prepared[i]
		if item.invoke == nil {
			continue
		}
		item.started = time.Now()
		toolCtx, toolSpan := telemetry.StartTool(ctx, item.call.Name, protocol.ArgsToRaw(item.call.Args))
		item.toolCtx = toolCtx
		r.publishToolEvent(toolCtx, applyHookDecision(toolEventFromCall(tools.ToolEventStarted, item.call), item.decision))
		r.notifyToolStarted(toolCtx, item.hookCall)
		item.finishTrace = func(result tools.Result, err error) {
			telemetry.AnnotateToolResult(toolSpan, result.Payload, result.IsError || err != nil)
			telemetry.FinishErr(toolSpan, err)
		}
		wait.Add(1)
		go func(item *preparedParallelTool, invokeCtx context.Context) {
			defer wait.Done()
			item.result, item.err = item.invoke(invokeCtx)
			item.finished = time.Now()
			item.finishTrace(item.result, item.err)
		}(item, toolCtx)
	}
	wait.Wait()

	var injectUser []protocol.ContentPart
	for i := range prepared {
		item := &prepared[i]
		if item.invoke != nil {
			r.notifyToolFinished(item.toolCtx, item.hookCall, item.err != nil || item.result.IsError, item.err)
			r.notifyToolResult(item.toolCtx, item.hookCall, item.result, item.err)
			r.publishToolEvent(item.toolCtx, finishToolEventAt(item.call, item.started, item.finished, item.result, item.err))
		}
		if errors.Is(item.err, subagent.ErrParentCheckpointUncertain) {
			return nil, item.err
		}
		if item.admissionErr != nil {
			output := toolErrorOutput(item.admissionErr.Error())
			var finished tools.ToolEvent
			if item.hookDenied {
				finished = applyHookDecision(toolEventFromCall(tools.ToolEventFinished, item.call), item.decision)
			} else {
				finished = finishToolEvent(item.call, item.admittedAt, tools.Result{}, item.admissionErr)
			}
			finished.Result.PayloadBytes = len(output)
			r.publishToolEvent(ctx, finished)
			if err := r.recordToolError(ctx, session, streamer, item.call.CallID, item.call.Name, output); err != nil {
				return nil, err
			}
			if err := execution.toolConfirmed(ctx, session, item.call.CallID, toolIterations); err != nil {
				return nil, err
			}
			continue
		}
		if item.err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("invoke tool %s: %w", item.call.Name, item.err)
			}
			slog.WarnContext(ctx, "tool call failed; returning error to model", slog.String("tool", item.call.Name), slog.Any("err", item.err))
			if err := r.recordToolError(ctx, session, streamer, item.call.CallID, item.call.Name, toolErrorOutput(item.err.Error())); err != nil {
				return nil, err
			}
			if err := execution.toolConfirmed(ctx, session, item.call.CallID, toolIterations); err != nil {
				return nil, err
			}
			continue
		}

		result := item.result
		if !result.IsError {
			messageID, _ := tools.MessageIDFrom(ctx)
			for index := range result.Metadata.References {
				reference := result.Metadata.References[index]
				r.notifyTurn(ctx, hooks.TurnEvent{
					Phase: hooks.PhaseResourceReferenced, Addr: addr, Iteration: toolIterations, MessageID: messageID,
					OperationID: fmt.Sprintf("tool-reference-%s-%d", item.call.CallID, index), Reference: &reference,
				})
			}
		}
		slog.InfoContext(ctx, "tool call",
			slog.String("tool", item.call.Name), slog.Bool("dynamic", item.dynamic),
			slog.Bool("is_error", result.IsError), slog.Duration("elapsed", item.finished.Sub(item.started)))
		recordToolFiles(ctx, result)
		slog.DebugContext(ctx, "tool call args", slog.String("tool", item.call.Name),
			slog.String("args", truncForLog(string(protocol.ArgsToRaw(item.call.Args)), 512)))

		part, err := result.ContentPart()
		if err != nil {
			return nil, fmt.Errorf("tool result part: %w", err)
		}
		if len(result.Parts) > 0 {
			parts := append([]protocol.ContentPart{part}, result.Parts...)
			if err := session.AppendToolResultParts(ctx, item.call.CallID, parts...); err != nil {
				return nil, fmt.Errorf("record tool result parts: %w", err)
			}
		} else if err := session.AppendToolResult(ctx, item.call.CallID, part); err != nil {
			return nil, fmt.Errorf("record tool result: %w", err)
		}
		if err := execution.toolConfirmed(ctx, session, item.call.CallID, toolIterations); err != nil {
			return nil, err
		}
		if _, err := streamer.appendPart(ctx, part); err != nil {
			return nil, fmt.Errorf("stream tool result: %w", err)
		}
		for _, extra := range result.Parts {
			if _, err := streamer.appendPart(ctx, extra); err != nil {
				return nil, fmt.Errorf("stream tool result extra part: %w", err)
			}
		}
		injectUser = append(injectUser, result.InjectUserParts...)
	}
	return injectUser, nil
}
