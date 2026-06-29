package turn

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func (r *Runner) publishToolEvent(ctx context.Context, event tools.ToolEvent) {
	if r.publisher == nil {
		return
	}
	raw, err := json.Marshal(event)
	if err != nil {
		slog.WarnContext(ctx, "marshal tool lifecycle event failed", slog.Any("err", err))
		return
	}
	if err := r.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventToolLifecycle, Addr: event.Addr, Payload: raw}); err != nil {
		slog.WarnContext(ctx, "publish tool lifecycle event failed", slog.Any("err", err))
	}
}

func toolEventFromCall(status tools.ToolEventStatus, call protocol.ToolInvokeEnvelope) tools.ToolEvent {
	return tools.NewToolEvent(status, tools.RequestFromEnvelope(call))
}

func applyHookDecision(event tools.ToolEvent, decision hooks.Decision) tools.ToolEvent {
	if decision.Allow {
		event.ApprovalDecision = "allow"
		return event
	}
	event.ApprovalDecision = "deny"
	event.Reason = decision.Reason
	event.IsError = true
	return event
}

func finishToolEvent(call protocol.ToolInvokeEnvelope, started time.Time, result tools.Result, err error) tools.ToolEvent {
	status := tools.ToolEventFinished
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		status = tools.ToolEventAborted
	}
	event := toolEventFromCall(status, call)
	event.DurationMS = tools.DurationMillis(time.Since(started))
	event.Result = result.Metadata
	event.Result.PayloadBytes = len(result.Payload)
	event.IsError = result.IsError || err != nil
	if err != nil {
		tools.ApplySafeError(&event, err)
		if errors.Is(err, tools.ErrToolRequiresApproval) {
			event.PolicyDecision = string(tools.DecisionRequiresApproval)
		}
	}
	return event
}
