// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"encoding/json"
	"fmt"
)

type Runtime struct {
	executor *Executor
	hooks    []CommandHook
}

func NewRuntime(cfg Config, hooks []CommandHook) *Runtime {
	copied := make([]CommandHook, len(hooks))
	copy(copied, hooks)
	return &Runtime{executor: NewExecutor(cfg), hooks: copied}
}

func (r *Runtime) Dispatch(ctx context.Context, inv Invocation) ([]Result, error) {
	if r == nil {
		return nil, nil
	}
	return r.executor.Dispatch(ctx, inv, r.hooks)
}

func (r *Runtime) ApproveTool(ctx context.Context, call ToolCall) Decision {
	if r == nil || recursionGuarded(ctx) {
		return Allow()
	}
	results, err := r.Dispatch(ctx, Invocation{Event: EventPreToolUse, Tool: &call, Input: toolCallInput(call)})
	if err != nil {
		return Deny(err.Error())
	}
	for _, result := range results {
		if result.Blocked {
			return Deny(result.Reason)
		}
	}
	return Allow()
}

func (r *Runtime) ToolStarted(ctx context.Context, call ToolCall) {
	if r == nil || recursionGuarded(ctx) {
		return
	}
	_, _ = r.Dispatch(ctx, Invocation{Event: EventToolLifecycle, Tool: &call, Input: toolLifecycleInput("started", call, false, nil)})
}

func (r *Runtime) ToolFinished(ctx context.Context, call ToolCall, isError bool, err error) {
	if r == nil || recursionGuarded(ctx) {
		return
	}
	event := EventPostToolUse
	if isError || err != nil {
		event = EventPostToolUseFailure
	}
	_, _ = r.Dispatch(ctx, Invocation{Event: event, Tool: &call, Input: toolLifecycleInput("finished", call, isError, err)})
}

func toolCallInput(call ToolCall) json.RawMessage {
	input := map[string]json.RawMessage{
		"tool_input": call.Args,
	}
	envelope := map[string]any{
		"tool_name":   call.Name,
		"tool_use_id": call.CallID,
		"addr":        call.Addr,
		"tool_input":  input["tool_input"],
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return nil
	}
	return raw
}

func toolLifecycleInput(status string, call ToolCall, isError bool, err error) json.RawMessage {
	envelope := map[string]any{
		"status":      status,
		"tool_name":   call.Name,
		"tool_use_id": call.CallID,
		"addr":        call.Addr,
		"is_error":    isError,
	}
	if err != nil {
		envelope["error"] = fmt.Sprint(err)
	}
	raw, marshalErr := json.Marshal(envelope)
	if marshalErr != nil {
		return nil
	}
	return raw
}
