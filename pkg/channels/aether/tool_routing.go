// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type toolCallResponse struct {
	body   spec.ToolResultPartBody
	source string
}

type pendingToolCall struct {
	response chan toolCallResponse
	binding  spec.ExecutionBinding
}

type remoteToolDelegate struct {
	channel *Channel
	binding spec.ExecutionBinding
	policy  workspacepkg.ExecutionViewPolicy
	access  workspacepkg.ExecutionBindingAuthorizationRequest
}

func (d remoteToolDelegate) HandlesTool(name string) bool {
	_, ok := workspaceToolNames[name]
	return ok
}

type workerToolDelegate struct {
	host    *WorkerToolHost
	binding spec.ExecutionBinding
	policy  ScheduledViewPolicy
}

func (d workerToolDelegate) HandlesTool(name string) bool {
	_, ok := workspaceToolNames[name]
	return ok
}

func (d workerToolDelegate) InvokeTool(ctx context.Context, req tools.Request) (tools.Result, error) {
	return d.host.invoke(ctx, d.binding, d.policy, req)
}

func (d remoteToolDelegate) InvokeTool(ctx context.Context, req tools.Request) (tools.Result, error) {
	return d.channel.invokeClientToolWithAccess(ctx, d.binding, d.policy, d.access, req)
}

// TurnContext binds the turn's built-in workspace tools to the exact client
// view captured at ingress. A turn without an execution binding stays on the
// worker, which is the required path for scheduled/background Aether work.
func (c *Channel) TurnContext(ctx context.Context, addr protocol.MessageAddress) context.Context {
	c.mu.Lock()
	binding, ok := c.executionBindings[addr.TaskID]
	policy := c.executionPolicies[addr.TaskID]
	access := c.executionAccess[addr.TaskID]
	c.mu.Unlock()
	if !ok {
		return ctx
	}
	viewPolicy := policy.executionViewPolicy()
	scope, err := workspacepkg.NewExecutionScope(binding, viewPolicy)
	if err == nil {
		ctx = workspacepkg.WithExecutionScope(ctx, scope)
	}
	if binding.ExecutionSite == spec.ExecutionSiteWorker {
		c.sessionMu.Lock()
		host := c.workerToolHost
		c.sessionMu.Unlock()
		return tools.WithToolDelegate(ctx, workerToolDelegate{host: host, binding: binding, policy: policy})
	}
	return tools.WithToolDelegate(ctx, remoteToolDelegate{channel: c, binding: binding, policy: viewPolicy, access: access})
}

// BindAssignedExecutionScope reconstructs the exact tool authority carried by
// a durable child task. Validation happens before claim. Worker views must be
// local to this exact agent; client views require both durable binding policy
// and an OBO provider because the executor is necessarily a different topic.
func (c *Channel) BindAssignedExecutionScope(
	ctx context.Context,
	taskID string,
	scope workspacepkg.ExecutionScope,
) (context.Context, func(), error) {
	if taskID == "" {
		return ctx, nil, errors.New("assigned execution scope requires a task id")
	}
	if err := scope.Validate(); err != nil {
		return ctx, nil, err
	}
	policy := ScheduledViewPolicy{
		WriteAccess:      scope.Policy.WriteAccess,
		AllowMutableView: scope.Policy.AllowMutableView,
		AllowDirtyView:   scope.Policy.AllowDirtyView,
	}
	ctx = workspacepkg.WithExecutionScope(ctx, scope)
	switch scope.Binding.ExecutionSite {
	case spec.ExecutionSiteWorker:
		c.sessionMu.Lock()
		host := c.workerToolHost
		c.sessionMu.Unlock()
		if scope.Binding.ToolHostID != c.Topic() {
			return ctx, nil, errors.New("worker execution scope targets another tool host")
		}
		if host == nil {
			return ctx, nil, errors.New("worker workspace tool host is not configured")
		}
		if err := host.validateScheduledBinding(ctx, scope.Binding, policy); err != nil {
			return ctx, nil, err
		}
		return tools.WithToolDelegate(ctx, workerToolDelegate{
			host: host, binding: scope.Binding, policy: policy,
		}), func() {}, nil
	case spec.ExecutionSiteClient:
		c.sessionMu.Lock()
		authorizer := c.executionBindingAuthorizer
		provider := c.toolCallAuthorizationProvider
		c.sessionMu.Unlock()
		if authorizer == nil || provider == nil {
			return ctx, nil, errors.New("client execution scope requires binding and OBO providers")
		}
		authority, _ := tools.MemoryAuthorityFrom(ctx)
		if authority.GrantID == "" || authority.SubjectType == "" || authority.SubjectID == "" {
			return ctx, nil, errors.New("client execution scope requires typed on-behalf-of task authority")
		}
		access := workspacepkg.ExecutionBindingAuthorizationRequest{
			Binding: scope.Binding, ViewPolicy: scope.Policy, SourceTopic: c.Topic(),
			OnBehalfOf: workspacepkg.Principal{Type: authority.SubjectType, ID: authority.SubjectID},
		}
		if err := authorizer.AuthorizeExecutionBinding(ctx, access); err != nil {
			return ctx, nil, err
		}
		return tools.WithToolDelegate(ctx, remoteToolDelegate{
			channel: c, binding: scope.Binding, policy: scope.Policy, access: access,
		}), func() {}, nil
	default:
		return ctx, nil, fmt.Errorf("unsupported assigned execution site %q", scope.Binding.ExecutionSite)
	}
}

func (c *Channel) invokeClientTool(
	ctx context.Context,
	binding spec.ExecutionBinding,
	req tools.Request,
) (tools.Result, error) {
	c.mu.Lock()
	current, bound := c.executionBindings[req.Addr.TaskID]
	routePolicy := c.executionPolicies[req.Addr.TaskID]
	access := c.executionAccess[req.Addr.TaskID]
	c.mu.Unlock()
	if !bound || current.ToolHostID != binding.ToolHostID || current.ViewID != binding.ViewID {
		return tools.Result{}, fmt.Errorf("aether: client execution binding is no longer active")
	}
	policy := routePolicy.executionViewPolicy()
	return c.invokeClientToolWithAccess(ctx, binding, policy, access, req)
}

func (c *Channel) invokeClientToolWithAccess(
	ctx context.Context,
	binding spec.ExecutionBinding,
	policy workspacepkg.ExecutionViewPolicy,
	access workspacepkg.ExecutionBindingAuthorizationRequest,
	req tools.Request,
) (tools.Result, error) {
	if req.Addr.TaskID == "" {
		return tools.Result{}, fmt.Errorf("aether: client tool invocation has no task id")
	}
	if req.Addr.WorkspaceID != binding.WorkspaceID {
		return tools.Result{}, fmt.Errorf("aether: client tool invocation workspace changed after binding")
	}
	access.Binding = binding
	access.ViewPolicy = policy
	bindingJSON, err := json.Marshal(binding)
	if err != nil {
		return tools.Result{}, fmt.Errorf("aether: encode execution binding: %w", err)
	}
	policyJSON, err := workspacepkg.EncodeExecutionViewPolicy(policy)
	if err != nil {
		return tools.Result{}, fmt.Errorf("aether: encode execution view policy: %w", err)
	}
	envelope := spec.ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion,
		CallID:        req.CallID,
		Name:          req.Name,
		Args:          protocol.RawToArgs(req.Arguments),
		Addr:          req.Addr,
		Meta: map[string]json.RawMessage{
			spec.ExecutionBindingMetaKey:            bindingJSON,
			workspacepkg.ExecutionViewPolicyMetaKey: policyJSON,
		},
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return tools.Result{}, fmt.Errorf("aether: encode tool invocation: %w", err)
	}
	key := toolCallKey(req.Addr.TaskID, req.CallID)
	response := make(chan toolCallResponse, 1)
	pending := &pendingToolCall{response: response, binding: binding}
	c.mu.Lock()
	topic := binding.ToolHostID
	if topic == "" {
		c.mu.Unlock()
		return tools.Result{}, fmt.Errorf("aether: bound client tool host is not routable")
	}
	if _, exists := c.pendingToolCalls[key]; exists {
		c.mu.Unlock()
		return tools.Result{}, fmt.Errorf("aether: duplicate pending client tool call %q", req.CallID)
	}
	c.pendingToolCalls[key] = pending
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.pendingToolCalls[key] == pending {
			delete(c.pendingToolCalls, key)
		}
		c.mu.Unlock()
	}()
	c.sessionMu.Lock()
	authorizationProvider := c.toolCallAuthorizationProvider
	c.sessionMu.Unlock()
	crossHost := access.SourceTopic != binding.ToolHostID
	if crossHost && authorizationProvider == nil {
		return tools.Result{}, fmt.Errorf("aether: authorize cross-host client tool call: an on-behalf-of authorization provider is required")
	}
	var sendErr error
	var authorization *pb.AuthorizationContext
	if authorizationProvider != nil {
		var authErr error
		authorization, authErr = authorizationProvider.AuthorizationForClientTool(ctx, access, req.Name)
		if authErr != nil {
			return tools.Result{}, fmt.Errorf("aether: authorize client tool call: %w", authErr)
		}
		if crossHost {
			if authErr := validateCrossHostAuthorization(authorization, access.OnBehalfOf); authErr != nil {
				return tools.Result{}, fmt.Errorf("aether: authorize cross-host client tool call: %w", authErr)
			}
		}
		if crossHost {
			scope, scopeErr := workspacepkg.NewExecutionScope(binding, policy)
			if scopeErr != nil {
				return tools.Result{}, fmt.Errorf("aether: bind cross-host client tool call: %w", scopeErr)
			}
			checkedAccess, accessErr := ExecutionBindingAccessRequest(scope, req.CallID)
			if accessErr != nil {
				return tools.Result{}, fmt.Errorf("aether: check cross-host client tool call: %w", accessErr)
			}
			sendErr = c.sendCheckedToolMessage(topic, payload, authorization, checkedAccess)
		} else {
			sendErr = c.sendAuthorizedToolMessage(topic, payload, authorization)
		}
	} else {
		sendErr = c.sendToolMessage(topic, payload)
	}
	if sendErr != nil {
		return tools.Result{}, fmt.Errorf("aether: send client tool invocation: %w", sendErr)
	}
	select {
	case <-ctx.Done():
		scope, _ := workspacepkg.NewExecutionScope(binding, policy)
		c.sendClientToolCancellation(topic, authorization, scope, crossHost, req, ctx.Err())
		return tools.Result{}, ctx.Err()
	case received := <-response:
		if received.source != binding.ToolHostID {
			return tools.Result{}, fmt.Errorf("aether: tool result came from an unexpected host")
		}
		if received.body.Name != req.Name {
			return tools.Result{}, fmt.Errorf("aether: tool result name does not match the pending call")
		}
		result := tools.Result{
			CallID:  received.body.CallID,
			Name:    received.body.Name,
			Payload: received.body.Output,
			IsError: received.body.IsError,
		}
		if raw := received.body.Meta[ToolResultMetadataKey]; len(raw) > 0 {
			_ = json.Unmarshal(raw, &result.Metadata)
		}
		if received.body.Error != nil {
			return result, errors.New(received.body.Error.Message)
		}
		return result, nil
	}
}

func (c *Channel) sendClientToolCancellation(
	topic string,
	authorization *pb.AuthorizationContext,
	scope workspacepkg.ExecutionScope,
	checked bool,
	req tools.Request,
	cause error,
) {
	envelope := spec.NewToolCancelEnvelope(req.CallID, req.Addr)
	switch {
	case errors.Is(cause, context.DeadlineExceeded):
		envelope.Reason = "deadline_exceeded"
	case errors.Is(cause, context.Canceled):
		envelope.Reason = "caller_cancelled"
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	if checked {
		access, accessErr := ExecutionBindingAccessRequest(scope, req.CallID)
		if accessErr == nil {
			_ = c.sendCheckedToolMessage(topic, payload, authorization, access)
		}
		return
	}
	if authorization != nil {
		_ = c.sendAuthorizedToolMessage(topic, payload, authorization)
		return
	}
	_ = c.sendToolMessage(topic, payload)
}

func validateCrossHostAuthorization(authorization *pb.AuthorizationContext, expected workspacepkg.Principal) error {
	if authorization == nil || authorization.GetAuthorityMode() != "on_behalf_of" ||
		authorization.GetSubject() == nil || authorization.GetGrantId() == "" {
		return fmt.Errorf("a validated on-behalf-of grant is required")
	}
	if expected.ID != "" && (authorization.GetSubject().GetPrincipalType() != expected.Type ||
		authorization.GetSubject().GetPrincipalId() != expected.ID) {
		return fmt.Errorf("on-behalf-of subject changed after turn admission")
	}
	return nil
}

func (c *Channel) onToolCallMessage(_ context.Context, msg *sdk.Message) error {
	if msg == nil || len(msg.Payload) == 0 {
		return nil
	}
	var body spec.ToolResultPartBody
	if err := json.Unmarshal(msg.Payload, &body); err != nil || body.Type != string(spec.PartToolResult) || body.CallID == "" {
		return nil
	}
	var taskID string
	if raw := body.Meta[ToolTaskIDMetaKey]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &taskID)
	}
	if taskID == "" {
		return nil
	}
	key := toolCallKey(taskID, body.CallID)
	c.mu.Lock()
	pending := c.pendingToolCalls[key]
	c.mu.Unlock()
	if pending == nil || msg.SourceTopic != pending.binding.ToolHostID {
		return nil
	}
	select {
	case pending.response <- toolCallResponse{body: body, source: msg.SourceTopic}:
	default:
	}
	return nil
}

func toolCallKey(taskID, callID string) string { return taskID + "\x00" + callID }

var _ tools.ToolDelegate = remoteToolDelegate{}
var _ tools.ToolDelegate = workerToolDelegate{}
