package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const ToolResultMetadataKey = "scitrera.result_metadata"
const ToolTaskIDMetaKey = "scitrera.tool_task_id"

// ClientToolHostConfig configures a frontend that owns local workspace tools.
type ClientToolHostConfig struct {
	WorkspaceID   string
	WorkspaceRoot string
	StateDir      string
	ToolHostID    string
	// AgentTopic is the only caller accepted by the default OSS access policy.
	AgentTopic string
	// AccessAuthorizer may replace the strict agent-topic check in an enterprise
	// composition that also validates OBO grants and workspace sharing policy.
	AccessAuthorizer ClientToolAccessAuthorizer
	Publisher        workspacepkg.ViewPublisher
	Python           string
	Timeout          time.Duration
	MaxOutput        int
}

// ClientToolHost owns host-private paths and executes only invocations carrying
// a matching logical view binding for this exact Aether user window.
type ClientToolHost struct {
	local            *boundWorkspaceToolHost
	agentTopic       string
	accessAuthorizer ClientToolAccessAuthorizer
}

type clientToolCallKey struct {
	sourceTopic string
	workspaceID string
	taskID      string
	callID      string
}

type activeClientToolCall struct {
	cancel     context.CancelFunc
	address    spec.MessageAddress
	onBehalfOf workspacepkg.Principal
	scope      workspacepkg.ExecutionScope
	checked    bool
	accessErr  error
}

// ClientToolAccessRequest carries both the logical binding and the identities
// authenticated by Aether. Enterprise policies can distinguish same-user,
// named-principal, and workspace-member sharing without trusting envelope JSON.
type ClientToolAccessRequest struct {
	Binding    spec.ExecutionBinding
	Policy     workspacepkg.ExecutionViewPolicy
	ToolName   string
	AgentTopic string
	OnBehalfOf workspacepkg.Principal
	Address    spec.MessageAddress
}

// ClientToolAccessAuthorizer is the client-side half of tool sharing policy.
// Aether ACLs remain the outer routing gate; this is the resource-level gate.
type ClientToolAccessAuthorizer interface {
	AuthorizeClientTool(ctx context.Context, request ClientToolAccessRequest) error
}

// NewClientToolHost registers and optionally publishes the initial local view.
func NewClientToolHost(ctx context.Context, cfg ClientToolHostConfig) (*ClientToolHost, error) {
	local, err := newBoundWorkspaceToolHost(ctx, boundWorkspaceToolHostConfig{
		WorkspaceID: cfg.WorkspaceID, WorkspaceRoot: cfg.WorkspaceRoot, StateDir: cfg.StateDir,
		ToolHostID: cfg.ToolHostID, ExecutionSite: spec.ExecutionSiteClient,
		Publisher: cfg.Publisher, Python: cfg.Python, Timeout: cfg.Timeout, MaxOutput: cfg.MaxOutput,
	})
	if err != nil {
		return nil, err
	}
	if cfg.AccessAuthorizer == nil && cfg.AgentTopic == "" {
		return nil, fmt.Errorf("client tool host: agent topic required by default access policy")
	}
	host := &ClientToolHost{
		local:            local,
		agentTopic:       cfg.AgentTopic,
		accessAuthorizer: cfg.AccessAuthorizer,
	}
	return host, nil
}

func (h *ClientToolHost) GrantWorkingDirectory(dir string) error {
	return h.local.GrantWorkingDirectory(dir)
}

func (h *ClientToolHost) GrantWorkspaceDirectory(dir string) error {
	return h.local.GrantWorkingDirectory(dir)
}

func (h *ClientToolHost) ExecutionBindingForDirectory(ctx context.Context, dir string) (spec.ExecutionBinding, error) {
	return h.local.ExecutionBindingForDirectory(ctx, dir)
}

// ResolveWorkspaceForDirectory exposes only the logical project identity the
// TUI needs to select its thread/history partition.
func (h *ClientToolHost) ResolveWorkspaceForDirectory(ctx context.Context, dir string) (string, error) {
	return h.local.views.ResolveWorkspaceForDirectory(ctx, dir)
}

// Renew republishes every view hosted by this client before its authoritative
// observation lease expires.
func (h *ClientToolHost) Renew(ctx context.Context) error { return h.local.Renew(ctx) }

func (h *ClientToolHost) invoke(ctx context.Context, access ClientToolAccessRequest, envelope spec.ToolInvokeEnvelope) (tools.Result, error) {
	if envelope.SchemaVersion != spec.ToolsSchemaVersion {
		return tools.Result{}, fmt.Errorf("unsupported tool schema version %q", envelope.SchemaVersion)
	}
	if _, ok := workspaceToolNames[envelope.Name]; !ok {
		return tools.Result{}, fmt.Errorf("tool %q is not hosted by this client", envelope.Name)
	}
	scope, err := toolEnvelopeExecutionScope(envelope)
	if err != nil {
		return tools.Result{}, err
	}
	binding := scope.Binding
	if envelope.Addr.WorkspaceID != binding.WorkspaceID {
		return tools.Result{}, fmt.Errorf("tool invocation workspace does not match execution binding")
	}
	access.Binding = binding
	access.Policy = scope.Policy
	access.ToolName = envelope.Name
	access.Address = envelope.Addr
	if h.accessAuthorizer != nil {
		if err := h.accessAuthorizer.AuthorizeClientTool(ctx, access); err != nil {
			return tools.Result{}, fmt.Errorf("client tool access denied: %w", err)
		}
	} else if access.AgentTopic == "" || access.AgentTopic != h.agentTopic {
		return tools.Result{}, fmt.Errorf("client tool access denied: unexpected agent")
	}
	ctx = workspacepkg.WithExecutionScope(ctx, scope)
	return h.local.invokeBound(ctx, binding, tools.RequestFromEnvelope(envelope))
}

func toolEnvelopeExecutionScope(envelope spec.ToolInvokeEnvelope) (workspacepkg.ExecutionScope, error) {
	raw := envelope.Meta[spec.ExecutionBindingMetaKey]
	if len(raw) == 0 {
		return workspacepkg.ExecutionScope{}, fmt.Errorf("tool invocation has no execution binding")
	}
	var binding spec.ExecutionBinding
	if err := json.Unmarshal(raw, &binding); err != nil {
		return workspacepkg.ExecutionScope{}, fmt.Errorf("decode tool execution binding: %w", err)
	}
	if err := binding.Validate(); err != nil {
		return workspacepkg.ExecutionScope{}, err
	}
	rawPolicy := envelope.Meta[workspacepkg.ExecutionViewPolicyMetaKey]
	if len(rawPolicy) == 0 {
		return workspacepkg.ExecutionScope{}, fmt.Errorf("tool invocation has no execution view policy")
	}
	policy, err := workspacepkg.DecodeExecutionViewPolicy(rawPolicy)
	if err != nil {
		return workspacepkg.ExecutionScope{}, err
	}
	return workspacepkg.NewExecutionScope(binding, policy)
}

func toolResultBody(envelope spec.ToolInvokeEnvelope, result tools.Result, invokeErr error) spec.ToolResultPartBody {
	body := spec.ToolResultPartBody{
		Type:    string(spec.PartToolResult),
		CallID:  envelope.CallID,
		Name:    envelope.Name,
		Output:  result.Payload,
		IsError: result.IsError || invokeErr != nil,
	}
	if invokeErr != nil {
		errorType := "tool_execution_error"
		if errors.Is(invokeErr, context.Canceled) {
			errorType = "tool_cancelled"
		}
		body.Error = &spec.ToolError{Type: errorType, Message: invokeErr.Error()}
	}
	if metadata, err := json.Marshal(result.Metadata); err == nil && string(metadata) != "{}" {
		body.Meta = map[string]json.RawMessage{ToolResultMetadataKey: metadata}
	}
	if envelope.Addr.TaskID != "" {
		if body.Meta == nil {
			body.Meta = map[string]json.RawMessage{}
		}
		if taskID, err := json.Marshal(envelope.Addr.TaskID); err == nil {
			body.Meta[ToolTaskIDMetaKey] = taskID
		}
	}
	return body
}

func (c *Client) onToolCallMessage(ctx context.Context, msg *sdk.Message) error {
	if msg == nil || len(msg.Payload) == 0 || msg.SourceTopic == "" {
		return nil
	}
	copyMessage := *msg
	copyMessage.Payload = append([]byte(nil), msg.Payload...)
	c.handleToolCall(context.WithoutCancel(ctx), &copyMessage)
	return nil
}

func (c *Client) handleToolCall(ctx context.Context, msg *sdk.Message) {
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg.Payload, &header); err != nil {
		return
	}
	if header.Type == spec.ToolCancelType {
		var envelope spec.ToolCancelEnvelope
		if err := json.Unmarshal(msg.Payload, &envelope); err != nil || envelope.Validate() != nil {
			return
		}
		c.cancelClientToolCall(msg, envelope)
		return
	}
	if header.Type != "" {
		return
	}
	var envelope spec.ToolInvokeEnvelope
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil || envelope.CallID == "" {
		return
	}
	c.mu.Lock()
	host := c.toolHost
	c.mu.Unlock()
	var scope workspacepkg.ExecutionScope
	var accessErr error
	checked := host != nil && host.accessAuthorizer != nil
	if checked {
		scope, accessErr = toolEnvelopeExecutionScope(envelope)
		if accessErr == nil {
			accessErr = ValidateExecutionBindingAccessReceipt(
				msg.AccessReceipt, msg.OnBehalfSubject, scope, envelope.CallID, c.ToolHostID(), time.Now(),
			)
		}
	}
	key := clientToolKey(msg.SourceTopic, envelope.Addr, envelope.CallID)
	callCtx, cancel := context.WithCancel(ctx)
	active := &activeClientToolCall{
		cancel: cancel, address: envelope.Addr, onBehalfOf: toolMessagePrincipal(msg),
		scope: scope, checked: checked, accessErr: accessErr,
	}
	c.mu.Lock()
	if _, exists := c.activeToolCalls[key]; exists {
		c.mu.Unlock()
		cancel()
		return
	}
	c.activeToolCalls[key] = active
	c.mu.Unlock()
	go c.executeClientToolCall(callCtx, msg, envelope, host, key, active)
}

func (c *Client) executeClientToolCall(
	ctx context.Context,
	msg *sdk.Message,
	envelope spec.ToolInvokeEnvelope,
	host *ClientToolHost,
	key clientToolCallKey,
	active *activeClientToolCall,
) {
	defer func() {
		c.mu.Lock()
		if c.activeToolCalls[key] == active {
			delete(c.activeToolCalls, key)
		}
		c.mu.Unlock()
		active.cancel()
	}()
	var result tools.Result
	var invokeErr error
	if active.accessErr != nil {
		invokeErr = fmt.Errorf("client tool access denied: %w", active.accessErr)
	} else if host == nil {
		invokeErr = fmt.Errorf("client workspace tool host is not configured")
	} else {
		access := ClientToolAccessRequest{AgentTopic: msg.SourceTopic, OnBehalfOf: active.onBehalfOf}
		result, invokeErr = host.invoke(ctx, access, envelope)
	}
	if ctx.Err() != nil {
		invokeErr = ctx.Err()
	}
	payload, err := json.Marshal(toolResultBody(envelope, result, invokeErr))
	if err != nil {
		return
	}
	_ = c.sendToolMessage(msg.SourceTopic, payload)
}

func (c *Client) cancelClientToolCall(msg *sdk.Message, envelope spec.ToolCancelEnvelope) {
	key := clientToolKey(msg.SourceTopic, envelope.Addr, envelope.CallID)
	onBehalfOf := toolMessagePrincipal(msg)
	c.mu.Lock()
	active := c.activeToolCalls[key]
	if active == nil || !sameToolCallAddress(active.address, envelope.Addr) || active.onBehalfOf != onBehalfOf {
		c.mu.Unlock()
		return
	}
	if active.checked {
		if err := ValidateExecutionBindingAccessReceipt(
			msg.AccessReceipt, msg.OnBehalfSubject, active.scope, envelope.CallID, c.ToolHostID(), time.Now(),
		); err != nil {
			c.mu.Unlock()
			return
		}
	}
	c.mu.Unlock()
	active.cancel()
}

func (c *Client) cancelAllActiveToolCalls() {
	c.mu.Lock()
	cancels := c.activeToolCancelsLocked()
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (c *Client) activeToolCancelsLocked() []context.CancelFunc {
	cancels := make([]context.CancelFunc, 0, len(c.activeToolCalls))
	for _, active := range c.activeToolCalls {
		cancels = append(cancels, active.cancel)
	}
	return cancels
}

func clientToolKey(sourceTopic string, addr spec.MessageAddress, callID string) clientToolCallKey {
	return clientToolCallKey{
		sourceTopic: sourceTopic, workspaceID: addr.WorkspaceID, taskID: addr.TaskID, callID: callID,
	}
}

func toolMessagePrincipal(msg *sdk.Message) workspacepkg.Principal {
	if msg == nil || msg.OnBehalfSubject == nil {
		return workspacepkg.Principal{}
	}
	return workspacepkg.Principal{
		Type: msg.OnBehalfSubject.GetPrincipalType(), ID: msg.OnBehalfSubject.GetPrincipalId(),
	}
}

func sameToolCallAddress(left, right spec.MessageAddress) bool {
	return left.TenantID == right.TenantID && left.WorkspaceID == right.WorkspaceID &&
		left.UserID == right.UserID && left.ThreadID == right.ThreadID &&
		left.AppID == right.AppID && left.AgentID == right.AgentID &&
		left.TaskID == right.TaskID && left.RequestID == right.RequestID &&
		left.Ownership == right.Ownership
}

var _ interface {
	GrantWorkingDirectory(string) error
} = (*ClientToolHost)(nil)
