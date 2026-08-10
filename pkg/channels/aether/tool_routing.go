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

type remoteToolDelegate struct {
	channel *Channel
	binding spec.ExecutionBinding
}

func (d remoteToolDelegate) HandlesTool(name string) bool {
	_, ok := clientWorkspaceTools[name]
	return ok
}

func (d remoteToolDelegate) InvokeTool(ctx context.Context, req tools.Request) (tools.Result, error) {
	return d.channel.invokeClientTool(ctx, d.binding, req)
}

// TurnContext binds the turn's built-in workspace tools to the exact client
// view captured at ingress. A turn without an execution binding stays on the
// worker, which is the required path for scheduled/background Aether work.
func (c *Channel) TurnContext(ctx context.Context, addr protocol.MessageAddress) context.Context {
	c.mu.Lock()
	binding, ok := c.executionBindings[addr.TaskID]
	c.mu.Unlock()
	if !ok {
		return ctx
	}
	return tools.WithToolDelegate(ctx, remoteToolDelegate{channel: c, binding: binding})
}

func (c *Channel) invokeClientTool(
	ctx context.Context,
	binding spec.ExecutionBinding,
	req tools.Request,
) (tools.Result, error) {
	if req.Addr.TaskID == "" {
		return tools.Result{}, fmt.Errorf("aether: client tool invocation has no task id")
	}
	if req.Addr.WorkspaceID != binding.WorkspaceID {
		return tools.Result{}, fmt.Errorf("aether: client tool invocation workspace changed after binding")
	}
	bindingJSON, err := json.Marshal(binding)
	if err != nil {
		return tools.Result{}, fmt.Errorf("aether: encode execution binding: %w", err)
	}
	envelope := spec.ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion,
		CallID:        req.CallID,
		Name:          req.Name,
		Args:          protocol.RawToArgs(req.Arguments),
		Addr:          req.Addr,
		Meta: map[string]json.RawMessage{
			spec.ExecutionBindingMetaKey: bindingJSON,
		},
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return tools.Result{}, fmt.Errorf("aether: encode tool invocation: %w", err)
	}
	key := toolCallKey(req.Addr.TaskID, req.CallID)
	response := make(chan toolCallResponse, 1)
	c.mu.Lock()
	current, bound := c.executionBindings[req.Addr.TaskID]
	access := c.executionAccess[req.Addr.TaskID]
	topic := binding.ToolHostID
	if !bound || current.ToolHostID != binding.ToolHostID || current.ViewID != binding.ViewID {
		c.mu.Unlock()
		return tools.Result{}, fmt.Errorf("aether: client execution binding is no longer active")
	}
	if topic == "" {
		c.mu.Unlock()
		return tools.Result{}, fmt.Errorf("aether: bound client tool host is not routable")
	}
	if _, exists := c.pendingToolCalls[key]; exists {
		c.mu.Unlock()
		return tools.Result{}, fmt.Errorf("aether: duplicate pending client tool call %q", req.CallID)
	}
	c.pendingToolCalls[key] = response
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.pendingToolCalls[key] == response {
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
	if authorizationProvider != nil {
		authorization, authErr := authorizationProvider.AuthorizationForClientTool(ctx, access, req.Name)
		if authErr != nil {
			return tools.Result{}, fmt.Errorf("aether: authorize client tool call: %w", authErr)
		}
		if crossHost {
			if authErr := validateCrossHostAuthorization(authorization, access.OnBehalfOf); authErr != nil {
				return tools.Result{}, fmt.Errorf("aether: authorize cross-host client tool call: %w", authErr)
			}
		}
		sendErr = c.sendAuthorizedToolMessage(topic, payload, authorization)
	} else {
		sendErr = c.sendToolMessage(topic, payload)
	}
	if sendErr != nil {
		return tools.Result{}, fmt.Errorf("aether: send client tool invocation: %w", sendErr)
	}
	select {
	case <-ctx.Done():
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
		if raw := received.body.Meta[toolResultMetadataKey]; len(raw) > 0 {
			_ = json.Unmarshal(raw, &result.Metadata)
		}
		if received.body.Error != nil {
			return result, errors.New(received.body.Error.Message)
		}
		return result, nil
	}
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
	if raw := body.Meta[toolTaskIDMetaKey]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &taskID)
	}
	if taskID == "" {
		return nil
	}
	key := toolCallKey(taskID, body.CallID)
	c.mu.Lock()
	pending := c.pendingToolCalls[key]
	binding, bound := c.executionBindings[taskID]
	c.mu.Unlock()
	if pending == nil || !bound || msg.SourceTopic != binding.ToolHostID {
		return nil
	}
	select {
	case pending <- toolCallResponse{body: body, source: msg.SourceTopic}:
	default:
	}
	return nil
}

func toolCallKey(taskID, callID string) string { return taskID + "\x00" + callID }

var _ tools.ToolDelegate = remoteToolDelegate{}
