package aether

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const toolResultMetadataKey = "scitrera.result_metadata"
const toolTaskIDMetaKey = "scitrera.tool_task_id"

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

func (h *ClientToolHost) ExecutionBindingForDirectory(ctx context.Context, dir string) (spec.ExecutionBinding, error) {
	return h.local.ExecutionBindingForDirectory(ctx, dir)
}

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

func toolEnvelopeBinding(envelope spec.ToolInvokeEnvelope) (spec.ExecutionBinding, error) {
	scope, err := toolEnvelopeExecutionScope(envelope)
	return scope.Binding, err
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
	policy := workspacepkg.ExecutionViewPolicy{WriteAccess: workspacepkg.ViewWriteAccessReadWrite}
	if rawPolicy := envelope.Meta[workspacepkg.ExecutionViewPolicyMetaKey]; len(rawPolicy) > 0 {
		var err error
		policy, err = workspacepkg.DecodeExecutionViewPolicy(rawPolicy)
		if err != nil {
			return workspacepkg.ExecutionScope{}, err
		}
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
		body.Error = &spec.ToolError{Type: "tool_execution_error", Message: invokeErr.Error()}
	}
	if metadata, err := json.Marshal(result.Metadata); err == nil && string(metadata) != "{}" {
		body.Meta = map[string]json.RawMessage{toolResultMetadataKey: metadata}
	}
	if envelope.Addr.TaskID != "" {
		if body.Meta == nil {
			body.Meta = map[string]json.RawMessage{}
		}
		if taskID, err := json.Marshal(envelope.Addr.TaskID); err == nil {
			body.Meta[toolTaskIDMetaKey] = taskID
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
	go c.handleToolCall(context.WithoutCancel(ctx), &copyMessage)
	return nil
}

func (c *Client) handleToolCall(ctx context.Context, msg *sdk.Message) {
	var envelope spec.ToolInvokeEnvelope
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil || envelope.CallID == "" {
		return
	}
	c.mu.Lock()
	host := c.toolHost
	c.mu.Unlock()
	var result tools.Result
	var invokeErr error
	if host == nil {
		invokeErr = fmt.Errorf("client workspace tool host is not configured")
	} else {
		access := ClientToolAccessRequest{AgentTopic: msg.SourceTopic}
		if msg.OnBehalfSubject != nil {
			access.OnBehalfOf = workspacepkg.Principal{
				Type: msg.OnBehalfSubject.GetPrincipalType(), ID: msg.OnBehalfSubject.GetPrincipalId(),
			}
		}
		result, invokeErr = host.invoke(ctx, access, envelope)
	}
	payload, err := json.Marshal(toolResultBody(envelope, result, invokeErr))
	if err != nil {
		return
	}
	_ = c.sendToolMessage(msg.SourceTopic, payload)
}

var _ interface {
	GrantWorkingDirectory(string) error
} = (*ClientToolHost)(nil)
