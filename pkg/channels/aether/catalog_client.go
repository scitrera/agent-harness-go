package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/catalog/catalogrpc"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	defaultCatalogClientServiceTimeout = 30 * time.Second
	defaultCatalogClientInvokeTimeout  = 2 * time.Minute
)

// CatalogClientConfig binds a catalog consumer to one exact Aether agent.
// Sender is normally the same AgentClient whose catch-all OnMessage handler
// calls CatalogClient.TryHandle.
type CatalogClientConfig struct {
	Route               string
	Tenant              string
	CatalogServiceTopic string
	Sender              CatalogToolMessageSender
	ServiceTimeout      time.Duration
	InvokeTimeout       time.Duration
}

type catalogClientWaiter struct {
	expectedSource string
	result         chan catalogHostRuntimeEnvelope
}

// CatalogClient is the reusable Aether consumer for catalog query, describe,
// exact provider invocation, and checked cancellation. It deliberately owns
// the private Aether/runtime-envelope adaptation while portable tool payloads
// remain in ecosystem-messaging-spec.
type CatalogClient struct {
	route               string
	tenant              string
	catalogServiceTopic string
	sender              CatalogToolMessageSender
	serviceTimeout      time.Duration
	invokeTimeout       time.Duration
	sequence            atomic.Uint64

	mu      sync.Mutex
	waiters map[string]catalogClientWaiter
}

func NewCatalogClient(cfg CatalogClientConfig) (*CatalogClient, error) {
	if !isExactCatalogAgentTopic(cfg.Route) {
		return nil, fmt.Errorf("aether catalog client: route must be an exact agent topic")
	}
	if cfg.Sender == nil {
		return nil, fmt.Errorf("aether catalog client: sender is required")
	}
	if cfg.CatalogServiceTopic == "" {
		cfg.CatalogServiceTopic = defaultCatalogServiceTopic
	}
	if !validCatalogServiceTopic(cfg.CatalogServiceTopic) {
		return nil, fmt.Errorf("aether catalog client: catalog service topic must be sv::tool-catalog or an exact instance")
	}
	if cfg.ServiceTimeout <= 0 {
		cfg.ServiceTimeout = defaultCatalogClientServiceTimeout
	}
	if cfg.InvokeTimeout <= 0 {
		cfg.InvokeTimeout = defaultCatalogClientInvokeTimeout
	}
	return &CatalogClient{
		route: cfg.Route, tenant: cfg.Tenant,
		catalogServiceTopic: cfg.CatalogServiceTopic, sender: cfg.Sender,
		serviceTimeout: cfg.ServiceTimeout, invokeTimeout: cfg.InvokeTimeout,
		waiters: make(map[string]catalogClientWaiter),
	}, nil
}

// TryHandle delivers a reply only when both its correlation ID and the
// gateway-authenticated source topic match an in-flight request. Payload source
// and target fields never establish identity. A true result means the message
// was consumed and must not be treated as an ordinary turn.
func (c *CatalogClient) TryHandle(message *sdk.Message) bool {
	if c == nil || message == nil || message.SourceTopic == "" || len(message.Payload) == 0 {
		return false
	}
	var envelope catalogHostRuntimeEnvelope
	if err := json.Unmarshal(message.Payload, &envelope); err != nil {
		return false
	}
	requestID := envelope.RequestID
	if requestID == "" {
		requestID = envelope.Target.RequestID
	}
	if requestID == "" {
		return false
	}
	c.mu.Lock()
	waiter, ok := c.waiters[requestID]
	if ok && catalogClientSourceMatches(message.SourceTopic, waiter.expectedSource) {
		delete(c.waiters, requestID)
	} else {
		ok = false
	}
	c.mu.Unlock()
	if !ok {
		return false
	}
	waiter.result <- envelope
	return true
}

// QueryCatalog queries the private resolved catalog projection so consumers
// receive authenticated provider routes and server-owned invocation authority
// alongside each portable entry.
func (c *CatalogClient) QueryCatalog(ctx context.Context, query spec.ToolCatalogQuery, auth tools.MemoryAuthority) (catalog.ResolvedCatalogPage, error) {
	if query.SchemaVersion == "" {
		query.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := query.Validate(); err != nil {
		return catalog.ResolvedCatalogPage{}, fmt.Errorf("aether catalog client: invalid query: %w", err)
	}
	raw, err := c.callCatalog(ctx, catalogrpc.MethodQuery, query, auth)
	if err != nil {
		return catalog.ResolvedCatalogPage{}, err
	}
	var page catalog.ResolvedCatalogPage
	if err := json.Unmarshal(raw, &page); err != nil {
		return catalog.ResolvedCatalogPage{}, fmt.Errorf("aether catalog client: decode query result: %w", err)
	}
	return page, nil
}

// DescribeCatalog resolves one exact entry through the same authenticated
// catalog service boundary used by QueryCatalog.
func (c *CatalogClient) DescribeCatalog(ctx context.Context, request spec.ToolCatalogDescribeRequest, auth tools.MemoryAuthority) (catalog.ResolvedCatalogDescribeResult, error) {
	if request.SchemaVersion == "" {
		request.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := request.Validate(); err != nil {
		return catalog.ResolvedCatalogDescribeResult{}, fmt.Errorf("aether catalog client: invalid describe request: %w", err)
	}
	raw, err := c.callCatalog(ctx, catalogrpc.MethodDescribe, request, auth)
	if err != nil {
		return catalog.ResolvedCatalogDescribeResult{}, err
	}
	var result catalog.ResolvedCatalogDescribeResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return catalog.ResolvedCatalogDescribeResult{}, fmt.Errorf("aether catalog client: decode describe result: %w", err)
	}
	return result, nil
}

// InvokeCatalogTool sends an exact checked invocation directly to the admitted
// provider. The catalog service is discovery/control plane only; it never
// forwards caller authority or tool payloads.
func (c *CatalogClient) InvokeCatalogTool(
	ctx context.Context,
	providerRoute string,
	catalogContext spec.ToolCatalogContext,
	effect spec.ToolEffect,
	invocationAuthority *catalog.InvocationAuthorityProfile,
	envelope spec.ToolInvokeEnvelope,
	auth tools.MemoryAuthority,
) (json.RawMessage, error) {
	if c == nil {
		return nil, fmt.Errorf("aether catalog client: client is not initialized")
	}
	if envelope.ToolRef == nil || envelope.CallID == "" {
		return nil, fmt.Errorf("aether catalog client: exact catalog reference and call id are required")
	}
	if err := envelope.Validate(); err != nil {
		return nil, fmt.Errorf("aether catalog client: invalid tool invoke envelope: %w", err)
	}
	if !IsExactCatalogProviderTopic(providerRoute) {
		return nil, fmt.Errorf("aether catalog client: provider route must be an exact service, agent, or user-session topic")
	}
	authorization, err := catalogClientOBOAuthorization(auth)
	if err != nil {
		return nil, err
	}
	resourceID, err := catalog.EntryResourceID(catalogContext, *envelope.ToolRef)
	if err != nil {
		return nil, fmt.Errorf("aether catalog client: derive catalog resource: %w", err)
	}
	operation, requiredAccess, err := catalogClientEffectAccess(effect)
	if err != nil {
		return nil, err
	}
	target, err := catalogClientProviderTarget(providerRoute, catalogContext)
	if err != nil {
		return nil, fmt.Errorf("aether catalog client: %w", err)
	}
	invokeRaw, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("aether catalog client: encode invocation: %w", err)
	}
	payload, err := c.runtimePayload(
		target, envelope.CallID, auth.SubjectID,
		map[string]json.RawMessage{catalogToolInvokeArgument: invokeRaw},
	)
	if err != nil {
		return nil, err
	}
	continuation, err := catalogClientInvocationContinuation(providerRoute, catalogContext, envelope.CallID, invocationAuthority)
	if err != nil {
		return nil, fmt.Errorf("aether catalog client: %w", err)
	}
	waiter, err := c.register(envelope.CallID, providerRoute)
	if err != nil {
		return nil, err
	}
	defer c.release(envelope.CallID, waiter)
	checked := &pb.ResourceAccessRequest{
		ResourceType: "tool-catalog/entry", ResourceId: resourceID,
		Operation: operation, Workspace: catalogContext.WorkspaceID,
		RequiredAccessLevel: requiredAccess, CorrelationId: envelope.CallID,
	}
	if err := c.sender.SendWithOptions(sdk.SendMessageOptions{
		TargetTopic: providerRoute, Payload: payload, MessageType: sdk.MessageTypeToolCall,
		Authorization: authorization, CheckedAccess: checked, AuthorityContinuation: continuation,
	}); err != nil {
		return nil, fmt.Errorf("aether catalog client: send invocation: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.invokeTimeout)
	defer cancel()
	select {
	case reply := <-waiter.result:
		result, ok := reply.Arguments[catalogToolResultArgument]
		if !ok {
			return nil, fmt.Errorf("aether catalog client: provider reply has no %s", catalogToolResultArgument)
		}
		var body spec.ToolResultPartBody
		if err := json.Unmarshal(result, &body); err != nil {
			return nil, fmt.Errorf("aether catalog client: decode tool result: %w", err)
		}
		if body.CallID != envelope.CallID {
			return nil, fmt.Errorf("aether catalog client: tool result call id %q does not match %q", body.CallID, envelope.CallID)
		}
		return result, nil
	case <-waitCtx.Done():
		c.sendCancellation(providerRoute, catalogContext, envelope, authorization, checked, waitCtx.Err())
		return nil, fmt.Errorf("aether catalog client: invoke %s: %w", envelope.CallID, waitCtx.Err())
	}
}

func (c *CatalogClient) callCatalog(ctx context.Context, method string, request any, auth tools.MemoryAuthority) (json.RawMessage, error) {
	if c == nil {
		return nil, fmt.Errorf("aether catalog client: client is not initialized")
	}
	if method != catalogrpc.MethodQuery && method != catalogrpc.MethodDescribe {
		return nil, fmt.Errorf("aether catalog client: unsupported catalog method %q", method)
	}
	authorization, err := catalogClientOBOAuthorization(auth)
	if err != nil {
		return nil, err
	}
	requestRaw, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("aether catalog client: encode %s: %w", method, err)
	}
	requestID := fmt.Sprintf("catalog.client.%d", c.sequence.Add(1))
	target := catalogClientServiceTarget(c.catalogServiceTopic)
	payload, err := c.runtimePayload(target, requestID, auth.SubjectID, map[string]json.RawMessage{method: requestRaw})
	if err != nil {
		return nil, err
	}
	waiter, err := c.register(requestID, c.catalogServiceTopic)
	if err != nil {
		return nil, err
	}
	defer c.release(requestID, waiter)
	if err := c.sender.SendWithOptions(sdk.SendMessageOptions{
		TargetTopic: c.catalogServiceTopic, Payload: payload, MessageType: sdk.MessageTypeChat,
		Authorization: authorization,
		AuthorityContinuation: &pb.AuthorityContinuationRequest{
			ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT,
		},
	}); err != nil {
		return nil, fmt.Errorf("aether catalog client: send %s: %w", method, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, c.serviceTimeout)
	defer cancel()
	select {
	case reply := <-waiter.result:
		return catalogClientReplyResult(method, reply)
	case <-waitCtx.Done():
		return nil, fmt.Errorf("aether catalog client: %s: %w", method, waitCtx.Err())
	}
}

func (c *CatalogClient) runtimePayload(target catalogHostRuntimeAddress, requestID, userID string, arguments map[string]json.RawMessage) ([]byte, error) {
	source := catalogClientAgentAddress(c.route)
	source.Tenant = c.tenant
	target.Tenant = c.tenant
	initiator := catalogHostRuntimeAddress{
		Tenant: c.tenant, Workspace: target.Workspace, User: userID, RequestID: requestID,
	}
	payload, err := json.Marshal(catalogHostRuntimeEnvelope{
		Source: source, Target: target, Content: catalogHostRuntimeContent{Role: "agent"},
		Arguments: arguments, RequestID: requestID, Initiator: &initiator,
	})
	if err != nil {
		return nil, fmt.Errorf("aether catalog client: encode runtime envelope: %w", err)
	}
	return payload, nil
}

func (c *CatalogClient) register(requestID, expectedSource string) (catalogClientWaiter, error) {
	waiter := catalogClientWaiter{expectedSource: expectedSource, result: make(chan catalogHostRuntimeEnvelope, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.waiters[requestID]; exists {
		return catalogClientWaiter{}, fmt.Errorf("aether catalog client: duplicate request %q", requestID)
	}
	c.waiters[requestID] = waiter
	return waiter, nil
}

func (c *CatalogClient) release(requestID string, waiter catalogClientWaiter) {
	c.mu.Lock()
	if current, ok := c.waiters[requestID]; ok && current.result == waiter.result {
		delete(c.waiters, requestID)
	}
	c.mu.Unlock()
}

func (c *CatalogClient) sendCancellation(
	providerRoute string,
	catalogContext spec.ToolCatalogContext,
	envelope spec.ToolInvokeEnvelope,
	authorization *pb.AuthorizationContext,
	checked *pb.ResourceAccessRequest,
	cause error,
) {
	if envelope.Addr.TaskID == "" {
		return
	}
	cancellation := spec.NewToolCancelEnvelope(envelope.CallID, envelope.Addr)
	switch {
	case errors.Is(cause, context.DeadlineExceeded):
		cancellation.Reason = "deadline_exceeded"
	case errors.Is(cause, context.Canceled):
		cancellation.Reason = "caller_cancelled"
	default:
		cancellation.Reason = "caller_stopped"
	}
	cancelRaw, err := json.Marshal(cancellation)
	if err != nil {
		return
	}
	target, err := catalogClientProviderTarget(providerRoute, catalogContext)
	if err != nil {
		return
	}
	payload, err := c.runtimePayload(
		target, envelope.CallID, authorization.GetSubject().GetPrincipalId(),
		map[string]json.RawMessage{catalogToolCancelArgument: cancelRaw},
	)
	if err != nil {
		return
	}
	_ = c.sender.SendWithOptions(sdk.SendMessageOptions{
		TargetTopic: providerRoute, Payload: payload, MessageType: sdk.MessageTypeToolCall,
		Authorization: authorization, CheckedAccess: proto.Clone(checked).(*pb.ResourceAccessRequest),
	})
}

func catalogClientReplyResult(method string, envelope catalogHostRuntimeEnvelope) (json.RawMessage, error) {
	if raw := envelope.Arguments["error"]; len(raw) > 0 {
		var protocolError spec.ToolCatalogError
		if err := json.Unmarshal(raw, &protocolError); err != nil {
			return nil, fmt.Errorf("aether catalog client: %s error: %s", method, raw)
		}
		return nil, fmt.Errorf("aether catalog client: %s %s: %s", method, protocolError.Code, protocolError.Message)
	}
	if raw := envelope.Arguments["result"]; len(raw) > 0 {
		return raw, nil
	}
	return nil, fmt.Errorf("aether catalog client: %s reply has no result", method)
}

func catalogClientOBOAuthorization(auth tools.MemoryAuthority) (*pb.AuthorizationContext, error) {
	if auth.SubjectType != "user" || auth.SubjectID == "" || auth.GrantID == "" {
		return nil, fmt.Errorf("aether catalog client: user OBO authority is required")
	}
	return &pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of",
		Subject:       &pb.PrincipalRef{PrincipalType: "user", PrincipalId: auth.SubjectID},
		GrantId:       auth.GrantID,
	}, nil
}

func catalogClientSourceMatches(actual, expected string) bool {
	if actual == "" || expected == "" {
		return false
	}
	if strings.Count(expected, "::") == 1 && validCatalogServiceTopic(expected) {
		return strings.HasPrefix(actual, expected+"::") && isExactCatalogServiceTopic(actual)
	}
	return actual == expected
}

func isExactCatalogUserSessionTopic(topic string) bool {
	parts := strings.Split(topic, "::")
	return len(parts) == 3 && parts[0] == "us" && exactCatalogTopicParts(parts[1:]...)
}

// IsExactCatalogProviderTopic reports whether topic selects one concrete
// service instance, agent instance, or user session suitable for a checked
// direct invocation. Wildcards and implementation-only service topics are
// intentionally rejected.
func IsExactCatalogProviderTopic(topic string) bool {
	return isExactCatalogAgentTopic(topic) || isExactCatalogServiceTopic(topic) || isExactCatalogUserSessionTopic(topic)
}

func catalogClientAgentAddress(topic string) catalogHostRuntimeAddress {
	parts := strings.Split(topic, "::")
	return catalogHostRuntimeAddress{Workspace: parts[1], Agent: parts[2], Key: parts[3]}
}

func catalogClientServiceTarget(topic string) catalogHostRuntimeAddress {
	parts := strings.Split(topic, "::")
	key := parts[1]
	if len(parts) == 3 {
		key = parts[1] + ":" + parts[2]
	}
	return catalogHostRuntimeAddress{Workspace: "", Agent: key, Key: key}
}

func catalogClientProviderTarget(topic string, context spec.ToolCatalogContext) (catalogHostRuntimeAddress, error) {
	parts := strings.Split(topic, "::")
	if isExactCatalogServiceTopic(topic) {
		if context.ToolHostID != "" && context.ToolHostID != topic {
			return catalogHostRuntimeAddress{}, fmt.Errorf("service provider route does not match catalog context")
		}
		key := parts[1] + ":" + parts[2]
		return catalogHostRuntimeAddress{Workspace: "", Agent: key, Key: key}, nil
	}
	if isExactCatalogUserSessionTopic(topic) {
		if (context.SurfaceInstanceID != "" && context.SurfaceInstanceID != parts[2]) ||
			(context.ToolHostID != "" && context.ToolHostID != topic) {
			return catalogHostRuntimeAddress{}, fmt.Errorf("user-session provider route does not match catalog context")
		}
		return catalogHostRuntimeAddress{Workspace: context.WorkspaceID, User: parts[1], RequestID: parts[2]}, nil
	}
	if isExactCatalogAgentTopic(topic) {
		if context.WorkspaceID != parts[1] || (context.ToolHostID != "" && context.ToolHostID != topic) {
			return catalogHostRuntimeAddress{}, fmt.Errorf("agent provider route does not match catalog context")
		}
		return catalogHostRuntimeAddress{Workspace: parts[1], Agent: parts[2], Key: parts[3]}, nil
	}
	return catalogHostRuntimeAddress{}, fmt.Errorf("provider route is not exact")
}

func catalogClientEffectAccess(effect spec.ToolEffect) (string, int32, error) {
	switch effect {
	case spec.ToolEffectRead:
		return catalog.CatalogActionInvokeRead, 10, nil
	case spec.ToolEffectWrite:
		return catalog.CatalogActionInvokeWrite, 20, nil
	case spec.ToolEffectExecute:
		return catalog.CatalogActionInvokeExecute, 20, nil
	case spec.ToolEffectExternal:
		return catalog.CatalogActionInvokeExternal, 20, nil
	case spec.ToolEffectInteraction:
		return catalog.CatalogActionInvokeInteraction, 20, nil
	default:
		return "", 0, fmt.Errorf("aether catalog client: unsupported catalog effect %q", effect)
	}
}

func catalogClientInvocationContinuation(
	providerRoute string,
	catalogContext spec.ToolCatalogContext,
	bindingID string,
	profile *catalog.InvocationAuthorityProfile,
) (*pb.AuthorityContinuationRequest, error) {
	if profile == nil {
		return nil, nil
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if !isExactCatalogServiceTopic(providerRoute) && !isExactCatalogAgentTopic(providerRoute) {
		return nil, fmt.Errorf("caller OBO continuation requires an exact service or agent provider")
	}
	if catalogContext.WorkspaceID == "" || bindingID == "" {
		return nil, fmt.Errorf("caller OBO continuation requires workspace and call bindings")
	}
	resources := make([]*pb.ACLAuthorityGrantResourceScopeEntry, len(profile.ResourceScope))
	for i, resource := range profile.ResourceScope {
		resources[i] = &pb.ACLAuthorityGrantResourceScopeEntry{
			ResourceType: resource.ResourceType, Patterns: append([]string(nil), resource.Patterns...),
		}
	}
	return &pb.AuthorityContinuationRequest{
		ScopeMode: pb.AuthorityContinuationRequest_SCOPE_MODE_ATTENUATE,
		BindingId: bindingID,
		Scope: &pb.AuthorityContinuationScope{
			WorkspaceScope: []string{catalogContext.WorkspaceID}, ResourceScope: resources,
			OperationScope: append([]string(nil), profile.OperationScope...), MaxAccessLevel: profile.MaxAccessLevel,
		},
	}, nil
}
