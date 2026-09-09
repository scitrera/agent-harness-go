// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/catalog/catalogrpc"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	defaultCatalogToolLease         = 5 * time.Minute
	defaultCatalogToolRenew         = 2 * time.Minute
	defaultCatalogToolRPCTimeout    = 30 * time.Second
	defaultCatalogServiceTopic      = "sv::tool-catalog"
	catalogToolInvokeArgument       = "tool.invoke"
	catalogToolCancelArgument       = "tool.cancel"
	catalogToolResultArgument       = "tool.result"
	catalogToolProviderResourceType = "tool-catalog/provider"
	catalogToolPublishOperation     = "catalog.publish"
	catalogToolMutationAccessLevel  = int32(20)
)

// CatalogToolExport is an explicit allowlist entry for one registry tool.
// Revision identifies the immutable implementation reviewed by catalog policy;
// InvocationAuthority is the host-local copy of the maximum server-owned OBO
// profile and is validated against gateway metadata before execution.
type CatalogToolExport struct {
	Name                string
	Revision            string
	Effect              spec.ToolEffect
	InvocationAuthority *catalog.InvocationAuthorityProfile
}

// CatalogToolCallerRequest is transport-authenticated input to an optional
// deployment-local caller policy. Aether's exact entry decision remains the
// mandatory outer gate even when this hook is nil.
type CatalogToolCallerRequest struct {
	SourceTopic string
	Subject     tools.MemoryAuthority
	Context     spec.ToolCatalogContext
	Entry       spec.ToolCatalogEntry
}

type CatalogToolCallerAuthorizer interface {
	AuthorizeCatalogToolCaller(context.Context, CatalogToolCallerRequest) error
}

type CatalogToolCallerAuthorizerFunc func(context.Context, CatalogToolCallerRequest) error

func (f CatalogToolCallerAuthorizerFunc) AuthorizeCatalogToolCaller(ctx context.Context, request CatalogToolCallerRequest) error {
	return f(ctx, request)
}

type CatalogToolMessageSender interface {
	SendWithOptions(sdk.SendMessageOptions) error
}

// CatalogToolHostConfig configures one exact Aether agent as a catalog
// provider. Context is availability only and may be workspace-wide; Route is
// stored separately as authenticated operational state by the catalog.
type CatalogToolHostConfig struct {
	Route                 string
	CatalogServiceTopic   string
	ProviderID            string
	RegistrationID        string
	Generation            string
	GenerationReplacement bool
	Context               spec.ToolCatalogContext
	Registry              *tools.Registry
	Exports               []CatalogToolExport
	Sender                CatalogToolMessageSender
	CallerAuthorizer      CatalogToolCallerAuthorizer
	LeaseDuration         time.Duration
	RenewInterval         time.Duration
	RPCTimeout            time.Duration
	Now                   func() time.Time
}

type catalogToolExport struct {
	entry   spec.ToolCatalogEntry
	profile *catalog.InvocationAuthorityProfile
}

type catalogToolCallKey struct {
	source string
	callID string
}

type activeCatalogToolCall struct {
	cancel  context.CancelFunc
	addr    spec.MessageAddress
	subject tools.MemoryAuthority
	export  catalogToolExport
}

type catalogToolRPCReply struct {
	result json.RawMessage
	err    error
}

// CatalogToolHost executes an explicitly exported subset of one registry,
// publishes it under a bounded lease, and validates every invocation and
// cancellation against gateway-authored metadata.
type CatalogToolHost struct {
	route                 string
	catalogServiceTopic   string
	providerID            string
	registrationID        string
	generation            string
	generationReplacement bool
	catalogContext        spec.ToolCatalogContext
	registry              *tools.Registry
	sender                CatalogToolMessageSender
	callerAuthorizer      CatalogToolCallerAuthorizer
	leaseDuration         time.Duration
	renewInterval         time.Duration
	rpcTimeout            time.Duration
	now                   func() time.Time
	entries               []spec.ToolCatalogEntry
	exports               map[string]catalogToolExport

	mutationMu sync.Mutex
	sequence   uint64
	published  bool

	mu      sync.Mutex
	waiters map[string]chan catalogToolRPCReply
	active  map[catalogToolCallKey]*activeCatalogToolCall
}

func NewCatalogToolHost(cfg CatalogToolHostConfig) (*CatalogToolHost, error) {
	if !isExactCatalogAgentTopic(cfg.Route) {
		return nil, fmt.Errorf("aether catalog tool host: route must be an exact agent topic")
	}
	parts := strings.Split(cfg.Route, "::")
	if err := cfg.Context.Validate(); err != nil {
		return nil, fmt.Errorf("aether catalog tool host: invalid context: %w", err)
	}
	if cfg.Context.WorkspaceID == "" || cfg.Context.WorkspaceID != parts[1] {
		return nil, fmt.Errorf("aether catalog tool host: context workspace must match the agent route")
	}
	if cfg.Context.ToolHostID != "" && cfg.Context.ToolHostID != cfg.Route {
		return nil, fmt.Errorf("aether catalog tool host: a tool_host_id selector must match the exact provider route")
	}
	if cfg.Registry == nil || cfg.Sender == nil {
		return nil, fmt.Errorf("aether catalog tool host: registry and sender are required")
	}
	if cfg.CatalogServiceTopic == "" {
		cfg.CatalogServiceTopic = defaultCatalogServiceTopic
	}
	if !validCatalogServiceTopic(cfg.CatalogServiceTopic) {
		return nil, fmt.Errorf("aether catalog tool host: catalog service topic must be sv::tool-catalog or an exact instance")
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultCatalogToolLease
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = defaultCatalogToolRenew
	}
	if cfg.RenewInterval >= cfg.LeaseDuration {
		return nil, fmt.Errorf("aether catalog tool host: renew interval must be shorter than the lease")
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = defaultCatalogToolRPCTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	entries, exports, err := buildCatalogToolExports(cfg)
	if err != nil {
		return nil, err
	}
	host := &CatalogToolHost{
		route: cfg.Route, catalogServiceTopic: cfg.CatalogServiceTopic,
		providerID: cfg.ProviderID, registrationID: cfg.RegistrationID, generation: cfg.Generation,
		generationReplacement: cfg.GenerationReplacement, catalogContext: cfg.Context,
		registry: cfg.Registry, sender: cfg.Sender, callerAuthorizer: cfg.CallerAuthorizer,
		leaseDuration: cfg.LeaseDuration, renewInterval: cfg.RenewInterval,
		rpcTimeout: cfg.RPCTimeout, now: cfg.Now, entries: entries, exports: exports,
		waiters: make(map[string]chan catalogToolRPCReply),
		active:  make(map[catalogToolCallKey]*activeCatalogToolCall),
	}
	return host, nil
}

func buildCatalogToolExports(cfg CatalogToolHostConfig) ([]spec.ToolCatalogEntry, map[string]catalogToolExport, error) {
	descriptors := make(map[string]tools.Descriptor)
	for _, descriptor := range cfg.Registry.Descriptors() {
		descriptors[descriptor.Name] = descriptor
	}
	entries := make([]spec.ToolCatalogEntry, 0, len(cfg.Exports))
	exports := make(map[string]catalogToolExport, len(cfg.Exports))
	seenNames := make(map[string]struct{}, len(cfg.Exports))
	for _, requested := range cfg.Exports {
		if _, duplicate := seenNames[requested.Name]; duplicate {
			return nil, nil, fmt.Errorf("aether catalog tool host: duplicate export %q", requested.Name)
		}
		seenNames[requested.Name] = struct{}{}
		descriptor, exists := descriptors[requested.Name]
		if !exists {
			return nil, nil, fmt.Errorf("aether catalog tool host: exported tool %q is not registered", requested.Name)
		}
		if strings.TrimSpace(descriptor.Description) == "" {
			return nil, nil, fmt.Errorf("aether catalog tool host: exported tool %q requires a description", requested.Name)
		}
		inputSchema := map[string]json.RawMessage{}
		parameters := descriptor.Parameters
		if len(parameters) == 0 {
			parameters = json.RawMessage(`{"type":"object"}`)
		}
		if err := json.Unmarshal(parameters, &inputSchema); err != nil {
			return nil, nil, fmt.Errorf("aether catalog tool host: invalid schema for %q: %w", requested.Name, err)
		}
		kind := strings.TrimSpace(descriptor.CatalogKind)
		if kind == "" {
			kind = "remote"
		}
		entry := spec.ToolCatalogEntry{
			Ref: spec.ToolReference{
				ProviderID: cfg.ProviderID, RegistrationID: cfg.RegistrationID,
				Generation: cfg.Generation, Name: requested.Name, Revision: requested.Revision,
			},
			Descriptor: spec.ToolDescriptor{
				Name: requested.Name, Description: descriptor.Description,
				InputSchema: inputSchema, Kind: kind, AwaitsResult: true,
			},
			Effect: requested.Effect,
		}
		if err := entry.Validate(); err != nil {
			return nil, nil, fmt.Errorf("aether catalog tool host: invalid export %q: %w", requested.Name, err)
		}
		var profile *catalog.InvocationAuthorityProfile
		if requested.InvocationAuthority != nil {
			if err := requested.InvocationAuthority.Validate(); err != nil {
				return nil, nil, fmt.Errorf("aether catalog tool host: invalid authority profile for %q: %w", requested.Name, err)
			}
			cloned := cloneCatalogInvocationAuthority(*requested.InvocationAuthority)
			profile = &cloned
		}
		entries = append(entries, entry)
		exports[catalogToolReferenceKey(entry.Ref)] = catalogToolExport{entry: entry, profile: profile}
	}
	if len(entries) == 0 {
		return nil, nil, fmt.Errorf("aether catalog tool host: at least one explicit export is required")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Ref.Name < entries[j].Ref.Name })
	return entries, exports, nil
}

func cloneCatalogInvocationAuthority(profile catalog.InvocationAuthorityProfile) catalog.InvocationAuthorityProfile {
	out := profile
	out.OperationScope = append([]string(nil), profile.OperationScope...)
	out.ResourceScope = make([]catalog.InvocationAuthorityResourceScope, len(profile.ResourceScope))
	for i, resource := range profile.ResourceScope {
		out.ResourceScope[i] = catalog.InvocationAuthorityResourceScope{
			ResourceType: resource.ResourceType, Patterns: append([]string(nil), resource.Patterns...),
		}
	}
	return out
}

func validCatalogServiceTopic(topic string) bool {
	parts := strings.Split(topic, "::")
	if len(parts) != 2 && len(parts) != 3 {
		return false
	}
	if parts[0] != "sv" || parts[1] != "tool-catalog" {
		return false
	}
	return len(parts) == 2 || exactCatalogTopicParts(parts[2])
}

func catalogToolReferenceKey(ref spec.ToolReference) string {
	return strings.Join([]string{ref.ProviderID, ref.RegistrationID, ref.Generation, ref.Name, ref.Revision}, "\x00")
}

func (h *CatalogToolHost) publication(sequence uint64) spec.ToolCatalogPublication {
	return spec.ToolCatalogPublication{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		ProviderID:    h.providerID, RegistrationID: h.registrationID, Generation: h.generation,
		Sequence: sequence, LeaseExpiresAt: h.now().UTC().Add(h.leaseDuration).Format(time.RFC3339Nano),
		Context:      h.catalogContext,
		Capabilities: []string{spec.ToolCancelCapability, spec.ToolReferenceCapability},
		Entries:      append([]spec.ToolCatalogEntry(nil), h.entries...),
	}
}

func (h *CatalogToolHost) Publish(ctx context.Context) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.published {
		return fmt.Errorf("aether catalog tool host: provider is already published")
	}
	h.sequence++
	publication := h.publication(h.sequence)
	request := catalogrpc.PublishRequest{
		Binding: h.mutationBinding(), Publication: publication,
	}
	if _, err := h.callCatalog(ctx, catalogrpc.MethodPublish, request, h.sequence); err != nil {
		return err
	}
	h.published = true
	return nil
}

func (h *CatalogToolHost) Renew(ctx context.Context) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if !h.published {
		return fmt.Errorf("aether catalog tool host: provider is not published")
	}
	h.sequence++
	request := catalogrpc.RenewRequest{
		Binding: h.mutationBinding(),
		Request: spec.ToolCatalogRenewRequest{
			SchemaVersion: spec.ToolCatalogSchemaVersion,
			ProviderID:    h.providerID, RegistrationID: h.registrationID, Generation: h.generation,
			Sequence:       h.sequence,
			LeaseExpiresAt: h.now().UTC().Add(h.leaseDuration).Format(time.RFC3339Nano),
		},
	}
	_, err := h.callCatalog(ctx, catalogrpc.MethodRenew, request, h.sequence)
	return err
}

func (h *CatalogToolHost) Revoke(ctx context.Context) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if !h.published {
		return nil
	}
	h.sequence++
	request := catalogrpc.RevokeRequest{
		Binding: h.mutationBinding(),
		Request: spec.ToolCatalogRevokeRequest{
			SchemaVersion: spec.ToolCatalogSchemaVersion,
			ProviderID:    h.providerID, RegistrationID: h.registrationID, Generation: h.generation,
			Sequence: h.sequence,
		},
	}
	if _, err := h.callCatalog(ctx, catalogrpc.MethodRevoke, request, h.sequence); err != nil {
		return err
	}
	h.published = false
	return nil
}

func (h *CatalogToolHost) mutationBinding() catalogrpc.MutationBinding {
	return catalogrpc.MutationBinding{
		ProviderID: h.providerID, RegistrationID: h.registrationID, Generation: h.generation,
		ProviderRoute: h.route, RequiredContext: h.catalogContext,
		GenerationReplacement: h.generationReplacement,
	}
}

// RunLease publishes, renews until ctx ends, and makes one bounded best-effort
// revoke. The catalog lease remains the crash fallback.
func (h *CatalogToolHost) RunLease(ctx context.Context) error {
	if err := h.Publish(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(h.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.rpcTimeout)
			defer cancel()
			if err := h.Revoke(revokeCtx); err != nil {
				return errors.Join(fmt.Errorf("aether catalog tool host: graceful revoke: %w", err), ctx.Err())
			}
			return nil
		case <-ticker.C:
			if err := h.Renew(ctx); err != nil {
				return err
			}
		}
	}
}

func (h *CatalogToolHost) callCatalog(ctx context.Context, method string, request any, sequence uint64) (json.RawMessage, error) {
	requestRaw, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("aether catalog tool host: encode %s: %w", method, err)
	}
	requestID := catalog.MutationCorrelation(method, h.providerID, h.registrationID, h.generation, sequence)
	envelope := catalogHostRuntimeEnvelope{
		Source:    catalogHostRuntimeAddress{Workspace: h.catalogContext.WorkspaceID},
		Content:   catalogHostRuntimeContent{Role: "agent"},
		Arguments: map[string]json.RawMessage{method: requestRaw}, RequestID: requestID,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("aether catalog tool host: encode catalog envelope: %w", err)
	}
	resourceID, err := catalog.ProviderResourceID(h.catalogContext, h.providerID)
	if err != nil {
		return nil, fmt.Errorf("aether catalog tool host: derive provider resource: %w", err)
	}
	waiter := make(chan catalogToolRPCReply, 1)
	h.mu.Lock()
	if _, exists := h.waiters[requestID]; exists {
		h.mu.Unlock()
		return nil, fmt.Errorf("aether catalog tool host: duplicate catalog request %q", requestID)
	}
	h.waiters[requestID] = waiter
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if h.waiters[requestID] == waiter {
			delete(h.waiters, requestID)
		}
		h.mu.Unlock()
	}()
	if err := h.sender.SendWithOptions(sdk.SendMessageOptions{
		TargetTopic: h.catalogServiceTopic, Payload: payload, MessageType: sdk.MessageTypeChat,
		CheckedAccess: &pb.ResourceAccessRequest{
			ResourceType: catalogToolProviderResourceType, ResourceId: resourceID,
			Operation: catalogToolPublishOperation, Workspace: h.catalogContext.WorkspaceID,
			RequiredAccessLevel: catalogToolMutationAccessLevel, CorrelationId: requestID,
		},
	}); err != nil {
		return nil, fmt.Errorf("aether catalog tool host: send %s: %w", method, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, h.rpcTimeout)
	defer cancel()
	select {
	case reply := <-waiter:
		return reply.result, reply.err
	case <-waitCtx.Done():
		return nil, fmt.Errorf("aether catalog tool host: %s: %w", method, waitCtx.Err())
	}
}

// Handle is registered as the agent client's catch-all OnMessage handler so it
// can receive both catalog CHAT replies and TOOL_CALL invocations.
func (h *CatalogToolHost) Handle(ctx context.Context, message *sdk.Message) error {
	if message == nil || message.SourceTopic == "" || len(message.Payload) == 0 {
		return nil
	}
	var envelope catalogHostRuntimeEnvelope
	if err := json.Unmarshal(message.Payload, &envelope); err != nil {
		return nil
	}
	if h.deliverCatalogReply(message.SourceTopic, envelope) {
		return nil
	}
	if message.MessageType != pb.MessageType_TOOL_CALL || len(envelope.Arguments) != 1 {
		return nil
	}
	if raw, ok := envelope.Arguments[catalogToolInvokeArgument]; ok {
		var invocation spec.ToolInvokeEnvelope
		if err := json.Unmarshal(raw, &invocation); err != nil {
			return nil
		}
		copyMessage := *message
		copyMessage.Payload = append([]byte(nil), message.Payload...)
		go h.executeInvocation(context.WithoutCancel(ctx), &copyMessage, envelope, invocation)
		return nil
	}
	if raw, ok := envelope.Arguments[catalogToolCancelArgument]; ok {
		var cancellation spec.ToolCancelEnvelope
		if err := json.Unmarshal(raw, &cancellation); err == nil && cancellation.Validate() == nil {
			h.cancelInvocation(message, cancellation)
		}
	}
	return nil
}

func (h *CatalogToolHost) deliverCatalogReply(source string, envelope catalogHostRuntimeEnvelope) bool {
	requestID := envelope.RequestID
	if requestID == "" {
		requestID = envelope.Target.RequestID
	}
	if requestID == "" || !h.catalogReplySourceMatches(source) {
		return false
	}
	h.mu.Lock()
	waiter := h.waiters[requestID]
	if waiter != nil {
		delete(h.waiters, requestID)
	}
	h.mu.Unlock()
	if waiter == nil {
		return false
	}
	reply := catalogToolRPCReply{result: envelope.Arguments["result"]}
	if raw := envelope.Arguments["error"]; len(raw) > 0 {
		var protocolError spec.ToolCatalogError
		if err := json.Unmarshal(raw, &protocolError); err != nil {
			reply.err = fmt.Errorf("aether catalog tool host: catalog error: %s", raw)
		} else {
			reply.err = fmt.Errorf("aether catalog tool host: catalog %s: %s", protocolError.Code, protocolError.Message)
		}
	}
	waiter <- reply
	return true
}

func (h *CatalogToolHost) catalogReplySourceMatches(source string) bool {
	if strings.Count(h.catalogServiceTopic, "::") == 2 {
		return source == h.catalogServiceTopic
	}
	return strings.HasPrefix(source, h.catalogServiceTopic+"::") && isExactCatalogServiceTopic(source)
}

func isExactCatalogAgentTopic(topic string) bool {
	parts := strings.Split(topic, "::")
	return len(parts) == 4 && parts[0] == "ag" && exactCatalogTopicParts(parts[1:]...)
}

func isExactCatalogServiceTopic(topic string) bool {
	parts := strings.Split(topic, "::")
	return len(parts) == 3 && parts[0] == "sv" && exactCatalogTopicParts(parts[1:]...)
}

func exactCatalogTopicParts(parts ...string) bool {
	for _, part := range parts {
		if part == "" || part != strings.TrimSpace(part) || strings.ContainsAny(part, "*?[]") {
			return false
		}
	}
	return true
}

func (h *CatalogToolHost) executeInvocation(
	ctx context.Context,
	message *sdk.Message,
	runtimeEnvelope catalogHostRuntimeEnvelope,
	envelope spec.ToolInvokeEnvelope,
) {
	var result tools.Result
	var invokeErr error
	exported, authority, err := h.admitInvocation(ctx, message, envelope)
	if err != nil {
		invokeErr = err
	} else {
		key := catalogToolCallKey{source: message.SourceTopic, callID: envelope.CallID}
		callCtx, cancel := context.WithCancel(ctx)
		active := &activeCatalogToolCall{
			cancel: cancel, addr: envelope.Addr, subject: authority, export: exported,
		}
		h.mu.Lock()
		if _, duplicate := h.active[key]; duplicate {
			h.mu.Unlock()
			cancel()
			invokeErr = fmt.Errorf("aether catalog tool host: duplicate active call %q", envelope.CallID)
		} else {
			h.active[key] = active
			h.mu.Unlock()
			request := tools.RequestFromEnvelope(envelope)
			request.Authority = authority
			callCtx = tools.WithMemoryAuthority(tools.WithoutToolDelegate(callCtx), authority)
			result, invokeErr = h.registry.Invoke(callCtx, request)
			if callCtx.Err() != nil {
				invokeErr = callCtx.Err()
			}
			h.mu.Lock()
			if h.active[key] == active {
				delete(h.active, key)
			}
			h.mu.Unlock()
			cancel()
		}
	}
	h.replyInvocation(message.SourceTopic, runtimeEnvelope, envelope, result, invokeErr)
}

func (h *CatalogToolHost) cancelInvocation(message *sdk.Message, envelope spec.ToolCancelEnvelope) {
	key := catalogToolCallKey{source: message.SourceTopic, callID: envelope.CallID}
	h.mu.Lock()
	active := h.active[key]
	h.mu.Unlock()
	if active == nil || !sameToolCallAddress(active.addr, envelope.Addr) {
		return
	}
	if err := h.validateInvocationReceipt(message, active.export, envelope.CallID); err != nil {
		return
	}
	subject, err := catalogToolMessageSubject(message)
	if err != nil || subject.SubjectType != active.subject.SubjectType || subject.SubjectID != active.subject.SubjectID {
		return
	}
	active.cancel()
}

func (h *CatalogToolHost) replyInvocation(
	target string,
	request catalogHostRuntimeEnvelope,
	envelope spec.ToolInvokeEnvelope,
	result tools.Result,
	invokeErr error,
) {
	body := toolResultBody(envelope, result, invokeErr)
	raw, err := json.Marshal(body)
	if err != nil {
		return
	}
	reply := catalogHostRuntimeEnvelope{
		Source: catalogHostRuntimeAddress{Workspace: h.catalogContext.WorkspaceID},
		Target: request.Source, Content: catalogHostRuntimeContent{Role: "assistant", End: true},
		Arguments: map[string]json.RawMessage{catalogToolResultArgument: raw},
		RequestID: envelope.CallID, Initiator: request.Initiator,
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return
	}
	_ = h.sender.SendWithOptions(sdk.SendMessageOptions{
		TargetTopic: target, Payload: payload, MessageType: sdk.MessageTypeChat,
	})
}

type catalogHostRuntimeEnvelope struct {
	Source    catalogHostRuntimeAddress  `json:"source"`
	Content   catalogHostRuntimeContent  `json:"content"`
	Arguments map[string]json.RawMessage `json:"arguments,omitempty"`
	Target    catalogHostRuntimeAddress  `json:"target,omitempty"`
	RequestID string                     `json:"request_id,omitempty"`
	Initiator *catalogHostRuntimeAddress `json:"initiator,omitempty"`
}

type catalogHostRuntimeAddress struct {
	Tenant    string `json:"tenant,omitempty"`
	Workspace string `json:"workspace"`
	User      string `json:"user,omitempty"`
	Agent     string `json:"agent,omitempty"`
	Key       string `json:"key,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type catalogHostRuntimeContent struct {
	Text string `json:"text,omitempty"`
	End  bool   `json:"end"`
	Role string `json:"role"`
}
