package aether

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func (h *CatalogToolHost) admitInvocation(
	ctx context.Context,
	message *sdk.Message,
	envelope spec.ToolInvokeEnvelope,
) (catalogToolExport, tools.MemoryAuthority, error) {
	if err := envelope.Validate(); err != nil {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: invalid invocation: %w", err)
	}
	if envelope.ToolRef == nil {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: exact tool reference is required")
	}
	if envelope.Addr.WorkspaceID != h.catalogContext.WorkspaceID {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: invocation workspace does not match the publication")
	}
	if h.catalogContext.ThreadID != "" && envelope.Addr.ThreadID != h.catalogContext.ThreadID {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: invocation thread does not match the publication")
	}
	if envelope.Addr.TaskID == "" {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: invocation task id is required")
	}
	exported, ok := h.exports[catalogToolReferenceKey(*envelope.ToolRef)]
	if !ok || exported.entry.Ref.Name != envelope.Name {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: exact tool reference is not exported by this generation")
	}
	if !isExactCatalogAgentTopic(message.SourceTopic) {
		return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: invocation source must be an exact agent")
	}
	if err := h.validateInvocationReceipt(message, exported, envelope.CallID); err != nil {
		return catalogToolExport{}, tools.MemoryAuthority{}, err
	}
	authority, err := h.validateForwardedInvocationAuthority(message, exported, envelope.CallID)
	if err != nil {
		return catalogToolExport{}, tools.MemoryAuthority{}, err
	}
	if h.callerAuthorizer != nil {
		if err := h.callerAuthorizer.AuthorizeCatalogToolCaller(ctx, CatalogToolCallerRequest{
			SourceTopic: message.SourceTopic, Subject: authority,
			Context: h.catalogContext, Entry: exported.entry,
		}); err != nil {
			return catalogToolExport{}, tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: caller policy denied invocation: %w", err)
		}
	}
	return exported, authority, nil
}

func (h *CatalogToolHost) validateInvocationReceipt(message *sdk.Message, exported catalogToolExport, callID string) error {
	receipt := message.AccessReceipt
	if receipt == nil || !receipt.GetAllowed() || receipt.GetDecision() != "ALLOW" {
		return fmt.Errorf("aether catalog tool host: gateway allow receipt is required")
	}
	resourceID, err := catalog.EntryResourceID(h.catalogContext, exported.entry.Ref)
	if err != nil {
		return fmt.Errorf("aether catalog tool host: derive entry resource: %w", err)
	}
	operation, requiredAccess, err := catalogToolEffectAccess(exported.entry.Effect)
	if err != nil {
		return err
	}
	want := &pb.ResourceAccessRequest{
		ResourceType: "tool-catalog/entry", ResourceId: resourceID,
		Operation: operation, Workspace: h.catalogContext.WorkspaceID,
		RequiredAccessLevel: requiredAccess, CorrelationId: callID,
	}
	request := receipt.GetRequest()
	if request == nil || request.GetResourceType() != want.GetResourceType() ||
		request.GetResourceId() != want.GetResourceId() || request.GetOperation() != want.GetOperation() ||
		request.GetWorkspace() != want.GetWorkspace() ||
		request.GetRequiredAccessLevel() != want.GetRequiredAccessLevel() ||
		request.GetCorrelationId() != want.GetCorrelationId() {
		return fmt.Errorf("aether catalog tool host: gateway receipt does not match the exact catalog entry")
	}
	if receipt.GetEffectiveAccessLevel() != 0 && receipt.GetEffectiveAccessLevel() < requiredAccess {
		return fmt.Errorf("aether catalog tool host: gateway receipt effective access is insufficient")
	}
	if receipt.GetDeliveryTarget() != h.route {
		return fmt.Errorf("aether catalog tool host: gateway receipt targets another provider")
	}
	if receipt.GetExpiresAtMs() <= h.now().UnixMilli() {
		return fmt.Errorf("aether catalog tool host: gateway receipt is expired")
	}
	if receipt.GetAuthorityMode() != "on_behalf_of" || receipt.GetGrantId() == "" || receipt.GetRootGrantId() == "" {
		return fmt.Errorf("aether catalog tool host: invocation requires checked caller OBO authority")
	}
	if receipt.GetActor() == nil || !strings.EqualFold(receipt.GetActor().GetPrincipalType(), "agent") ||
		receipt.GetActor().GetPrincipalId() != message.SourceTopic {
		return fmt.Errorf("aether catalog tool host: gateway receipt actor does not match the source agent")
	}
	if !samePrincipal(receipt.GetSubject(), message.OnBehalfSubject) || message.OnBehalfSubject == nil ||
		!strings.EqualFold(message.OnBehalfSubject.GetPrincipalType(), "user") {
		return fmt.Errorf("aether catalog tool host: gateway OBO subject changed")
	}
	return nil
}

func (h *CatalogToolHost) validateForwardedInvocationAuthority(
	message *sdk.Message,
	exported catalogToolExport,
	callID string,
) (tools.MemoryAuthority, error) {
	if exported.profile == nil {
		if message.ForwardedAuthorization != nil {
			return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: unreviewed forwarded authority was attached")
		}
		return catalogToolMessageSubject(message)
	}
	forwarded := message.ForwardedAuthorization
	if forwarded == nil {
		return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: reviewed caller OBO continuation is required")
	}
	if forwarded.GetDeliveryTarget() != h.route || forwarded.GetBindingId() != callID {
		return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: forwarded authority is bound to another invocation or provider")
	}
	if forwarded.GetExpiresAtMs() <= h.now().UnixMilli() {
		return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: forwarded authority is expired")
	}
	if forwarded.GetRootGrantId() == "" || forwarded.GetRootGrantId() != message.AccessReceipt.GetRootGrantId() {
		return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: forwarded authority lineage changed")
	}
	authorization := forwarded.GetAuthorization()
	if authorization == nil || authorization.GetAuthorityMode() != "on_behalf_of" || authorization.GetGrantId() == "" ||
		!samePrincipal(authorization.GetSubject(), message.OnBehalfSubject) {
		return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: forwarded authorization context is invalid")
	}
	if err := validateCatalogAuthorityScope(forwarded.GetScope(), h.catalogContext.WorkspaceID, *exported.profile); err != nil {
		return tools.MemoryAuthority{}, err
	}
	return tools.MemoryAuthority{
		GrantID:     authorization.GetGrantId(),
		SubjectType: strings.ToLower(authorization.GetSubject().GetPrincipalType()),
		SubjectID:   authorization.GetSubject().GetPrincipalId(),
	}, nil
}

func catalogToolMessageSubject(message *sdk.Message) (tools.MemoryAuthority, error) {
	if message == nil || message.OnBehalfSubject == nil ||
		!strings.EqualFold(message.OnBehalfSubject.GetPrincipalType(), "user") ||
		message.OnBehalfSubject.GetPrincipalId() == "" {
		return tools.MemoryAuthority{}, fmt.Errorf("aether catalog tool host: gateway user subject is required")
	}
	return tools.MemoryAuthority{
		SubjectType: "user", SubjectID: message.OnBehalfSubject.GetPrincipalId(),
	}, nil
}

func samePrincipal(left, right *pb.PrincipalRef) bool {
	return left != nil && right != nil &&
		strings.EqualFold(left.GetPrincipalType(), right.GetPrincipalType()) &&
		left.GetPrincipalId() == right.GetPrincipalId()
}

func catalogToolEffectAccess(effect spec.ToolEffect) (string, int32, error) {
	action, err := catalog.InvocationCatalogAction(effect)
	if err != nil {
		return "", 0, fmt.Errorf("aether catalog tool host: %w", err)
	}
	if effect == spec.ToolEffectRead {
		return action, 10, nil
	}
	return action, 20, nil
}

func validateCatalogAuthorityScope(
	actual *pb.AuthorityContinuationScope,
	workspace string,
	expected catalog.InvocationAuthorityProfile,
) error {
	if actual == nil || len(actual.GetWorkspaceScope()) != 1 || actual.GetWorkspaceScope()[0] != workspace {
		return fmt.Errorf("aether catalog tool host: forwarded authority has the wrong workspace scope")
	}
	if actual.GetMaxAccessLevel() != expected.MaxAccessLevel {
		return fmt.Errorf("aether catalog tool host: forwarded authority has the wrong access ceiling")
	}
	if !sameStringSet(actual.GetOperationScope(), expected.OperationScope) {
		return fmt.Errorf("aether catalog tool host: forwarded authority has the wrong operation scope")
	}
	actualResources := make(map[string][]string, len(actual.GetResourceScope()))
	for _, resource := range actual.GetResourceScope() {
		if resource == nil {
			return fmt.Errorf("aether catalog tool host: forwarded authority has a malformed resource scope")
		}
		if _, duplicate := actualResources[resource.GetResourceType()]; duplicate {
			return fmt.Errorf("aether catalog tool host: forwarded authority repeats a resource scope")
		}
		actualResources[resource.GetResourceType()] = resource.GetPatterns()
	}
	if len(actualResources) != len(expected.ResourceScope) {
		return fmt.Errorf("aether catalog tool host: forwarded authority has the wrong resource scope")
	}
	for _, resource := range expected.ResourceScope {
		if !sameStringSet(actualResources[resource.ResourceType], resource.Patterns) {
			return fmt.Errorf("aether catalog tool host: forwarded authority has the wrong %s resource scope", resource.ResourceType)
		}
	}
	return nil
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
