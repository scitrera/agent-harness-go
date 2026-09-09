// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalogrpc

import (
	"fmt"
	"strings"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
)

const (
	catalogProviderResourceType = "tool-catalog/provider"
	catalogPublishOperation     = "catalog.publish"
	catalogMutationAccessLevel  = int32(20)
)

func isExactAgentRoute(route string) bool {
	parts := strings.Split(route, "::")
	if len(parts) != 4 || parts[0] != "ag" {
		return false
	}
	for _, part := range parts[1:] {
		if part == "" || part != strings.TrimSpace(part) || strings.ContainsAny(part, "*?[]") {
			return false
		}
	}
	return true
}

func validateDirectAgentMutationReceipt(caller Caller, method string, binding MutationBinding, sequence uint64) error {
	receipt := caller.AccessReceipt
	if receipt == nil || !receipt.GetAllowed() || receipt.GetDecision() != "ALLOW" {
		return fmt.Errorf("catalogrpc: direct agent mutation requires a gateway allow receipt")
	}
	resourceID, err := catalog.ProviderResourceID(binding.RequiredContext, binding.ProviderID)
	if err != nil {
		return fmt.Errorf("catalogrpc: derive provider resource: %w", err)
	}
	want := &pb.ResourceAccessRequest{
		ResourceType:        catalogProviderResourceType,
		ResourceId:          resourceID,
		Operation:           catalogPublishOperation,
		Workspace:           binding.RequiredContext.WorkspaceID,
		RequiredAccessLevel: catalogMutationAccessLevel,
		CorrelationId: catalog.MutationCorrelation(
			method, binding.ProviderID, binding.RegistrationID, binding.Generation, sequence,
		),
	}
	request := receipt.GetRequest()
	if request == nil || request.GetResourceType() != want.GetResourceType() ||
		request.GetResourceId() != want.GetResourceId() || request.GetOperation() != want.GetOperation() ||
		request.GetWorkspace() != want.GetWorkspace() ||
		request.GetRequiredAccessLevel() != want.GetRequiredAccessLevel() ||
		request.GetCorrelationId() != want.GetCorrelationId() {
		return fmt.Errorf("catalogrpc: direct agent mutation receipt does not match the exact provider action")
	}
	if caller.DeliveryTarget == "" || receipt.GetDeliveryTarget() != caller.DeliveryTarget {
		return fmt.Errorf("catalogrpc: direct agent mutation receipt targets another catalog service")
	}
	if receipt.GetExpiresAtMs() <= time.Now().UnixMilli() {
		return fmt.Errorf("catalogrpc: direct agent mutation receipt is expired")
	}
	if receipt.GetAuthorityMode() != "direct" || receipt.GetGrantId() != "" || receipt.GetSubject() != nil {
		return fmt.Errorf("catalogrpc: direct agent mutation receipt has unexpected delegated authority")
	}
	if receipt.GetActor() == nil || !strings.EqualFold(receipt.GetActor().GetPrincipalType(), "agent") ||
		receipt.GetActor().GetPrincipalId() != caller.SourceTopic {
		return fmt.Errorf("catalogrpc: direct agent mutation receipt actor is not an agent")
	}
	return nil
}
