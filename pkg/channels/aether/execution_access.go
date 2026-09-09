// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"errors"
	"fmt"
	"time"

	pb "github.com/scitrera/aether/api/proto"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// ExecutionBindingAccessRequest constructs the one canonical Aether check for
// admitting a workspace scope. Read/write remains an access-level ceiling on
// the stable bind operation.
func ExecutionBindingAccessRequest(scope workspacepkg.ExecutionScope, correlationID string) (*pb.ResourceAccessRequest, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if correlationID == "" {
		return nil, errors.New("workspace binding access: correlation id is required")
	}
	resourceID, err := workspacepkg.ExecutionViewResourceID(scope.Binding)
	if err != nil {
		return nil, fmt.Errorf("workspace binding access: derive resource: %w", err)
	}
	requiredAccess, err := workspacepkg.ExecutionViewRequiredAccess(scope.Policy)
	if err != nil {
		return nil, fmt.Errorf("workspace binding access: derive level: %w", err)
	}
	return &pb.ResourceAccessRequest{
		ResourceType: workspacepkg.ExecutionViewResourceType,
		ResourceId:   resourceID, Operation: workspacepkg.ExecutionViewBindOperation,
		Workspace: scope.Binding.WorkspaceID, RequiredAccessLevel: requiredAccess,
		CorrelationId: correlationID,
	}, nil
}

// ValidateExecutionBindingAccessReceipt compares gateway-authored transport
// metadata with the exact scope the application decoded. Payload copies of a
// receipt never reach this function.
func ValidateExecutionBindingAccessReceipt(
	receipt *pb.AccessDecisionReceipt,
	onBehalfSubject *pb.PrincipalRef,
	scope workspacepkg.ExecutionScope,
	correlationID string,
	deliveryTarget string,
	now time.Time,
) error {
	if receipt == nil || !receipt.GetAllowed() || receipt.GetDecision() != "ALLOW" {
		return errors.New("workspace binding access: gateway allow receipt is required")
	}
	want, err := ExecutionBindingAccessRequest(scope, correlationID)
	if err != nil {
		return err
	}
	request := receipt.GetRequest()
	if request == nil || request.GetResourceType() != want.GetResourceType() ||
		request.GetResourceId() != want.GetResourceId() || request.GetOperation() != want.GetOperation() ||
		request.GetWorkspace() != want.GetWorkspace() ||
		request.GetRequiredAccessLevel() != want.GetRequiredAccessLevel() ||
		request.GetCorrelationId() != want.GetCorrelationId() {
		return errors.New("workspace binding access: gateway receipt does not match the exact view")
	}
	if deliveryTarget == "" || receipt.GetDeliveryTarget() != deliveryTarget {
		return errors.New("workspace binding access: gateway receipt targets another executor")
	}
	if receipt.GetExpiresAtMs() <= now.UnixMilli() {
		return errors.New("workspace binding access: gateway receipt is expired")
	}
	if receipt.GetAuthorityMode() == "on_behalf_of" && onBehalfSubject == nil {
		return errors.New("workspace binding access: gateway OBO subject is missing from the delivery")
	}
	if onBehalfSubject != nil {
		if receipt.GetAuthorityMode() != "on_behalf_of" || receipt.GetGrantId() == "" ||
			receipt.GetSubject() == nil ||
			receipt.GetSubject().GetPrincipalType() != onBehalfSubject.GetPrincipalType() ||
			receipt.GetSubject().GetPrincipalId() != onBehalfSubject.GetPrincipalId() {
			return errors.New("workspace binding access: gateway OBO subject changed")
		}
	}
	return nil
}
