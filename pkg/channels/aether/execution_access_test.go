package aether

import (
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

func testBindingScope(t *testing.T, access workspacepkg.ViewWriteAccess) workspacepkg.ExecutionScope {
	t.Helper()
	binding := spec.NewExecutionBinding("project-a", "view-a", "us::alice::window-1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-a"
	scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{WriteAccess: access})
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestExecutionBindingAccessRequestAndReceiptAreExact(t *testing.T) {
	scope := testBindingScope(t, workspacepkg.ViewWriteAccessReadWrite)
	request, err := ExecutionBindingAccessRequest(scope, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if request.GetOperation() != "bind" || request.GetRequiredAccessLevel() != 20 ||
		request.GetResourceId() != "workspaces/project-a/views/view-a/hosts/us%3A%3Aalice%3A%3Awindow-1" {
		t.Fatalf("request = %+v", request)
	}
	now := time.Now()
	receipt := &pb.AccessDecisionReceipt{
		Allowed: true, Decision: "ALLOW", Request: request,
		DeliveryTarget: "ag::default::sahara::one", ExpiresAtMs: now.Add(time.Minute).UnixMilli(),
	}
	if err := ValidateExecutionBindingAccessReceipt(receipt, nil, scope, "task-a", "ag::default::sahara::one", now); err != nil {
		t.Fatal(err)
	}
	receipt.Request.RequiredAccessLevel = 10
	if err := ValidateExecutionBindingAccessReceipt(receipt, nil, scope, "task-a", "ag::default::sahara::one", now); err == nil {
		t.Fatal("read receipt admitted a read-write scope")
	}
}

func TestExecutionBindingAccessReceiptRequiresTrustedOBOSubject(t *testing.T) {
	scope := testBindingScope(t, workspacepkg.ViewWriteAccessReadOnly)
	request, err := ExecutionBindingAccessRequest(scope, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	receipt := &pb.AccessDecisionReceipt{
		Allowed: true, Decision: "ALLOW", Request: request,
		DeliveryTarget: "ag::default::sahara::one", ExpiresAtMs: now.Add(time.Minute).UnixMilli(),
		AuthorityMode: "on_behalf_of", GrantId: "grant-a",
		Subject: &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"},
	}
	if err := ValidateExecutionBindingAccessReceipt(receipt, nil, scope, "task-a", "ag::default::sahara::one", now); err == nil {
		t.Fatal("OBO receipt without a gateway-stamped delivery subject was admitted")
	}
}
