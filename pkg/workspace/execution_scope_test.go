package workspace

import (
	"context"
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func testExecutionScope(t *testing.T, access ViewWriteAccess) ExecutionScope {
	t.Helper()
	binding := spec.NewExecutionBinding("project-a", "view-a", "us::drew::window-1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-a"
	scope, err := NewExecutionScope(binding, ExecutionViewPolicy{WriteAccess: access})
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestExecutionScopeMessageRoundTripRequiresExplicitPolicy(t *testing.T) {
	scope := testExecutionScope(t, ViewWriteAccessReadOnly)
	message := spec.NewChatMessage("message-1", spec.RoleUser)
	if err := PutExecutionScope(&message, scope); err != nil {
		t.Fatal(err)
	}
	got, err := GetExecutionScope(message)
	if err != nil || !ExecutionScopesEqual(&scope, got) {
		t.Fatalf("scope = %#v, %v", got, err)
	}

	missingPolicy := spec.NewChatMessage("message-2", spec.RoleUser)
	if err := spec.PutExecutionBinding(&missingPolicy, scope.Binding); err != nil {
		t.Fatal(err)
	}
	if _, err = GetExecutionScope(missingPolicy); err == nil {
		t.Fatal("expected binding without explicit policy to fail")
	}
}

func TestExecutionScopeRejectsOrphanAndUnknownPolicy(t *testing.T) {
	message := spec.NewChatMessage("message-1", spec.RoleUser)
	message.Meta[ExecutionViewPolicyMetaKey] = json.RawMessage(`{"write_access":"read_only"}`)
	if _, err := GetExecutionScope(message); err == nil {
		t.Fatal("expected orphan policy rejection")
	}
	if _, err := DecodeExecutionViewPolicy([]byte(`{"write_access":"read_only","future":true}`)); err == nil {
		t.Fatal("expected unknown policy field rejection")
	}
}

func TestExecutionScopeContextAndMonotonicNarrowing(t *testing.T) {
	parent := testExecutionScope(t, ViewWriteAccessReadWrite)
	child := parent.ReadOnly()
	if !IsMonotonicScopeNarrowing(&parent, &child) || IsMonotonicScopeNarrowing(&child, &parent) {
		t.Fatal("read-only narrowing rules are inverted")
	}
	ctx := WithExecutionScope(context.Background(), child)
	got, ok := ExecutionScopeFrom(ctx)
	if !ok || !ExecutionScopesEqual(&child, &got) {
		t.Fatalf("context scope = %#v, %v", got, ok)
	}
}
