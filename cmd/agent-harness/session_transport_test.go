package main

import (
	"context"
	"reflect"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestParseVisibleWorkspacesNormalizesList(t *testing.T) {
	want := []string{"project-a", "project-b"}
	if got := parseVisibleWorkspaces(" project-b,project-a,,project-b "); !reflect.DeepEqual(got, want) {
		t.Fatalf("parseVisibleWorkspaces() = %#v, want %#v", got, want)
	}
}

func TestAdditionalVisibleWorkspaceIgnoresDefault(t *testing.T) {
	if hasAdditionalVisibleWorkspace("project-a", []string{"project-a"}) {
		t.Fatal("default-only list enabled multi-workspace capability")
	}
	if !hasAdditionalVisibleWorkspace("project-a", []string{"project-a", "project-b"}) {
		t.Fatal("additional workspace did not enable multi-workspace capability")
	}
}

func TestSessionTransportAdvertisesAndEnforcesVisibleWorkspaces(t *testing.T) {
	resolver, err := newSessionWorkspaceResolver("project-a", []string{"project-b"})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := newSessionTransport(&recordingScopedHistory{}, resolver, "project-a", true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest("shared", "client-1")
	request.WorkspaceID = "project-b"
	request.Capabilities = append(request.Capabilities, spec.SessionCapabilityMultiWorkspace)
	result, err := transport.Coordinator.Attach(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkspaceID != "project-b" || !spec.SupportsSessionCapability(result.Capabilities, spec.SessionCapabilityMultiWorkspace) {
		t.Fatalf("attach result = %+v", result)
	}
	request.WorkspaceID = "project-c"
	if _, err := transport.Coordinator.Attach(context.Background(), request); err == nil {
		t.Fatal("unlisted workspace was accepted")
	}
}
