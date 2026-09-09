// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalog

import (
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestEntryResourceFamilyPatternKeepsOnlyProviderAndToolGlobs(t *testing.T) {
	context := spec.ToolCatalogContext{
		WorkspaceID: "workspace/a", ThreadID: "thread*", ViewID: "view one",
		ToolHostID: "ag::worker::one", SurfaceKind: "worker", SurfaceInstanceID: "ag::worker::one",
	}
	got, err := EntryResourceFamilyPattern(context)
	if err != nil {
		t.Fatal(err)
	}
	want := "workspaces/workspace%2Fa/threads/thread%2A/views/view%20one/hosts/ag%3A%3Aworker%3A%3Aone/" +
		"surfaces/worker/instances/ag%3A%3Aworker%3A%3Aone/providers/*/tools/*"
	if got != want {
		t.Fatalf("pattern = %q, want %q", got, want)
	}
}

func TestEntryResourceIDUsesSentinelsForAbsentSurface(t *testing.T) {
	got, err := EntryResourceID(spec.ToolCatalogContext{WorkspaceID: "workspace"}, spec.ToolReference{
		ProviderID: "provider", Name: "tool",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "workspaces/workspace/threads/!/views/!/hosts/!/surfaces/!/instances/!/providers/provider/tools/tool"
	if got != want {
		t.Fatalf("resource = %q, want %q", got, want)
	}
}

func TestProviderResourceAndMutationCorrelationMatchEntryFamily(t *testing.T) {
	contextValue := spec.ToolCatalogContext{WorkspaceID: "workspace"}
	provider, err := ProviderResourceID(contextValue, "documents")
	if err != nil {
		t.Fatal(err)
	}
	want := "workspaces/workspace/threads/!/views/!/hosts/!/surfaces/!/instances/!/providers/documents"
	if provider != want {
		t.Fatalf("provider resource = %q, want %q", provider, want)
	}
	publish := MutationCorrelation("tool.catalog.publish", "documents", "primary", "generation-1", 7)
	if publish != MutationCorrelation("tool.catalog.publish", "documents", "primary", "generation-1", 7) ||
		publish == MutationCorrelation("tool.catalog.revoke", "documents", "primary", "generation-1", 7) {
		t.Fatalf("mutation correlation is not deterministic and action-bound: %q", publish)
	}
}
