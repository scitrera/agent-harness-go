package catalog

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestLiveServiceSurfacesOnlyServerOwnedInvocationAuthority(t *testing.T) {
	now := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	catalogContext := spec.ToolCatalogContext{
		WorkspaceID: "project-a", ToolHostID: "ag::project-a::tool-host::one",
	}
	publication := testPublication(
		"provider", "registration", "generation", 1, now.Add(time.Minute), catalogContext, "vfs_search",
	)
	ref := publication.Entries[0].Ref
	publication.Entries[0].Descriptor.Meta = map[string]json.RawMessage{
		"provider_invocation_authority": json.RawMessage(`{"mode":"caller_obo","max_access_level":50}`),
	}
	want := InvocationAuthorityProfile{
		Mode: InvocationAuthorityModeCallerOBO,
		ResourceScope: []InvocationAuthorityResourceScope{
			{ResourceType: "vfs", Patterns: []string{"workspaces/project-a/*"}},
		},
		OperationScope: []string{"read"}, MaxAccessLevel: 10,
	}
	policy, err := NewStaticInvocationAuthorityPolicy([]InvocationAuthorityRule{{Ref: ref, Profile: want}})
	if err != nil {
		t.Fatal(err)
	}
	service := newTestLiveService(t, &now, LiveServiceOptions{InvocationAuthority: policy})
	publishTestCatalog(t, service, MutationBinding{
		ProviderID: "provider", RegistrationID: "registration", Generation: "generation", RequiredContext: catalogContext,
	}, publication)
	page, err := service.QueryResolved(context.Background(), QueryBinding{
		SubjectID: "alice", PolicyEpoch: "policy-1", AuthorityLineageID: "root-1", Context: catalogContext,
	}, spec.ToolCatalogQuery{SchemaVersion: spec.ToolCatalogSchemaVersion, Context: catalogContext, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].InvocationAuthority == nil {
		t.Fatalf("resolved records = %+v", page.Records)
	}
	got := page.Records[0].InvocationAuthority
	if got.MaxAccessLevel != 10 || got.OperationScope[0] != "read" || got.ResourceScope[0].ResourceType != "vfs" {
		t.Fatalf("resolved profile = %+v", got)
	}
	if string(page.Records[0].Entry.Descriptor.Meta["provider_invocation_authority"]) == "" {
		t.Fatal("portable provider metadata unexpectedly mutated")
	}

	other := publication.Entries[0]
	other.Ref.Revision = "sha256:unreviewed-revision"
	profile, err := policy.ResolveInvocationAuthority(context.Background(), catalogContext, other)
	if err != nil || profile != nil {
		t.Fatalf("unreviewed revision profile = %+v, %v", profile, err)
	}
}

func TestStaticInvocationAuthorityPolicyRejectsInvalidAndDuplicateRules(t *testing.T) {
	ref := spec.ToolReference{
		ProviderID: "provider", RegistrationID: "registration", Generation: "generation",
		Name: "tool", Revision: "sha256:tool-v1",
	}
	valid := InvocationAuthorityProfile{
		Mode:           InvocationAuthorityModeCallerOBO,
		ResourceScope:  []InvocationAuthorityResourceScope{{ResourceType: "vfs", Patterns: []string{"project-a/*"}}},
		OperationScope: []string{"read"}, MaxAccessLevel: 10,
	}
	if _, err := NewStaticInvocationAuthorityPolicy([]InvocationAuthorityRule{
		{Ref: ref, Profile: valid}, {Ref: ref, Profile: valid},
	}); err == nil {
		t.Fatal("duplicate exact authority rules were accepted")
	}
	invalid := valid
	invalid.OperationScope = nil
	if _, err := NewStaticInvocationAuthorityPolicy([]InvocationAuthorityRule{{Ref: ref, Profile: invalid}}); err == nil {
		t.Fatal("unbounded authority profile was accepted")
	}
}
