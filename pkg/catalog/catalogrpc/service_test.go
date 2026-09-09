// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalogrpc

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestServicePreservesContextAndReportsSnapshotWideBareNameAmbiguity(t *testing.T) {
	resolver := newTestResolver(t)
	service, err := NewService(resolver, ServiceOptions{
		MutationSourcePrefixes: []string{"sv::platform-bridge::"}, PolicyEpoch: "policy-1",
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	ctx := context.Background()
	catalogContext := spec.ToolCatalogContext{
		WorkspaceID: "project-a", ToolHostID: "us::alice::window-1",
		SurfaceKind: "web", SurfaceInstanceID: "window-1",
	}
	for _, provider := range []string{"provider-a", "provider-z"} {
		publication := testPublication(provider, "window-1", "generation-1", catalogContext, "shared_name")
		request := PublishRequest{
			Binding: MutationBinding{
				ProviderID: provider, RegistrationID: "window-1", Generation: "generation-1",
				ProviderRoute:   catalogContext.ToolHostID,
				RequiredContext: catalogContext,
			},
			Publication: publication,
		}
		if _, err := service.HandleJSON(ctx, Caller{SourceTopic: "sv::platform-bridge::edge-1"}, MethodPublish, mustJSON(t, request)); err != nil {
			t.Fatalf("publish %s: %v", provider, err)
		}
	}

	query := spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion, Context: catalogContext, Limit: 1,
	}
	caller := Caller{
		SourceTopic: "sv::agent-harness::worker-1", SubjectID: "alice",
		ForwardedAuthorization: testForwardedAuthorization("alice", "root-1", "sv::tool-catalog::catalog-1"),
	}
	raw, err := service.HandleJSON(ctx, caller, MethodQuery, mustJSON(t, query))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var page catalog.ResolvedCatalogPage
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("decode query result: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].Context.ToolHostID != "us::alice::window-1" {
		t.Fatalf("resolved records = %+v", page.Records)
	}
	if got := strings.Join(page.AmbiguousNames, ","); got != "shared_name" {
		t.Fatalf("ambiguous_names = %q, want shared_name", got)
	}
	if page.NextCursor == "" {
		t.Fatal("first page has no cursor")
	}

	query.Cursor = page.NextCursor
	otherLineage := caller
	otherLineage.ForwardedAuthorization = testForwardedAuthorization("alice", "root-2", "sv::tool-catalog::catalog-1")
	if _, err := service.HandleJSON(ctx, otherLineage, MethodQuery, mustJSON(t, query)); err == nil || !strings.Contains(err.Error(), "cursor") {
		t.Fatalf("cursor replay under another root grant error = %v", err)
	}
	raw, err = service.HandleJSON(ctx, caller, MethodQuery, mustJSON(t, query))
	if err != nil {
		t.Fatalf("query continuation: %v", err)
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("decode continuation: %v", err)
	}
	if got := strings.Join(page.AmbiguousNames, ","); got != "shared_name" {
		t.Fatalf("continuation ambiguous_names = %q, want shared_name", got)
	}
}

func TestServiceRejectsUntrustedMutationAndUnboundQuery(t *testing.T) {
	service, err := NewService(newTestResolver(t), ServiceOptions{
		PolicyEpoch: "policy-1",
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	catalogContext := spec.ToolCatalogContext{WorkspaceID: "project-a", ToolHostID: "host-1"}
	request := PublishRequest{
		Binding: MutationBinding{
			ProviderID: "provider", RegistrationID: "reg", Generation: "gen",
			ProviderRoute: "ag::project-a::provider::one", RequiredContext: catalogContext,
		},
		Publication: testPublication("provider", "reg", "gen", catalogContext, "tool"),
	}
	if _, err := service.HandleJSON(context.Background(), Caller{SourceTopic: "sv::untrusted::one"}, MethodPublish, mustJSON(t, request)); err == nil || !strings.Contains(err.Error(), "not a trusted catalog edge") {
		t.Fatalf("untrusted publish error = %v", err)
	}
	query := spec.ToolCatalogQuery{SchemaVersion: spec.ToolCatalogSchemaVersion, Context: catalogContext, Limit: 10}
	if _, err := service.HandleJSON(context.Background(), Caller{}, MethodQuery, mustJSON(t, query)); err == nil || !strings.Contains(err.Error(), "OBO subject") {
		t.Fatalf("unbound query error = %v", err)
	}
	if _, err := service.HandleJSON(context.Background(), Caller{SubjectID: "alice"}, MethodQuery, mustJSON(t, query)); err == nil || !strings.Contains(err.Error(), "forwarded authorization") {
		t.Fatalf("query without forwarded authority error = %v", err)
	}
}

func TestServiceRejectsTrailingJSONValue(t *testing.T) {
	service, err := NewService(newTestResolver(t), ServiceOptions{
		MutationSourcePrefixes: []string{"sv::platform-bridge::"}, PolicyEpoch: "policy-1",
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	query := spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		Context:       spec.ToolCatalogContext{WorkspaceID: "project-a", ToolHostID: "host-1"},
		Limit:         10,
	}
	payload := append(mustJSON(t, query), []byte(` {}`)...)
	_, err = service.HandleJSON(
		context.Background(), Caller{
			SubjectID:              "alice",
			ForwardedAuthorization: testForwardedAuthorization("alice", "root-1", "sv::tool-catalog::catalog-1"),
		}, MethodQuery, payload,
	)
	if err == nil || !strings.Contains(err.Error(), "multiple JSON values") {
		t.Fatalf("trailing JSON error = %v", err)
	}
}

func TestServiceAdmitsCheckedDirectAgentMutationAndRejectsRouteTakeover(t *testing.T) {
	service, err := NewService(newTestResolver(t), ServiceOptions{
		PolicyEpoch: "policy-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogContext := spec.ToolCatalogContext{WorkspaceID: "project-a"}
	route := "ag::project-a::tool-host::one"
	publication := testPublication("documents", "tool-host-one", "generation-1", catalogContext, "vfs_search")
	request := PublishRequest{
		Binding: MutationBinding{
			ProviderID: publication.ProviderID, RegistrationID: publication.RegistrationID,
			Generation: publication.Generation, ProviderRoute: route, RequiredContext: catalogContext,
		},
		Publication: publication,
	}
	caller := directAgentMutationCaller(t, route, "sv::tool-catalog::one", MethodPublish, request.Binding, publication.Sequence)
	if _, err := service.HandleJSON(context.Background(), caller, MethodPublish, mustJSON(t, request)); err != nil {
		t.Fatalf("checked direct publish: %v", err)
	}

	replacement := testPublication("documents", "tool-host-one", "generation-2", catalogContext, "vfs_search")
	replacementBinding := MutationBinding{
		ProviderID: replacement.ProviderID, RegistrationID: replacement.RegistrationID,
		Generation: replacement.Generation, ProviderRoute: "ag::project-a::tool-host::other",
		RequiredContext: catalogContext, GenerationReplacement: true,
	}
	replacementRequest := PublishRequest{Binding: replacementBinding, Publication: replacement}
	replacementCaller := directAgentMutationCaller(
		t, replacementBinding.ProviderRoute, "sv::tool-catalog::one",
		MethodPublish, replacementBinding, replacement.Sequence,
	)
	_, err = service.HandleJSON(context.Background(), replacementCaller, MethodPublish, mustJSON(t, replacementRequest))
	if err == nil || !strings.Contains(err.Error(), "owned by another authenticated route") {
		t.Fatalf("route takeover error = %v", err)
	}

	caller.AccessReceipt.Request.CorrelationId = "forged"
	_, err = service.HandleJSON(context.Background(), caller, MethodPublish, mustJSON(t, request))
	if err == nil || !strings.Contains(err.Error(), "exact provider action") {
		t.Fatalf("forged receipt error = %v", err)
	}
}

func directAgentMutationCaller(
	t *testing.T,
	route string,
	deliveryTarget string,
	method string,
	binding MutationBinding,
	sequence uint64,
) Caller {
	t.Helper()
	resourceID, err := catalog.ProviderResourceID(binding.RequiredContext, binding.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	return Caller{
		SourceTopic: route, DeliveryTarget: deliveryTarget,
		AccessReceipt: &pb.AccessDecisionReceipt{
			Allowed: true, Decision: "ALLOW", EffectiveAccessLevel: 20,
			Request: &pb.ResourceAccessRequest{
				ResourceType: catalogProviderResourceType, ResourceId: resourceID,
				Operation: catalogPublishOperation, Workspace: binding.RequiredContext.WorkspaceID,
				RequiredAccessLevel: catalogMutationAccessLevel,
				CorrelationId: catalog.MutationCorrelation(
					method, binding.ProviderID, binding.RegistrationID, binding.Generation, sequence,
				),
			},
			Actor:         &pb.PrincipalRef{PrincipalType: "agent", PrincipalId: route},
			AuthorityMode: "direct", DeliveryTarget: deliveryTarget,
			ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(),
		},
	}
}

func testForwardedAuthorization(subject, rootGrantID, target string) *pb.ForwardedAuthorization {
	return &pb.ForwardedAuthorization{
		Authorization: &pb.AuthorizationContext{
			AuthorityMode: "on_behalf_of",
			Subject:       &pb.PrincipalRef{PrincipalType: "user", PrincipalId: subject},
			GrantId:       "child-" + rootGrantID,
		},
		RootGrantId: rootGrantID, ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(), DeliveryTarget: target,
	}
}

type testResolver struct {
	t        *testing.T
	mu       sync.Mutex
	services map[string]*catalog.LiveService
}

func newTestResolver(t *testing.T) *testResolver {
	return &testResolver{t: t, services: make(map[string]*catalog.LiveService)}
}

func (r *testResolver) ResolveCatalogWorkspace(_ context.Context, workspace string) (*catalog.LiveService, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if service := r.services[workspace]; service != nil {
		return service, nil
	}
	service, err := catalog.NewLiveService(catalog.NewMemoryBackend(), catalog.LiveServiceOptions{
		Authorizer: catalog.AllowAllEntryAuthorizer{},
		CursorKey:  []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		r.t.Fatalf("NewLiveService: %v", err)
	}
	r.services[workspace] = service
	return service, nil
}

func testPublication(provider, registration, generation string, catalogContext spec.ToolCatalogContext, name string) spec.ToolCatalogPublication {
	return spec.ToolCatalogPublication{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		ProviderID:    provider, RegistrationID: registration, Generation: generation,
		Sequence: 1, LeaseExpiresAt: time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Context: catalogContext,
		Entries: []spec.ToolCatalogEntry{{
			Ref: spec.ToolReference{
				ProviderID: provider, RegistrationID: registration, Generation: generation,
				Name: name, Revision: "sha256:" + provider + "-v1",
			},
			Descriptor: spec.ToolDescriptor{
				Name: name, Description: "test", Kind: "frontend", AwaitsResult: true,
				InputSchema: map[string]json.RawMessage{"type": json.RawMessage(`"object"`)},
			},
			Effect: spec.ToolEffectRead,
		}},
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return data
}
