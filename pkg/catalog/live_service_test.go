package catalog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestLiveServiceQueryRetainsDeterministicSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	service := newTestLiveService(t, &now, LiveServiceOptions{})
	ctx := context.Background()
	catalogContext := spec.ToolCatalogContext{WorkspaceID: "project-a", SurfaceKind: "tui", SurfaceInstanceID: "window-1"}

	publishTestCatalog(t, service, MutationBinding{ProviderID: "z-provider", RegistrationID: "reg-z", Generation: "gen-z", RequiredContext: catalogContext},
		testPublication("z-provider", "reg-z", "gen-z", 1, now.Add(time.Minute), catalogContext, "zeta"))
	publishTestCatalog(t, service, MutationBinding{ProviderID: "a-provider", RegistrationID: "reg-a", Generation: "gen-a", RequiredContext: catalogContext},
		testPublication("a-provider", "reg-a", "gen-a", 1, now.Add(time.Minute), catalogContext, "alpha", "beta"))

	binding := QueryBinding{SubjectID: "user::drew", PolicyEpoch: "policy-1", Context: catalogContext}
	query := spec.ToolCatalogQuery{SchemaVersion: spec.ToolCatalogSchemaVersion, Context: catalogContext, Limit: 2}
	first, err := service.Query(ctx, binding, query)
	if err != nil {
		t.Fatalf("Query(first): %v", err)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("first page validation: %v", err)
	}
	if got := entryNames(first.Entries); strings.Join(got, ",") != "alpha,beta" {
		t.Fatalf("first names = %v, want [alpha beta]", got)
	}
	if first.NextCursor == "" {
		t.Fatal("first page has no continuation cursor")
	}

	// Mutate the live registration after the first page. The continuation must
	// still resolve against the retained pre-mutation snapshot.
	updated := testPublication("a-provider", "reg-a", "gen-a", 2, now.Add(time.Minute), catalogContext, "alpha", "beta", "new-live-tool")
	publishTestCatalog(t, service, MutationBinding{ProviderID: "a-provider", RegistrationID: "reg-a", Generation: "gen-a", RequiredContext: catalogContext}, updated)

	query.Cursor = first.NextCursor
	second, err := service.Query(ctx, binding, query)
	if err != nil {
		t.Fatalf("Query(second): %v", err)
	}
	if second.SnapshotID != first.SnapshotID || second.CatalogRevision != first.CatalogRevision {
		t.Fatalf("continuation moved snapshots: first=%+v second=%+v", first, second)
	}
	if got := entryNames(second.Entries); strings.Join(got, ",") != "zeta" {
		t.Fatalf("second names = %v, want [zeta] from retained snapshot", got)
	}

	query.Cursor = first.NextCursor[:len(first.NextCursor)-1] + "x"
	if _, err := service.Query(ctx, binding, query); catalogErrorCode(err) != CatalogErrorStaleCursor {
		t.Fatalf("tampered cursor error = %v, want %s", err, CatalogErrorStaleCursor)
	}

	query.Cursor = first.NextCursor
	binding.PolicyEpoch = "policy-2"
	if _, err := service.Query(ctx, binding, query); catalogErrorCode(err) != CatalogErrorStaleCursor {
		t.Fatalf("policy-changed cursor error = %v, want %s", err, CatalogErrorStaleCursor)
	}
}

func TestLiveServiceMutationOrderingLeaseAndReplayRules(t *testing.T) {
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	backend := newMemoryBackend(func() time.Time { return now })
	service, err := NewLiveService(backend, LiveServiceOptions{
		Authorizer: AllowAllEntryAuthorizer{},
		CursorKey:  []byte("0123456789abcdef0123456789abcdef"),
		MaxLease:   10 * time.Second, TombstoneRetention: time.Minute,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLiveService: %v", err)
	}
	ctx := context.Background()
	catalogContext := spec.ToolCatalogContext{WorkspaceID: "project-a", ToolHostID: "host-1"}
	binding := MutationBinding{ProviderID: "provider", RegistrationID: "registration", Generation: "generation-1", RequiredContext: catalogContext}
	publication := testPublication("provider", "registration", "generation-1", 1, now.Add(time.Hour), catalogContext, "read_file")
	result := publishTestCatalog(t, service, binding, publication)
	if result.CatalogRevision == "" {
		t.Fatal("publication returned an empty catalog revision")
	}
	record, err := backend.LoadState(ctx)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if got, want := record.State.Publications[0].LeaseExpiresAt, now.Add(10*time.Second).Format(time.RFC3339Nano); got != want {
		t.Fatalf("admitted lease = %q, want clamp %q", got, want)
	}

	if _, err := service.Publish(ctx, binding, publication); catalogErrorCode(err) != CatalogErrorStaleSequence {
		t.Fatalf("replayed publication error = %v, want %s", err, CatalogErrorStaleSequence)
	}
	changed := testPublication("provider", "registration", "generation-1", 2, now.Add(time.Second), catalogContext, "read_file")
	changed.Entries[0].Descriptor.Description = "changed without revision"
	if _, err := service.Publish(ctx, binding, changed); catalogErrorCode(err) != CatalogErrorInvalidRequest {
		t.Fatalf("mutable exact ref error = %v, want %s", err, CatalogErrorInvalidRequest)
	}

	replacement := testPublication("provider", "registration", "generation-2", 1, now.Add(time.Second), catalogContext, "read_file")
	replacementBinding := binding
	replacementBinding.Generation = "generation-2"
	if _, err := service.Publish(ctx, replacementBinding, replacement); catalogErrorCode(err) != CatalogErrorStaleGeneration {
		t.Fatalf("untrusted replacement error = %v, want %s", err, CatalogErrorStaleGeneration)
	}
	replacementBinding.GenerationReplacement = true
	publishTestCatalog(t, service, replacementBinding, replacement)

	renewOld := spec.ToolCatalogRenewRequest{
		SchemaVersion: spec.ToolCatalogSchemaVersion, ProviderID: "provider", RegistrationID: "registration",
		Generation: "generation-1", Sequence: 3, LeaseExpiresAt: now.Add(time.Second).Format(time.RFC3339Nano),
	}
	if _, err := service.Renew(ctx, binding, renewOld); catalogErrorCode(err) != CatalogErrorStaleGeneration {
		t.Fatalf("old generation renew error = %v, want %s", err, CatalogErrorStaleGeneration)
	}

	revoke := spec.ToolCatalogRevokeRequest{
		SchemaVersion: spec.ToolCatalogSchemaVersion, ProviderID: "provider", RegistrationID: "registration",
		Generation: "generation-2", Sequence: 2,
	}
	if _, err := service.Revoke(ctx, replacementBinding, revoke); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	replacement.Sequence = 3
	if _, err := service.Publish(ctx, replacementBinding, replacement); catalogErrorCode(err) != CatalogErrorStaleGeneration {
		t.Fatalf("revoked generation resurrection error = %v, want %s", err, CatalogErrorStaleGeneration)
	}
}

func TestLiveServiceExpiredGenerationCanBeReplacedWithoutTransitionGrant(t *testing.T) {
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	service := newTestLiveService(t, &now, LiveServiceOptions{})
	ctx := context.Background()
	catalogContext := spec.ToolCatalogContext{WorkspaceID: "project-a"}
	firstBinding := MutationBinding{ProviderID: "provider", RegistrationID: "registration", Generation: "gen-1", RequiredContext: catalogContext}
	publishTestCatalog(t, service, firstBinding, testPublication("provider", "registration", "gen-1", 1, now.Add(time.Second), catalogContext, "tool"))

	now = now.Add(2 * time.Second)
	secondBinding := firstBinding
	secondBinding.Generation = "gen-2"
	if _, err := service.Publish(ctx, secondBinding, testPublication("provider", "registration", "gen-2", 1, now.Add(time.Second), catalogContext, "tool")); err != nil {
		t.Fatalf("replace expired generation: %v", err)
	}
}

func TestLiveServiceAuthorizationLivenessAndExactDescribe(t *testing.T) {
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	authorizer := EntryAuthorizerFunc(func(_ context.Context, action string, _ QueryBinding, entry spec.ToolCatalogEntry) (bool, error) {
		if action == CatalogActionDescribe && entry.Ref.Name == "read_file" {
			return false, nil
		}
		return entry.Effect == spec.ToolEffectRead, nil
	})
	liveness := PublicationLivenessFunc(func(_ context.Context, publication spec.ToolCatalogPublication) (bool, error) {
		return publication.ProviderID != "offline", nil
	})
	service := newTestLiveService(t, &now, LiveServiceOptions{Authorizer: authorizer, Liveness: liveness})
	ctx := context.Background()
	catalogContext := spec.ToolCatalogContext{WorkspaceID: "project-a", ViewID: "worktree-1"}

	live := testPublication("live", "registration", "gen", 1, now.Add(time.Minute), catalogContext, "read_file", "write_file")
	live.Entries[1].Effect = spec.ToolEffectWrite
	publishTestCatalog(t, service, MutationBinding{ProviderID: "live", RegistrationID: "registration", Generation: "gen", RequiredContext: catalogContext}, live)
	offline := testPublication("offline", "registration", "gen", 1, now.Add(time.Minute), catalogContext, "offline_read")
	publishTestCatalog(t, service, MutationBinding{ProviderID: "offline", RegistrationID: "registration", Generation: "gen", RequiredContext: catalogContext}, offline)

	binding := QueryBinding{SubjectID: "user::drew", PolicyEpoch: "policy-1", Context: catalogContext}
	page, err := service.Query(ctx, binding, spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion, Context: catalogContext, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := entryNames(page.Entries); strings.Join(got, ",") != "read_file" {
		t.Fatalf("authorized live entries = %v, want [read_file]", got)
	}

	describe := spec.ToolCatalogDescribeRequest{
		SchemaVersion: spec.ToolCatalogSchemaVersion, Context: catalogContext,
		Ref: page.Entries[0].Ref, SnapshotID: page.SnapshotID,
	}
	if _, err := service.Describe(ctx, binding, describe); catalogErrorCode(err) != CatalogErrorUnauthorized {
		t.Fatalf("independent describe authorization error = %v, want %s", err, CatalogErrorUnauthorized)
	}
	resolved, err := service.ResolveInvocation(ctx, binding, page.Entries[0].Ref)
	if err != nil {
		t.Fatalf("ResolveInvocation(read): %v", err)
	}
	if resolved.Ref.Name != "read_file" {
		t.Fatalf("resolved invocation = %+v", resolved)
	}
	if _, err := service.ResolveInvocation(ctx, binding, live.Entries[1].Ref); catalogErrorCode(err) != CatalogErrorUnauthorized {
		t.Fatalf("write invocation authorization error = %v, want %s", err, CatalogErrorUnauthorized)
	}

	wrongContext := catalogContext
	wrongContext.ViewID = "worktree-2"
	if _, err := service.Query(ctx, binding, spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion, Context: wrongContext, Limit: 10,
	}); catalogErrorCode(err) != CatalogErrorUnauthorized {
		t.Fatalf("spoofed query context error = %v, want %s", err, CatalogErrorUnauthorized)
	}
}

func TestLiveServiceFailsClosedUnlessStandaloneIsExplicit(t *testing.T) {
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	backend := newMemoryBackend(func() time.Time { return now })
	service, err := NewLiveService(backend, LiveServiceOptions{
		CursorKey: []byte("0123456789abcdef0123456789abcdef"), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLiveService: %v", err)
	}
	publication := testPublication("provider", "registration", "generation", 1, now.Add(time.Minute), spec.ToolCatalogContext{}, "tool")
	publishTestCatalog(t, service, MutationBinding{ProviderID: "provider", RegistrationID: "registration", Generation: "generation"}, publication)
	page, err := service.Query(context.Background(), StandaloneQueryBinding(spec.ToolCatalogContext{}), spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(page.Entries) != 0 {
		t.Fatalf("default service returned %d entries without an authorizer", len(page.Entries))
	}

	standalone, err := NewStandaloneLiveService(LiveServiceOptions{
		CursorKey: []byte("0123456789abcdef0123456789abcdef"), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewStandaloneLiveService: %v", err)
	}
	publishTestCatalog(t, standalone, MutationBinding{ProviderID: "provider", RegistrationID: "registration", Generation: "generation"}, publication)
	page, err = standalone.Query(context.Background(), StandaloneQueryBinding(spec.ToolCatalogContext{}), spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion, Limit: 10,
	})
	if err != nil {
		t.Fatalf("standalone Query: %v", err)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("standalone returned %d entries, want 1", len(page.Entries))
	}
}

func TestInvocationCatalogActionUsesCanonicalEffectOperations(t *testing.T) {
	tests := map[spec.ToolEffect]string{
		spec.ToolEffectRead:        CatalogActionInvokeRead,
		spec.ToolEffectWrite:       CatalogActionInvokeWrite,
		spec.ToolEffectExecute:     CatalogActionInvokeExecute,
		spec.ToolEffectExternal:    CatalogActionInvokeExternal,
		spec.ToolEffectInteraction: CatalogActionInvokeInteraction,
	}
	for effect, want := range tests {
		got, err := InvocationCatalogAction(effect)
		if err != nil || got != want {
			t.Fatalf("InvocationCatalogAction(%q) = (%q, %v), want (%q, nil)", effect, got, err, want)
		}
	}
	if _, err := InvocationCatalogAction(spec.ToolEffect("unknown")); catalogErrorCode(err) != CatalogErrorUnauthorized {
		t.Fatalf("unknown effect error = %v, want fail-closed unauthorized", err)
	}
}

func newTestLiveService(t *testing.T, now *time.Time, options LiveServiceOptions) *LiveService {
	t.Helper()
	if options.Authorizer == nil {
		options.Authorizer = AllowAllEntryAuthorizer{}
	}
	if options.CursorKey == nil {
		options.CursorKey = []byte("0123456789abcdef0123456789abcdef")
	}
	options.Now = func() time.Time { return *now }
	backend := newMemoryBackend(options.Now)
	service, err := NewLiveService(backend, options)
	if err != nil {
		t.Fatalf("NewLiveService: %v", err)
	}
	return service
}

func publishTestCatalog(t *testing.T, service *LiveService, binding MutationBinding, publication spec.ToolCatalogPublication) spec.ToolCatalogMutationResult {
	t.Helper()
	result, err := service.Publish(context.Background(), binding, publication)
	if err != nil {
		t.Fatalf("Publish(%s/%s/%s): %v", publication.ProviderID, publication.RegistrationID, publication.Generation, err)
	}
	return result
}

func testPublication(providerID, registrationID, generation string, sequence uint64, expires time.Time, catalogContext spec.ToolCatalogContext, names ...string) spec.ToolCatalogPublication {
	entries := make([]spec.ToolCatalogEntry, 0, len(names))
	for _, name := range names {
		entries = append(entries, spec.ToolCatalogEntry{
			Ref: spec.ToolReference{
				ProviderID: providerID, RegistrationID: registrationID, Generation: generation,
				Name: name, Revision: "sha256:" + name + "-v1",
			},
			Descriptor: spec.ToolDescriptor{
				Name: name, Description: "Test " + name, Kind: "frontend", AwaitsResult: true,
				InputSchema: map[string]json.RawMessage{"type": json.RawMessage(`"object"`)},
			},
			Effect: spec.ToolEffectRead,
		})
	}
	return spec.ToolCatalogPublication{
		SchemaVersion: spec.ToolCatalogSchemaVersion, ProviderID: providerID, RegistrationID: registrationID,
		Generation: generation, Sequence: sequence, LeaseExpiresAt: expires.UTC().Format(time.RFC3339Nano),
		Context: catalogContext, Entries: entries,
	}
}

func entryNames(entries []spec.ToolCatalogEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Ref.Name
	}
	return names
}

func catalogErrorCode(err error) string {
	protocol, ok := AsToolCatalogError(err)
	if !ok {
		return ""
	}
	return protocol.Code
}
