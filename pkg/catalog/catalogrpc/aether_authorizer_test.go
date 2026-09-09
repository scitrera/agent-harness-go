// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalogrpc

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

type recordingBatchChecker struct {
	batches       [][]*pb.ResourceAccessRequest
	authorization *pb.AuthorizationContext
	malformed     bool
	err           error
}

func (c *recordingBatchChecker) BatchCheckAccess(_ context.Context, requests []*pb.ResourceAccessRequest, authorization *pb.AuthorizationContext) ([]*pb.AccessDecisionReceipt, error) {
	c.batches = append(c.batches, requests)
	c.authorization = authorization
	if c.err != nil {
		return nil, c.err
	}
	receipts := make([]*pb.AccessDecisionReceipt, len(requests))
	for i, request := range requests {
		receipts[i] = &pb.AccessDecisionReceipt{Request: request, Allowed: i%2 == 0}
	}
	if c.malformed {
		return receipts[:len(receipts)-1], nil
	}
	return receipts, nil
}

func TestAetherEntryAuthorizerChunksAndPreservesOrderedDecisions(t *testing.T) {
	checker := &recordingBatchChecker{}
	authorizer, err := NewAetherEntryAuthorizer(checker)
	if err != nil {
		t.Fatalf("NewAetherEntryAuthorizer: %v", err)
	}
	forwarded := testForwardedAuthorization("alice", "root-1", "sv::tool-catalog::catalog-1")
	ctx := context.WithValue(context.Background(), forwardedAuthorizationContextKey{}, forwarded)
	binding := catalog.QueryBinding{
		SubjectID: "alice", PolicyEpoch: "aether-entry-v1", AuthorityLineageID: "root-1",
		Context: spec.ToolCatalogContext{
			WorkspaceID: "project-a", ToolHostID: "us::alice::window-1",
			SurfaceKind: "web", SurfaceInstanceID: "window-1",
		},
	}
	entries := make([]spec.ToolCatalogEntry, 205)
	for i := range entries {
		entries[i].Ref = spec.ToolReference{ProviderID: "provider", Name: fmt.Sprintf("tool-%03d", i)}
	}
	decisions, err := authorizer.AuthorizeCatalogEntries(ctx, catalog.CatalogActionDiscover, binding, entries)
	if err != nil {
		t.Fatalf("AuthorizeCatalogEntries: %v", err)
	}
	if len(checker.batches) != 3 || len(checker.batches[0]) != 100 || len(checker.batches[1]) != 100 || len(checker.batches[2]) != 5 {
		t.Fatalf("batch sizes = %d/%d/%d", len(checker.batches[0]), len(checker.batches[1]), len(checker.batches[2]))
	}
	if checker.authorization != forwarded.GetAuthorization() {
		t.Fatal("batch checks did not use the trusted forwarded authorization context")
	}
	for i, allowed := range decisions {
		if allowed != ((i%100)%2 == 0) {
			t.Fatalf("decision[%d] = %v", i, allowed)
		}
	}
	first := checker.batches[0][0]
	if first.GetResourceType() != "tool-catalog/entry" || first.GetOperation() != catalog.CatalogActionDiscover ||
		first.GetWorkspace() != "project-a" || first.GetRequiredAccessLevel() != 10 {
		t.Fatalf("first access request = %+v", first)
	}
	wantResource, err := catalog.EntryResourceID(binding.Context, entries[0].Ref)
	if err != nil {
		t.Fatalf("EntryResourceID: %v", err)
	}
	if first.GetResourceId() != wantResource {
		t.Fatalf("resource id = %q, want %q", first.GetResourceId(), wantResource)
	}
	if first.GetCorrelationId() != catalogAccessCorrelation(catalog.CatalogActionDiscover, wantResource) ||
		len(first.GetCorrelationId()) > 128 {
		t.Fatalf("correlation id = %q", first.GetCorrelationId())
	}
}

func TestAetherEntryAuthorizerFailsClosedOnMissingLineageAndMalformedBatch(t *testing.T) {
	checker := &recordingBatchChecker{malformed: true}
	authorizer, _ := NewAetherEntryAuthorizer(checker)
	forwarded := testForwardedAuthorization("alice", "root-1", "sv::tool-catalog::catalog-1")
	ctx := context.WithValue(context.Background(), forwardedAuthorizationContextKey{}, forwarded)
	binding := catalog.QueryBinding{
		SubjectID: "alice", PolicyEpoch: "aether-entry-v1", AuthorityLineageID: "other-root",
		Context: spec.ToolCatalogContext{WorkspaceID: "project-a", SurfaceKind: "cli", SurfaceInstanceID: "terminal-1"},
	}
	entries := []spec.ToolCatalogEntry{{Ref: spec.ToolReference{ProviderID: "provider", Name: "tool"}}}
	if _, err := authorizer.AuthorizeCatalogEntries(ctx, catalog.CatalogActionDiscover, binding, entries); err == nil {
		t.Fatal("expected lineage mismatch to fail")
	}
	binding.AuthorityLineageID = "root-1"
	if _, err := authorizer.AuthorizeCatalogEntries(ctx, catalog.CatalogActionDiscover, binding, entries); err == nil {
		t.Fatal("expected malformed batch to fail")
	}
}
