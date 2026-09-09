// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package memorylayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/refinement"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestRefinementRecordStoreUsesTypedSDKWorkspaceAuthorityAndIdempotency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refinement-records" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Idempotency-Key") != "op-1" || r.Header.Get("If-None-Match") != "*" {
			t.Errorf("conditional headers = %#v", r.Header)
		}
		if r.Header.Get("X-Aether-Grant-ID") != "grant-1" || r.Header.Get("X-Aether-Subject-ID") != "alice" {
			t.Errorf("authority headers = %#v", r.Header)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["workspace_id"] != "project-a" || body["refinement_id"] != "refine-1" {
			t.Errorf("body = %#v", body)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"record":{"id":"record-1","tenant_id":"tenant","workspace_id":"project-a","key":"refinements/r1/proposal","refinement_id":"refine-1","phase":"proposal","trigger":"user","scope":"workspace","summary":"Add reviewer","rationale":"Repeated unsupported claims","expected_outcome":"Claims cite evidence","evidence":[{"kind":"user_instruction","reference":"message:m1","description":"User requested evidence"}],"edits":[{"action":"create","resource_kind":"agent_specification","resource_key":"review/evidence","reason":"Reusable review"}],"outcome":"proposed","task_ref":{"system":"aether","id":"task-1"},"schema_version":1,"metadata":{},"revision":1,"etag":"etag-1","created_at":"2026-08-09T00:00:00Z"},"replayed":true}`))
	}))
	defer server.Close()

	store, err := NewRefinementRecordStore(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"})
	request := refinement.AppendRequest{
		Key: "refinements/r1/proposal",
		Plan: refinement.Plan{
			RefinementID: "refine-1", Trigger: "user", Scope: refinement.ScopeWorkspace,
			Summary: "Add reviewer", Rationale: "Repeated unsupported claims", ExpectedOutcome: "Claims cite evidence",
			Evidence: []refinement.Evidence{{Kind: refinement.EvidenceUserInstruction, Reference: "message:m1", Description: "User requested evidence"}},
			Edits:    []refinement.Edit{{Action: refinement.ActionCreate, ResourceKind: refinement.ResourceAgentSpecification, ResourceKey: "review/evidence", Reason: "Reusable review"}},
		},
		Phase: refinement.PhaseProposal, Outcome: refinement.OutcomeProposed,
		TaskRef: &refinement.ExternalReference{System: "aether", ID: "task-1"},
	}
	result, err := store.Append(ctx, "project-a", "op-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Replayed || result.Record.ID != "record-1" || result.Record.TaskRef == nil || result.Record.TaskRef.ID != "task-1" {
		t.Fatalf("result = %#v", result)
	}
}

func TestRefinementRecordStoreQueriesOneBoundedAuthorityPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Header.Get("X-Aether-Grant-ID") != "grant-query" || r.Header.Get("X-Aether-Subject-ID") != "operator" {
			t.Errorf("authority headers = %#v", r.Header)
		}
		if query.Get("workspace_id") != "project" || query.Get("limit") != "7" || query.Get("page_token") != "cursor" ||
			query.Get("phase") != "application" || query.Get("outcome") != "failed" || query.Get("scope") != "workspace" ||
			query.Get("resource_kind") != "memory" || query.Get("refinement_id") != "refine-1" || query.Get("search") != "broken" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"records":[],"next_page_token":"next","scanned_count":31,"scan_truncated":true}`))
	}))
	defer server.Close()
	store, _ := NewRefinementRecordStore(Config{BaseURL: server.URL})
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{GrantID: "grant-query", SubjectType: "user", SubjectID: "operator"})
	page, err := store.Query(ctx, "project", refinement.Query{
		Phases: []refinement.Phase{refinement.PhaseApplication}, Outcomes: []refinement.Outcome{refinement.OutcomeFailed},
		Scopes: []refinement.Scope{refinement.ScopeWorkspace}, ResourceKinds: []refinement.ResourceKind{refinement.ResourceMemory},
		RefinementID: "refine-1", Text: "broken", Limit: 7, PageToken: "cursor",
	})
	if err != nil || page.NextPageToken != "next" || page.ScannedCount != 31 || !page.ScanTruncated {
		t.Fatalf("page=%#v err=%v", page, err)
	}
}

func TestRefinementRecordStoreRejectsNonAdvancingCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"records":[],"next_page_token":"same","scanned_count":1}`))
	}))
	defer server.Close()
	store, _ := NewRefinementRecordStore(Config{BaseURL: server.URL})
	if _, err := store.Query(context.Background(), "project", refinement.Query{PageToken: "same"}); err == nil {
		t.Fatal("expected non-advancing cursor error")
	}
}

func TestRefinementEditAdapterPreservesContentAndSnapshots(t *testing.T) {
	applied := true
	edits := []refinement.Edit{{
		Action: refinement.ActionReplace, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "note", ResourceID: "note-1",
		ExpectedETag: "e1", BeforeETag: "e1", AfterETag: "e2", Reason: "improve guidance",
		Content: map[string]any{"title": "New", "content": "New content"},
		Before:  &refinement.ResourceSnapshot{ResourceID: "note-1", ResourceKey: "note", ETag: "e1", SchemaVersion: 1, Content: map[string]any{"title": "Old"}},
		After:   &refinement.ResourceSnapshot{ResourceID: "note-1", ResourceKey: "note", ETag: "e2", SchemaVersion: 1, Content: map[string]any{"title": "New"}},
		Applied: &applied,
	}}
	roundTrip := refinementEdits(sdkRefinementEdits(edits))
	if len(roundTrip) != 1 || roundTrip[0].Content["title"] != "New" || roundTrip[0].Before == nil || roundTrip[0].Before.ETag != "e1" || roundTrip[0].After == nil || roundTrip[0].After.ETag != "e2" {
		t.Fatalf("round trip = %#v", roundTrip)
	}
}
