package refinement

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func proposal(key string) AppendRequest {
	return AppendRequest{
		Key: key,
		Plan: Plan{
			RefinementID: "refine-1", Trigger: "user", Scope: ScopeWorkspace,
			Summary: "Add reviewer", Rationale: "Repeated unsupported claims", ExpectedOutcome: "Claims cite evidence",
			Evidence: []Evidence{{Kind: EvidenceUserInstruction, Reference: "message:m1", Description: "User requested evidence"}},
			Edits:    []Edit{{Action: ActionCreate, ResourceKind: ResourceAgentSpecification, ResourceKey: "review/evidence", Reason: "Reusable review"}},
		},
		Phase: PhaseProposal, Outcome: OutcomeProposed,
		TaskRef: &ExternalReference{System: "aether", ID: "task-1"},
	}
}

func TestFileStoreAppendReplayRestartAndWorkspaceIsolation(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	store, err := NewFileStore(t.TempDir(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := proposal("refinements/r1/proposal")
	created, err := store.Append(context.Background(), "project-a", "op-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if created.Replayed || created.Record.Revision != 1 || created.Record.ETag == "" || created.Record.CreatedAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("created = %#v", created)
	}
	replay, err := store.Append(context.Background(), "project-a", "op-1", request)
	if err != nil || !replay.Replayed || replay.Record.ID != created.Record.ID {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	other, err := store.Query(context.Background(), "project-b", Query{})
	if err != nil || len(other.Records) != 0 {
		t.Fatalf("other workspace = %#v, %v", other, err)
	}
	restarted, _ := NewFileStore(store.stateDir, nil)
	loaded, err := restarted.Get(context.Background(), "project-a", created.Record.ID)
	if err != nil || loaded.Key != request.Key || loaded.TaskRef == nil || loaded.TaskRef.ID != "task-1" {
		t.Fatalf("loaded = %#v, %v", loaded, err)
	}
}

func TestFileStoreRejectsConflictingOperationsAndKeys(t *testing.T) {
	store, _ := NewFileStore(t.TempDir(), nil)
	request := proposal("refinements/r1/proposal")
	if _, err := store.Append(context.Background(), "project", "op-1", request); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.Summary = "Changed"
	if _, err := store.Append(context.Background(), "project", "op-1", changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("operation conflict = %v", err)
	}
	if _, err := store.Append(context.Background(), "project", "op-2", request); !errors.Is(err, ErrConflict) {
		t.Fatalf("key conflict = %v", err)
	}
}

func TestFileStoreListsNewestFirst(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	store, _ := NewFileStore(t.TempDir(), func() time.Time {
		now = now.Add(time.Second)
		return now
	})
	first := proposal("refinements/r1/proposal")
	first.RefinementID = "r1"
	second := proposal("refinements/r2/proposal")
	second.RefinementID = "r2"
	if _, err := store.Append(context.Background(), "project", "op-1", first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), "project", "op-2", second); err != nil {
		t.Fatal(err)
	}
	page, err := store.Query(context.Background(), "project", Query{})
	if err != nil || len(page.Records) != 2 {
		t.Fatalf("records = %#v, %v", page, err)
	}
	records := page.Records
	if records[0].RefinementID != "r2" || records[1].RefinementID != "r1" {
		t.Fatalf("record order = %#v", records)
	}
}

func TestFileStoreQueryFiltersAndUsesStableScopedCursor(t *testing.T) {
	store, _ := NewFileStore(t.TempDir(), nil)
	appendProposal := func(operationID, refinementID string, kind ResourceKind, summary string) {
		t.Helper()
		request := proposal("refinements/" + refinementID + "/proposal")
		request.RefinementID = refinementID
		request.Summary = summary
		request.Edits[0].ResourceKind = kind
		if _, err := store.Append(context.Background(), "project", operationID, request); err != nil {
			t.Fatal(err)
		}
	}
	appendProposal("op-1", "r1", ResourceAgentSpecification, "Evidence reviewer one")
	appendProposal("op-2", "r2", ResourcePromptNote, "Unrelated prompt")
	appendProposal("op-3", "r3", ResourceAgentSpecification, "Evidence reviewer three")

	query := Query{ResourceKinds: []ResourceKind{ResourceAgentSpecification}, Text: "EVIDENCE", Limit: 1}
	first, err := store.Query(context.Background(), "project", query)
	if err != nil || len(first.Records) != 1 || first.Records[0].RefinementID != "r3" || first.NextPageToken == "" {
		t.Fatalf("first page=%#v err=%v", first, err)
	}
	appendProposal("op-4", "r4", ResourceAgentSpecification, "Evidence added after cursor")
	query.PageToken = first.NextPageToken
	second, err := store.Query(context.Background(), "project", query)
	if err != nil || len(second.Records) != 1 || second.Records[0].RefinementID != "r1" || second.NextPageToken != "" || second.ScannedCount != 2 {
		t.Fatalf("second page=%#v err=%v", second, err)
	}
	query.Text = "different"
	if _, err := store.Query(context.Background(), "project", query); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched cursor error = %v", err)
	}
}

func TestFileStoreConcurrentReplayAppendsOnce(t *testing.T) {
	store, _ := NewFileStore(t.TempDir(), nil)
	request := proposal("refinements/r1/proposal")
	results := make(chan AppendResult, 8)
	errorsCh := make(chan error, 8)
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := store.Append(context.Background(), "project", "op-1", request)
			results <- result
			errorsCh <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	ids := map[string]struct{}{}
	for result := range results {
		ids[result.Record.ID] = struct{}{}
	}
	if len(ids) != 1 {
		t.Fatalf("record ids = %#v", ids)
	}
	page, err := store.Query(context.Background(), "project", Query{})
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("records = %#v, %v", page, err)
	}
}

func TestAppendRequestValidatesPhaseOutcomeEvidenceAndRollback(t *testing.T) {
	request := proposal("refinements/r1/proposal")
	request.Outcome = OutcomeApplied
	if err := request.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("phase outcome error = %v", err)
	}
	request = proposal("refinements/r1/proposal")
	request.Evidence = nil
	if err := request.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("evidence error = %v", err)
	}
	request = proposal("refinements/r1/rollback")
	request.Phase, request.Outcome = PhaseRollback, OutcomeRolledBack
	if err := request.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("rollback link error = %v", err)
	}
	request = proposal("refinements/r1/restore")
	request.Edits = []Edit{{
		Action: ActionRestore, ResourceKind: ResourcePromptNote, ResourceKey: "note", ResourceID: "note-1", ExpectedETag: "e2", Reason: "restore deletion",
	}}
	if err := request.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("schema v1 restore error = %v", err)
	}
	request.SchemaVersion = 2
	if err := request.Validate(); err != nil {
		t.Fatalf("schema v2 restore error = %v", err)
	}
}
