package goal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestFileStorePersistsGoalAcrossRestartAndWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	store, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	budget := uint64(1000)
	goal := spec.SessionGoalRecord{
		ID: "goal-1", Objective: "verify restart", Status: spec.SessionGoalActive,
		CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
		TokenBudget: &budget, TokenUsage: 125, Evidence: []string{"message:assistant-1"},
	}
	if err := store.PutGoal(ctx, "project-a", "session-1", goal); err != nil {
		t.Fatal(err)
	}
	completed := goal
	completed.Status = spec.SessionGoalCompleted
	completed.CreatedAt = "2026-08-08T21:00:00Z"
	completed.UpdatedAt = "2026-08-08T20:02:00Z"
	completed.CompletedAt = completed.UpdatedAt
	completed.TokenUsage = 500
	if err := store.PutGoal(ctx, "project-a", "session-1", completed); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := restarted.ListGoals(ctx, "project-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Status != spec.SessionGoalCompleted || records[0].CreatedAt != goal.CreatedAt || records[0].TokenUsage != 500 {
		t.Fatalf("restart goals = %#v", records)
	}
	*records[0].TokenBudget = 1
	records[0].Evidence[0] = "mutated"
	again, err := restarted.ListGoals(ctx, "project-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if *again[0].TokenBudget != budget || again[0].Evidence[0] != goal.Evidence[0] {
		t.Fatalf("caller mutated store: %#v", again[0])
	}
	isolated, err := restarted.ListGoals(ctx, "project-b", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(isolated) != 0 {
		t.Fatalf("workspace B leaked goals: %#v", isolated)
	}
}

func TestFileStoreConcurrentGoalsRemainSorted(t *testing.T) {
	const count = 32
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	errors := make(chan error, count)
	var wait sync.WaitGroup
	for i := count - 1; i >= 0; i-- {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			id := fmt.Sprintf("goal-%02d", i)
			errors <- store.PutGoal(context.Background(), "project-a", "session-1", spec.SessionGoalRecord{
				ID: id, Objective: id, Status: spec.SessionGoalActive,
				CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
			})
		}(i)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	records, err := store.ListGoals(context.Background(), "project-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count {
		t.Fatalf("records = %d, want %d", len(records), count)
	}
	for i, record := range records {
		if record.ID != fmt.Sprintf("goal-%02d", i) {
			t.Fatalf("record %d = %q", i, record.ID)
		}
	}
}

func TestFileStoreRejectsCorruptState(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := store.path("project-a", "session-1")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"workspace_id":"project-b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListGoals(context.Background(), "project-a", "session-1"); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("corrupt store error = %v", err)
	}
}
