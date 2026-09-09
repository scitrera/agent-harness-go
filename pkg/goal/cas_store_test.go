// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package goal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

type memoryCASBlobs struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newMemoryCASBlobs() *memoryCASBlobs {
	return &memoryCASBlobs{values: map[string][]byte{}}
}

func (b *memoryCASBlobs) Read(_ context.Context, key string) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	value, found := b.values[key]
	return append([]byte(nil), value...), found, nil
}

func (b *memoryCASBlobs) Create(_ context.Context, key string, value []byte) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, found := b.values[key]; found {
		return false, nil
	}
	b.values[key] = append([]byte(nil), value...)
	return true, nil
}

func (b *memoryCASBlobs) CompareAndSwap(_ context.Context, key string, expected, value []byte) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !bytes.Equal(b.values[key], expected) {
		return false, nil
	}
	b.values[key] = append([]byte(nil), value...)
	return true, nil
}

func TestCASStorePersistsConcurrentGoalsAcrossReplicas(t *testing.T) {
	blobs := newMemoryCASBlobs()
	first, err := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	if err != nil {
		t.Fatal(err)
	}
	const count = 64
	errorsCh := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			store := first
			if i%2 == 1 {
				store = second
			}
			id := fmt.Sprintf("goal-%02d", i)
			errorsCh <- store.PutGoal(context.Background(), "project-a", "session-1", spec.SessionGoalRecord{
				ID: id, Objective: id, Status: spec.SessionGoalActive,
				CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
			})
		}(i)
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	restarted, err := NewCASStore(CASStoreConfig{Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	records, err := restarted.ListGoals(context.Background(), "project-a", "session-1")
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
	isolated, err := restarted.ListGoals(context.Background(), "project-b", "session-1")
	if err != nil || len(isolated) != 0 {
		t.Fatalf("isolated records = %#v err=%v", isolated, err)
	}
}

func TestCASStorePreservesCreationAndDefensiveCopies(t *testing.T) {
	blobs := newMemoryCASBlobs()
	store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
	budget := uint64(100)
	base := spec.SessionGoalRecord{
		ID: "goal-1", Objective: "finish", Status: spec.SessionGoalActive,
		CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
		TokenBudget: &budget, Evidence: []string{"start"},
	}
	if err := store.PutGoal(context.Background(), "project-a", "session-1", base); err != nil {
		t.Fatal(err)
	}
	done := base
	done.Status = spec.SessionGoalCompleted
	done.CreatedAt = "replacement"
	done.UpdatedAt = "2026-08-08T20:02:00Z"
	done.CompletedAt = done.UpdatedAt
	if err := store.PutGoal(context.Background(), "project-a", "session-1", done); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListGoals(context.Background(), "project-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].CreatedAt != base.CreatedAt {
		t.Fatalf("records = %#v", records)
	}
	*records[0].TokenBudget = 1
	records[0].Evidence[0] = "changed"
	again, _ := store.ListGoals(context.Background(), "project-a", "session-1")
	if *again[0].TokenBudget != budget || again[0].Evidence[0] != "start" {
		t.Fatalf("caller mutated distributed store: %#v", again)
	}
}

func TestCASStoreRejectsCorruptRemoteState(t *testing.T) {
	blobs := newMemoryCASBlobs()
	store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
	blobs.values[store.key("project-a", "session-1")] = []byte(`{"schema_version":1,"workspace_id":"project-b"}`)
	if _, err := store.ListGoals(context.Background(), "project-a", "session-1"); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("error = %v", err)
	}
}
