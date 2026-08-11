package executionledger

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func deterministicStore(max int) *MemoryStore {
	sequence := 0
	return NewMemoryStore(MemoryStoreConfig{
		MaxEvents: max,
		Now:       func() time.Time { return time.Date(2026, 8, 11, 12, 0, sequence, 0, time.UTC) },
		NewID: func() (string, error) {
			sequence++
			return "event-" + string(rune('0'+sequence)), nil
		},
	})
}

func TestMemoryStoreRepresentsConcurrentBranches(t *testing.T) {
	ctx := context.Background()
	store := deterministicStore(20)
	ref := Ref{WorkspaceID: "workspace-a", SessionID: "session-a"}
	first, err := store.Append(ctx, ref, "start-a", AppendRequest{Type: EventTurnStarted, BranchID: "task-a", TaskID: "task-a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(ctx, ref, "start-b", AppendRequest{Type: EventTurnStarted, BranchID: "task-b", TaskID: "task-b"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Event.ParentID != "" || second.Event.ParentID != "" {
		t.Fatalf("concurrent roots should share the empty terminal parent: first=%q second=%q", first.Event.ParentID, second.Event.ParentID)
	}
	finishA, err := store.Append(ctx, ref, "finish-a", AppendRequest{Type: EventTurnFinished, BranchID: "task-a", TaskID: "task-a"})
	if err != nil {
		t.Fatal(err)
	}
	if finishA.Event.ParentID != first.Event.ID {
		t.Fatalf("branch A parent = %q, want %q", finishA.Event.ParentID, first.Event.ID)
	}
	finishB, err := store.Append(ctx, ref, "finish-b", AppendRequest{Type: EventTurnFinished, BranchID: "task-b", TaskID: "task-b"})
	if err != nil {
		t.Fatal(err)
	}
	if finishB.Event.ParentID != second.Event.ID {
		t.Fatalf("branch B parent = %q, want %q", finishB.Event.ParentID, second.Event.ID)
	}
	next, err := store.Append(ctx, ref, "start-c", AppendRequest{Type: EventTurnStarted, BranchID: "task-c", TaskID: "task-c"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Event.ParentID != finishB.Event.ID {
		t.Fatalf("next root parent = %q, want latest terminal %q", next.Event.ParentID, finishB.Event.ID)
	}
}

func TestMemoryStoreIdempotencyPinAndBoundedCursor(t *testing.T) {
	ctx := context.Background()
	store := deterministicStore(4)
	ref := Ref{WorkspaceID: "workspace-a", SessionID: "session-a"}
	request := AppendRequest{Type: EventTurnStarted, BranchID: "task-a", TaskID: "task-a"}
	first, err := store.Append(ctx, ref, "start", request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.Append(ctx, ref, "start", request)
	if err != nil || !replay.Replayed || replay.Event.ID != first.Event.ID {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	if _, err := store.Append(ctx, ref, "start", AppendRequest{Type: EventTurnStarted, BranchID: "other"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("operation reuse error = %v", err)
	}
	if _, err := store.Append(ctx, ref, "pin", AppendRequest{Type: EventModelPinned, BranchID: "task-a", Model: "model-a"}); err != nil {
		t.Fatal(err)
	}
	if model, err := store.PinnedModel(ctx, ref); err != nil || model != "model-a" {
		t.Fatalf("pinned model = %q, %v", model, err)
	}
	for i, kind := range []EventType{EventUserPromptSubmitted, EventContextCompacted, EventTurnFinished} {
		if _, err := store.Append(ctx, ref, "more-"+string(rune('0'+i)), AppendRequest{Type: kind, BranchID: "task-a"}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.Query(ctx, ref, Query{Limit: 2})
	if err != nil || len(page.Events) != 2 || page.NextPageToken == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	next, err := store.Query(ctx, ref, Query{Limit: 2, PageToken: page.NextPageToken})
	if err != nil || len(next.Events) != 2 {
		t.Fatalf("next page = %+v, %v", next, err)
	}
	if _, err := store.Query(ctx, ref, Query{Limit: 2, BranchID: "another-branch", PageToken: page.NextPageToken}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cursor scope mismatch = %v", err)
	}
}

type memoryCAS struct {
	mu     sync.Mutex
	values map[string][]byte
}

func (m *memoryCAS) Read(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[key]
	return append([]byte(nil), value...), ok, nil
}

func (m *memoryCAS) Create(_ context.Context, key string, value []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.values[key]; exists {
		return false, nil
	}
	m.values[key] = append([]byte(nil), value...)
	return true, nil
}

func (m *memoryCAS) CompareAndSwap(_ context.Context, key string, expected, value []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, exists := m.values[key]
	if !exists || string(current) != string(expected) {
		return false, nil
	}
	m.values[key] = append([]byte(nil), value...)
	return true, nil
}

func TestFileAndCASStoresReopenTheSameNeutralState(t *testing.T) {
	ctx := context.Background()
	ref := Ref{WorkspaceID: "workspace-a", SessionID: "session-a"}
	file, err := NewFileStore(FileStoreConfig{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Append(ctx, ref, "start", AppendRequest{Type: EventTurnStarted, BranchID: "task-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Append(ctx, ref, "pin", AppendRequest{Type: EventModelPinned, BranchID: "task-a", Model: "model-a"}); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileStore(FileStoreConfig{StateDir: file.stateDir})
	if err != nil {
		t.Fatal(err)
	}
	if model, err := reopened.PinnedModel(ctx, ref); err != nil || model != "model-a" {
		t.Fatalf("file pin after reopen = %q, %v", model, err)
	}

	blobs := &memoryCAS{values: map[string][]byte{}}
	cas, err := NewCASStore(CASStoreConfig{Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Append(ctx, ref, "start", AppendRequest{Type: EventTurnStarted, BranchID: "task-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Append(ctx, ref, "pin", AppendRequest{Type: EventModelPinned, BranchID: "task-a", Model: "model-b"}); err != nil {
		t.Fatal(err)
	}
	casReopened, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
	if model, err := casReopened.PinnedModel(ctx, ref); err != nil || model != "model-b" {
		t.Fatalf("CAS pin after reopen = %q, %v", model, err)
	}
}
