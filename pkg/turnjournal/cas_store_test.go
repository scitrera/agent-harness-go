package turnjournal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type memoryCASStore struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newMemoryCASStore() *memoryCASStore {
	return &memoryCASStore{values: map[string][]byte{}}
}

func (s *memoryCASStore) Read(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, found := s.values[key]
	return append([]byte(nil), value...), found, nil
}

func (s *memoryCASStore) Create(_ context.Context, key string, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.values[key]; found {
		return false, nil
	}
	s.values[key] = append([]byte(nil), value...)
	return true, nil
}

func (s *memoryCASStore) CompareAndSwap(_ context.Context, key string, expected, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !bytes.Equal(s.values[key], expected) {
		return false, nil
	}
	s.values[key] = append([]byte(nil), value...)
	return true, nil
}

func TestCASStore_ConcurrentReplicasIndexEveryRecoverableExecution(t *testing.T) {
	blobs := newMemoryCASStore()
	now := time.Date(2026, 8, 8, 14, 0, 0, 0, time.UTC)
	first, err := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	second, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256, Now: func() time.Time { return now }})
	const count = 48
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := first
			if i%2 == 1 {
				store = second
			}
			record := preparedRecord(fmt.Sprintf("project-%d", i%3), fmt.Sprintf("thread-%02d", i), fmt.Sprintf("task-%02d", i), "stable-owner")
			_, err := store.Create(context.Background(), record)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	restarted, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
	active, err := restarted.ListActive(context.Background(), "stable-owner")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != count {
		t.Fatalf("active executions = %d, want %d", len(active), count)
	}
	for _, record := range active {
		if record.OwnerIdentity != "stable-owner" || record.Phase != PhasePrepared || record.Revision != 1 {
			t.Fatalf("unexpected active record: %+v", record)
		}
	}
}

func TestCASStore_RevisionConflictAcrossReplicas(t *testing.T) {
	blobs := newMemoryCASStore()
	now := time.Date(2026, 8, 8, 15, 0, 0, 0, time.UTC)
	first, _ := NewCASStore(CASStoreConfig{Blobs: blobs, Now: func() time.Time { return now }})
	second, _ := NewCASStore(CASStoreConfig{Blobs: blobs, Now: func() time.Time { return now.Add(time.Second) }})
	base, err := first.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner"))
	if err != nil {
		t.Fatal(err)
	}
	left := base
	left.Phase = PhaseProviderPending
	if _, err := first.Update(context.Background(), left, left.Revision); err != nil {
		t.Fatal(err)
	}
	right := base
	right.Phase = PhaseProviderPending
	if _, err := second.Update(context.Background(), right, right.Revision); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	loaded, err := second.Get(context.Background(), "project-a", "task-1")
	if err != nil || loaded.Revision != 2 || loaded.Phase != PhaseProviderPending {
		t.Fatalf("loaded = %+v, err=%v", loaded, err)
	}
}

func TestCASStore_IndexFirstMissingRecordIsHarmlessAndLaterCreateSucceeds(t *testing.T) {
	blobs := newMemoryCASStore()
	store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
	ref := casExecutionRef{WorkspaceID: "project-a", TaskID: "task-1", OwnerIdentity: "owner"}
	if err := store.ensureIndexed(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	active, err := store.ListActive(context.Background(), "owner")
	if err != nil || len(active) != 0 {
		t.Fatalf("missing indexed record = %+v, err=%v", active, err)
	}
	created, err := store.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner"))
	if err != nil || created.TaskID != "task-1" {
		t.Fatalf("later create = %+v, err=%v", created, err)
	}
}

func TestCASStore_FailsClosedOnCorruptIndexAndState(t *testing.T) {
	t.Run("unknown index field", func(t *testing.T) {
		blobs := newMemoryCASStore()
		store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
		blobs.values[store.indexKey()] = []byte(`{"schema_version":1,"refs":[],"unknown":true}`)
		if _, err := store.ListActive(context.Background(), "owner"); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("unsorted index", func(t *testing.T) {
		blobs := newMemoryCASStore()
		store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
		index := casExecutionIndex{SchemaVersion: casIndexVersion, Refs: []casExecutionRef{
			{WorkspaceID: "z", TaskID: "task", OwnerIdentity: "owner"},
			{WorkspaceID: "a", TaskID: "task", OwnerIdentity: "owner"},
		}}
		blobs.values[store.indexKey()], _ = json.Marshal(index)
		if _, err := store.ListActive(context.Background(), "owner"); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("record owner mismatch", func(t *testing.T) {
		blobs := newMemoryCASStore()
		store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
		created, err := store.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner"))
		if err != nil {
			t.Fatal(err)
		}
		created.OwnerIdentity = "other-owner"
		blobs.values[store.key(created.WorkspaceID, created.TaskID)], _ = json.Marshal(created)
		if _, err := store.ListActive(context.Background(), "owner"); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestCASStore_ConflictingOwnerCannotClaimIndexedTask(t *testing.T) {
	blobs := newMemoryCASStore()
	store, _ := NewCASStore(CASStoreConfig{Blobs: blobs})
	if _, err := store.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner-b")); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting owner error = %v", err)
	}
}
