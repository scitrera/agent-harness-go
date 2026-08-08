package subagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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

func casRecord(id, parent string, status spec.SessionSubagentStatus) spec.SessionSubagentRecord {
	record := spec.SessionSubagentRecord{
		ID: id, ParentSessionID: parent, ChildSessionID: id,
		Status: status, CreatedAt: timeCreated, UpdatedAt: timeRunning,
	}
	if status == spec.SessionSubagentCompleted {
		record.UpdatedAt = timeDone
		record.CompletedAt = timeDone
	}
	return record
}

func TestCASRegistryPersistsConcurrentLifecycleAcrossReplicas(t *testing.T) {
	blobs := newMemoryCASBlobs()
	first, err := NewCASRegistry(CASRegistryConfig{Blobs: blobs, MaxRetries: 256})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCASRegistry(CASRegistryConfig{Blobs: blobs, MaxRetries: 256})
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
			registry := first
			if i%2 == 1 {
				registry = second
			}
			id := fmt.Sprintf("child-%02d", i)
			errorsCh <- registry.ObserveSubagent(context.Background(), LifecycleEvent{
				WorkspaceID: "project-a", Record: casRecord(id, "parent-1", spec.SessionSubagentRunning),
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
	restarted, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs})
	records, err := restarted.ListSubagents(context.Background(), "project-a", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count {
		t.Fatalf("records = %d, want %d", len(records), count)
	}
	for i, record := range records {
		if record.ID != fmt.Sprintf("child-%02d", i) {
			t.Fatalf("record %d = %q", i, record.ID)
		}
	}
	isolated, err := restarted.ListSubagents(context.Background(), "project-b", "parent-1")
	if err != nil || len(isolated) != 0 {
		t.Fatalf("isolated records = %#v err=%v", isolated, err)
	}
}

func TestCASRegistryDeferredRecoveryRunsOnFirstUse(t *testing.T) {
	blobs := newMemoryCASBlobs()
	seed, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs})
	if err := seed.ObserveSubagent(context.Background(), LifecycleEvent{
		WorkspaceID: "project-a", Record: casRecord("running", "parent-1", spec.SessionSubagentRunning),
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.ObserveSubagent(context.Background(), LifecycleEvent{
		WorkspaceID: "project-b", Record: casRecord("done", "parent-2", spec.SessionSubagentCompleted),
	}); err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs, DeferRecovery: true})
	recoveryAt := time.Date(2026, 8, 8, 21, 0, 0, 0, time.UTC)
	if err := restarted.RecoverInterrupted(context.Background(), recoveryAt); err != nil {
		t.Fatal(err)
	}
	// The first remote operation occurs after an Aether channel has established
	// its exclusive stable identity, and applies recovery across the CAS index.
	records, err := restarted.ListSubagents(context.Background(), "project-a", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Status != spec.SessionSubagentInterrupted || records[0].CompletedAt != recoveryAt.Format(time.RFC3339Nano) {
		t.Fatalf("recovered records = %#v", records)
	}
	done, err := restarted.ListSubagents(context.Background(), "project-b", "parent-2")
	if err != nil || len(done) != 1 || done[0].Status != spec.SessionSubagentCompleted {
		t.Fatalf("terminal record changed: %#v err=%v", done, err)
	}
}

func TestCASRegistryIndexesConcurrentParentsForCompleteRecovery(t *testing.T) {
	blobs := newMemoryCASBlobs()
	first, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs, MaxRetries: 256})
	second, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs, MaxRetries: 256})
	const count = 32
	errorsCh := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			registry := first
			if i%2 == 1 {
				registry = second
			}
			parent := fmt.Sprintf("parent-%02d", i)
			errorsCh <- registry.ObserveSubagent(context.Background(), LifecycleEvent{
				WorkspaceID: "project-a", Record: casRecord("child", parent, spec.SessionSubagentRunning),
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
	restarted, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs, MaxRetries: 256})
	recoveryAt := time.Date(2026, 8, 8, 21, 30, 0, 0, time.UTC)
	if err := restarted.RecoverInterrupted(context.Background(), recoveryAt); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		parent := fmt.Sprintf("parent-%02d", i)
		records, err := restarted.ListSubagents(context.Background(), "project-a", parent)
		if err != nil || len(records) != 1 || records[0].Status != spec.SessionSubagentInterrupted {
			t.Fatalf("parent=%s records=%#v err=%v", parent, records, err)
		}
	}
}

func TestCASRegistryPreservesDeletionTombstone(t *testing.T) {
	blobs := newMemoryCASBlobs()
	registry, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs})
	record := casRecord("child-1", "parent-1", spec.SessionSubagentDeleted)
	record.UpdatedAt = timeDone
	record.DeletedAt = timeDone
	if err := registry.ObserveSubagent(context.Background(), LifecycleEvent{WorkspaceID: "project-a", Record: record}); err != nil {
		t.Fatal(err)
	}
	record.Status = spec.SessionSubagentRunning
	record.DeletedAt = ""
	if err := registry.ObserveSubagent(context.Background(), LifecycleEvent{WorkspaceID: "project-a", Record: record}); !errors.Is(err, ErrDeletedLifecycle) {
		t.Fatalf("error = %v", err)
	}
}

func TestCASRegistryRejectsCorruptStateAndIndex(t *testing.T) {
	t.Run("state", func(t *testing.T) {
		blobs := newMemoryCASBlobs()
		registry, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs})
		blobs.values[registry.key("project-a", "parent-1")] = []byte(`{"schema_version":1,"workspace_id":"project-b"}`)
		if _, err := registry.ListSubagents(context.Background(), "project-a", "parent-1"); !errors.Is(err, ErrCorruptRegistry) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("index", func(t *testing.T) {
		blobs := newMemoryCASBlobs()
		registry, _ := NewCASRegistry(CASRegistryConfig{Blobs: blobs})
		blobs.values[registry.indexKey()] = []byte(`{"schema_version":1,"refs":[{"workspace_id":"z","parent_session_id":"p"},{"workspace_id":"a","parent_session_id":"p"}]}`)
		if err := registry.RecoverInterrupted(context.Background(), time.Now()); !errors.Is(err, ErrCorruptRegistry) {
			t.Fatalf("error = %v", err)
		}
	})
}
