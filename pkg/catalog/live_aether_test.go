// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalog

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

func TestAetherLiveBackendCASAndImmutableSnapshots(t *testing.T) {
	kv := &fakeAtomicCatalogKV{values: make(map[string][]byte), ttls: make(map[string]time.Duration)}
	backend := newAetherLiveBackend(kv, "test/catalog")
	ctx := context.Background()

	initial, err := backend.LoadState(ctx)
	if err != nil {
		t.Fatalf("LoadState(initial): %v", err)
	}
	if initial.Exists {
		t.Fatal("initial state unexpectedly exists")
	}
	state1 := LiveState{SchemaVersion: LiveStateSchemaVersion, Publications: nil}
	if applied, err := backend.CompareAndSwapState(ctx, initial, state1); err != nil || !applied {
		t.Fatalf("initial CompareAndSwapState = (%v, %v), want (true, nil)", applied, err)
	}
	loaded, err := backend.LoadState(ctx)
	if err != nil || !loaded.Exists {
		t.Fatalf("LoadState(stored) = (%+v, %v)", loaded, err)
	}
	state2 := LiveState{SchemaVersion: LiveStateSchemaVersion, Tombstones: []GenerationTombstone{}}
	if applied, err := backend.CompareAndSwapState(ctx, initial, state2); err != nil || applied {
		t.Fatalf("stale CompareAndSwapState = (%v, %v), want (false, nil)", applied, err)
	}
	if applied, err := backend.CompareAndSwapState(ctx, loaded, state2); err != nil || !applied {
		t.Fatalf("current CompareAndSwapState = (%v, %v), want (true, nil)", applied, err)
	}

	snapshot1 := RetainedSnapshot{SchemaVersion: LiveStateSchemaVersion, SnapshotID: "sha256:snapshot", CatalogRevision: "sha256:revision"}
	stored, err := backend.StoreSnapshot(ctx, snapshot1, 1500*time.Millisecond)
	if err != nil {
		t.Fatalf("StoreSnapshot(first): %v", err)
	}
	if stored.CatalogRevision != snapshot1.CatalogRevision {
		t.Fatalf("stored snapshot = %+v", stored)
	}
	if got := kv.ttls["test/catalog/snapshots/sha256:snapshot"]; got != 2*time.Second {
		t.Fatalf("snapshot KV TTL = %v, want 2s ceiling", got)
	}

	snapshot2 := snapshot1
	snapshot2.CatalogRevision = "sha256:different"
	stored, err = backend.StoreSnapshot(ctx, snapshot2, time.Minute)
	if err != nil {
		t.Fatalf("StoreSnapshot(second): %v", err)
	}
	if stored.CatalogRevision != snapshot1.CatalogRevision {
		t.Fatalf("immutable snapshot was replaced: %+v", stored)
	}
}

type fakeAtomicCatalogKV struct {
	mu     sync.Mutex
	values map[string][]byte
	ttls   map[string]time.Duration
}

func (f *fakeAtomicCatalogKV) get(_ context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, found := f.values[key]
	return append([]byte(nil), value...), found, nil
}

func (f *fakeAtomicCatalogKV) setIfAbsent(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.values[key]; exists {
		return false, nil
	}
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeAtomicCatalogKV) compareAndSwap(_ context.Context, key string, expected, value []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, exists := f.values[key]
	if !exists || !bytes.Equal(current, expected) {
		return false, nil
	}
	f.values[key] = append([]byte(nil), value...)
	f.ttls[key] = ttl
	return true, nil
}
