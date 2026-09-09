// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	aether "github.com/scitrera/aether/sdk/go/aether"
)

const defaultAetherCatalogPrefix = "sahara/tool-catalog/v1"

// AetherLiveBackendOptions binds operational catalog state to one Aether KV
// namespace. Workspace scope is the default so separate coding projects can
// have independent live catalogs while sharing the same Aether deployment.
type AetherLiveBackendOptions struct {
	KeyPrefix string
	Scope     aether.KVScope
	UserID    string
	Workspace string
	Timeout   time.Duration
}

// AetherLiveBackend persists the current catalog state with Aether KV CAS and
// retains immutable snapshots as TTL keys. Protocol leases remain explicit in
// the publication state; snapshot retention additionally uses native KV TTL.
type AetherLiveBackend struct {
	kv        atomicCatalogKV
	stateKey  string
	snapshots string
}

// NewAetherLiveBackend adapts an Aether SDK KV client to LiveBackend.
func NewAetherLiveBackend(kv *aether.KV, options AetherLiveBackendOptions) (*AetherLiveBackend, error) {
	if kv == nil {
		return nil, fmt.Errorf("catalog: Aether KV client is required")
	}
	if options.Scope == "" {
		options.Scope = aether.KVScopeWorkspace
	}
	if !options.Scope.Valid() {
		return nil, fmt.Errorf("catalog: invalid Aether KV scope %q", options.Scope)
	}
	prefix := strings.TrimSuffix(strings.TrimSpace(options.KeyPrefix), "/")
	if prefix == "" {
		prefix = defaultAetherCatalogPrefix
	}
	if strings.ContainsRune(prefix, '\x00') {
		return nil, fmt.Errorf("catalog: Aether key prefix must not contain NUL")
	}
	adapter := &aetherCatalogKV{
		kv: kv, scope: options.Scope, userID: options.UserID,
		workspace: options.Workspace, timeout: options.Timeout,
	}
	return newAetherLiveBackend(adapter, prefix), nil
}

func newAetherLiveBackend(kv atomicCatalogKV, prefix string) *AetherLiveBackend {
	return &AetherLiveBackend{
		kv: kv, stateKey: prefix + "/state", snapshots: prefix + "/snapshots/",
	}
}

func (b *AetherLiveBackend) LoadState(ctx context.Context) (LiveStateRecord, error) {
	value, found, err := b.kv.get(ctx, b.stateKey)
	if err != nil {
		return LiveStateRecord{}, err
	}
	if !found {
		return LiveStateRecord{}, nil
	}
	var state LiveState
	if err := json.Unmarshal(value, &state); err != nil {
		return LiveStateRecord{}, fmt.Errorf("decode Aether catalog state: %w", err)
	}
	return LiveStateRecord{State: state, Token: append([]byte(nil), value...), Exists: true}, nil
}

func (b *AetherLiveBackend) CompareAndSwapState(ctx context.Context, current LiveStateRecord, next LiveState) (bool, error) {
	value, err := json.Marshal(next)
	if err != nil {
		return false, fmt.Errorf("encode Aether catalog state: %w", err)
	}
	if !current.Exists {
		return b.kv.setIfAbsent(ctx, b.stateKey, value, 0)
	}
	return b.kv.compareAndSwap(ctx, b.stateKey, current.Token, value, 0)
}

func (b *AetherLiveBackend) StoreSnapshot(ctx context.Context, snapshot RetainedSnapshot, ttl time.Duration) (RetainedSnapshot, error) {
	value, err := json.Marshal(snapshot)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("encode Aether retained snapshot: %w", err)
	}
	key := b.snapshots + snapshot.SnapshotID
	applied, err := b.kv.setIfAbsent(ctx, key, value, ceilSecond(ttl))
	if err != nil {
		return RetainedSnapshot{}, err
	}
	if applied {
		return snapshot, nil
	}
	existing, found, err := b.LoadSnapshot(ctx, snapshot.SnapshotID)
	if err != nil {
		return RetainedSnapshot{}, err
	}
	if found {
		return existing, nil
	}
	// The prior immutable key can expire between SET_NX and GET. One retry is
	// enough: either this value wins or another equivalent writer does.
	applied, err = b.kv.setIfAbsent(ctx, key, value, ceilSecond(ttl))
	if err != nil {
		return RetainedSnapshot{}, err
	}
	if applied {
		return snapshot, nil
	}
	existing, found, err = b.LoadSnapshot(ctx, snapshot.SnapshotID)
	if err != nil {
		return RetainedSnapshot{}, err
	}
	if !found {
		return RetainedSnapshot{}, fmt.Errorf("retained snapshot changed during Aether KV write")
	}
	return existing, nil
}

func (b *AetherLiveBackend) LoadSnapshot(ctx context.Context, snapshotID string) (RetainedSnapshot, bool, error) {
	value, found, err := b.kv.get(ctx, b.snapshots+snapshotID)
	if err != nil || !found {
		return RetainedSnapshot{}, found, err
	}
	var snapshot RetainedSnapshot
	if err := json.Unmarshal(value, &snapshot); err != nil {
		return RetainedSnapshot{}, false, fmt.Errorf("decode Aether retained snapshot: %w", err)
	}
	return snapshot, true, nil
}

type atomicCatalogKV interface {
	get(ctx context.Context, key string) ([]byte, bool, error)
	setIfAbsent(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
	compareAndSwap(ctx context.Context, key string, expected, value []byte, ttl time.Duration) (bool, error)
}

type aetherCatalogKV struct {
	kv        *aether.KV
	scope     aether.KVScope
	userID    string
	workspace string
	timeout   time.Duration
}

func (a *aetherCatalogKV) get(ctx context.Context, key string) ([]byte, bool, error) {
	response, err := a.kv.GetSync(ctx, aether.KVGetOptions{
		Key: key, Scope: a.scope, UserID: a.userID, Workspace: a.workspace, Timeout: a.timeout,
	})
	if err != nil {
		return nil, false, err
	}
	if response == nil || !response.Success || len(response.Value) == 0 {
		return nil, false, nil
	}
	return append([]byte(nil), response.Value...), true, nil
}

func (a *aetherCatalogKV) setIfAbsent(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return a.kv.SetNXSync(ctx, key, value, a.scope, a.userID, a.workspace, ttl, a.timeout)
}

func (a *aetherCatalogKV) compareAndSwap(ctx context.Context, key string, expected, value []byte, ttl time.Duration) (bool, error) {
	return a.kv.CompareAndSetSync(ctx, key, expected, value, a.scope, a.userID, a.workspace, ttl, a.timeout)
}

func ceilSecond(value time.Duration) time.Duration {
	if value <= 0 {
		return 0
	}
	return ((value + time.Second - 1) / time.Second) * time.Second
}

var _ LiveBackend = (*AetherLiveBackend)(nil)
