package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// LiveStateSchemaVersion versions the backend record and retained-snapshot
// representation independently of the portable ecosystem protocol.
const LiveStateSchemaVersion = "3"

// LiveState is the backend-neutral, CAS-protected operational catalog state.
// Publications contain only the current generation for each provider
// registration. Tombstones reject delayed messages from replaced or revoked
// generations during a bounded replay-retention window.
type LiveState struct {
	SchemaVersion  string                        `json:"schema_version"`
	Publications   []spec.ToolCatalogPublication `json:"publications"`
	ProviderRoutes []ProviderRouteBinding        `json:"provider_routes,omitempty"`
	Tombstones     []GenerationTombstone         `json:"tombstones,omitempty"`
}

// ProviderRouteBinding is authenticated operational state for one live
// provider generation. It is deliberately separate from the portable
// ToolCatalogContext: availability selectors describe where a tool applies,
// while ProviderRoute identifies the exact Aether recipient that executes it.
type ProviderRouteBinding struct {
	ProviderID     string `json:"provider_id"`
	RegistrationID string `json:"registration_id"`
	Generation     string `json:"generation"`
	ProviderRoute  string `json:"provider_route"`
}

// GenerationTombstone remembers the final accepted sequence for a generation
// after it is replaced, expires, or is revoked.
type GenerationTombstone struct {
	ProviderID     string `json:"provider_id"`
	RegistrationID string `json:"registration_id"`
	Generation     string `json:"generation"`
	ProviderRoute  string `json:"provider_route,omitempty"`
	Sequence       uint64 `json:"sequence"`
	RetainUntil    string `json:"retain_until"`
}

// LiveStateRecord is one backend read. Token is opaque and must be passed back
// unchanged to CompareAndSwapState. Exists distinguishes an absent initial
// state from a stored state.
type LiveStateRecord struct {
	State  LiveState
	Token  []byte
	Exists bool
}

// RetainedSnapshot is an immutable, authorization-bound query result retained
// long enough to serve deterministic continuation cursors.
type RetainedSnapshot struct {
	SchemaVersion   string                 `json:"schema_version"`
	SnapshotID      string                 `json:"snapshot_id"`
	CatalogRevision string                 `json:"catalog_revision"`
	BindingDigest   string                 `json:"binding_digest"`
	QueryDigest     string                 `json:"query_digest"`
	Limit           uint32                 `json:"limit"`
	CreatedAt       string                 `json:"created_at"`
	ExpiresAt       string                 `json:"expires_at"`
	Records         []ResolvedCatalogEntry `json:"records"`
}

// LiveBackend is the complete storage contract needed by LiveService. State
// replacement is CAS-protected; snapshots are immutable by SnapshotID. If a
// snapshot with the same ID already exists, StoreSnapshot returns that
// existing record rather than replacing it.
type LiveBackend interface {
	LoadState(ctx context.Context) (LiveStateRecord, error)
	CompareAndSwapState(ctx context.Context, current LiveStateRecord, next LiveState) (bool, error)
	StoreSnapshot(ctx context.Context, snapshot RetainedSnapshot, ttl time.Duration) (RetainedSnapshot, error)
	LoadSnapshot(ctx context.Context, snapshotID string) (RetainedSnapshot, bool, error)
}

type memorySnapshot struct {
	snapshot RetainedSnapshot
	expires  time.Time
}

// MemoryBackend is the dependency-free LiveBackend used by standalone Sahara.
// It is safe for concurrent use within one process.
type MemoryBackend struct {
	mu        sync.Mutex
	state     LiveState
	revision  uint64
	exists    bool
	snapshots map[string]memorySnapshot
	now       func() time.Time
}

// NewMemoryBackend constructs an empty in-process catalog backend.
func NewMemoryBackend() *MemoryBackend {
	return newMemoryBackend(time.Now)
}

func newMemoryBackend(now func() time.Time) *MemoryBackend {
	return &MemoryBackend{
		snapshots: make(map[string]memorySnapshot),
		now:       now,
	}
}

func (b *MemoryBackend) LoadState(context.Context) (LiveStateRecord, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	state, err := cloneLiveState(b.state)
	if err != nil {
		return LiveStateRecord{}, err
	}
	return LiveStateRecord{
		State:  state,
		Token:  []byte(strconv.FormatUint(b.revision, 10)),
		Exists: b.exists,
	}, nil
}

func (b *MemoryBackend) CompareAndSwapState(_ context.Context, current LiveStateRecord, next LiveState) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if current.Exists != b.exists || string(current.Token) != strconv.FormatUint(b.revision, 10) {
		return false, nil
	}
	cloned, err := cloneLiveState(next)
	if err != nil {
		return false, err
	}
	b.state = cloned
	b.exists = true
	b.revision++
	return true, nil
}

func (b *MemoryBackend) StoreSnapshot(_ context.Context, snapshot RetainedSnapshot, ttl time.Duration) (RetainedSnapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now().UTC()
	b.pruneSnapshots(now)
	if existing, ok := b.snapshots[snapshot.SnapshotID]; ok {
		return cloneSnapshot(existing.snapshot)
	}
	cloned, err := cloneSnapshot(snapshot)
	if err != nil {
		return RetainedSnapshot{}, err
	}
	b.snapshots[snapshot.SnapshotID] = memorySnapshot{snapshot: cloned, expires: now.Add(ttl)}
	return cloneSnapshot(cloned)
}

func (b *MemoryBackend) LoadSnapshot(_ context.Context, snapshotID string) (RetainedSnapshot, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now().UTC()
	b.pruneSnapshots(now)
	stored, ok := b.snapshots[snapshotID]
	if !ok {
		return RetainedSnapshot{}, false, nil
	}
	cloned, err := cloneSnapshot(stored.snapshot)
	return cloned, true, err
}

func (b *MemoryBackend) pruneSnapshots(now time.Time) {
	for id, stored := range b.snapshots {
		if !now.Before(stored.expires) {
			delete(b.snapshots, id)
		}
	}
}

func cloneLiveState(state LiveState) (LiveState, error) {
	if state.SchemaVersion == "" && len(state.Publications) == 0 && len(state.ProviderRoutes) == 0 && len(state.Tombstones) == 0 {
		return LiveState{}, nil
	}
	data, err := json.Marshal(state)
	if err != nil {
		return LiveState{}, fmt.Errorf("catalog: clone live state: %w", err)
	}
	var cloned LiveState
	if err := json.Unmarshal(data, &cloned); err != nil {
		return LiveState{}, fmt.Errorf("catalog: clone live state: %w", err)
	}
	return cloned, nil
}

func cloneSnapshot(snapshot RetainedSnapshot) (RetainedSnapshot, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: clone retained snapshot: %w", err)
	}
	var cloned RetainedSnapshot
	if err := json.Unmarshal(data, &cloned); err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: clone retained snapshot: %w", err)
	}
	return cloned, nil
}

var _ LiveBackend = (*MemoryBackend)(nil)
