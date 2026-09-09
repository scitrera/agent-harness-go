// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	defaultCASRegistryPrefix  = "agent-harness/session-state/subagents/v1"
	defaultCASRegistryRetries = 32
	casRegistryIndexVersion   = 1
)

var ErrCASRetryLimit = errors.New("subagent: distributed lifecycle update retry limit exceeded")

type CASRegistryConfig struct {
	Blobs      casblob.Store
	Prefix     string
	MaxRetries int
	// DeferRecovery makes RecoverInterrupted record the requested recovery and
	// apply it on the first later read/write. Aether workers use this because KV
	// becomes available only after Start; successful connection of their stable
	// agent identity proves the prior duplicate is no longer active.
	DeferRecovery bool
}

// CASRegistry persists one bounded-by-parent lifecycle projection per
// workspace/session. A small CAS-maintained identity index supports startup
// interruption recovery without requiring backend-specific key scans.
type CASRegistry struct {
	blobs          casblob.Store
	prefix         string
	maxRetries     int
	deferRecovery  bool
	recoveryMu     sync.Mutex
	pendingRecover time.Time
	pendingTasks   TaskBackend
}

type casRegistryRef struct {
	WorkspaceID     string `json:"workspace_id"`
	ParentSessionID string `json:"parent_session_id"`
}

type casRegistryIndex struct {
	SchemaVersion uint32           `json:"schema_version"`
	Refs          []casRegistryRef `json:"refs"`
}

func NewCASRegistry(config CASRegistryConfig) (*CASRegistry, error) {
	if config.Blobs == nil {
		return nil, errors.New("subagent: CAS lifecycle registry requires a blob store")
	}
	prefix := strings.Trim(strings.TrimSpace(config.Prefix), "/")
	if prefix == "" {
		prefix = defaultCASRegistryPrefix
	}
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultCASRegistryRetries
	}
	return &CASRegistry{
		blobs: config.Blobs, prefix: prefix, maxRetries: maxRetries, deferRecovery: config.DeferRecovery,
	}, nil
}

func (r *CASRegistry) key(workspaceID, parentSessionID string) string {
	return strings.Join([]string{
		r.prefix,
		"workspaces", workspacepkg.PathSegment(workspaceID),
		"parents", workspacepkg.PathSegment(parentSessionID),
	}, "/")
}

func (r *CASRegistry) indexKey() string { return r.prefix + "/index" }

func (r *CASRegistry) readState(ctx context.Context, workspaceID, parentSessionID string) ([]byte, fileRegistryState, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, fileRegistryState{}, false, err
	}
	if workspaceID == "" || parentSessionID == "" {
		return nil, fileRegistryState{}, false, errors.New("subagent: workspace and parent session are required")
	}
	raw, found, err := r.blobs.Read(ctx, r.key(workspaceID, parentSessionID))
	if err != nil {
		return nil, fileRegistryState{}, false, fmt.Errorf("subagent: read distributed lifecycle registry: %w", err)
	}
	if !found {
		return nil, newRegistryState(workspaceID, parentSessionID), false, nil
	}
	if len(raw) == 0 {
		return nil, fileRegistryState{}, false, fmt.Errorf("%w: distributed lifecycle value is empty", ErrCorruptRegistry)
	}
	var state fileRegistryState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fileRegistryState{}, false, fmt.Errorf("%w: decode distributed lifecycle state: %v", ErrCorruptRegistry, err)
	}
	if state.SchemaVersion != fileRegistrySchemaVersion || state.WorkspaceID != workspaceID || state.ParentSessionID != parentSessionID {
		return nil, fileRegistryState{}, false, fmt.Errorf("%w: distributed lifecycle identity or version does not match", ErrCorruptRegistry)
	}
	if err := state.State.Validate(parentSessionID); err != nil {
		return nil, fileRegistryState{}, false, fmt.Errorf("%w: %v", ErrCorruptRegistry, err)
	}
	return append([]byte(nil), raw...), state, true, nil
}

func (r *CASRegistry) commitState(ctx context.Context, workspaceID, parentSessionID string, previous []byte, existed bool, state fileRegistryState) (bool, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return false, fmt.Errorf("subagent: encode distributed lifecycle registry: %w", err)
	}
	if existed {
		swapped, err := r.blobs.CompareAndSwap(ctx, r.key(workspaceID, parentSessionID), previous, encoded)
		if err != nil {
			return false, fmt.Errorf("subagent: compare-and-swap distributed lifecycle registry: %w", err)
		}
		return swapped, nil
	}
	created, err := r.blobs.Create(ctx, r.key(workspaceID, parentSessionID), encoded)
	if err != nil {
		return false, fmt.Errorf("subagent: create distributed lifecycle registry: %w", err)
	}
	return created, nil
}

func compareRegistryRef(left, right casRegistryRef) int {
	if left.WorkspaceID < right.WorkspaceID {
		return -1
	}
	if left.WorkspaceID > right.WorkspaceID {
		return 1
	}
	if left.ParentSessionID < right.ParentSessionID {
		return -1
	}
	if left.ParentSessionID > right.ParentSessionID {
		return 1
	}
	return 0
}

func validateRegistryIndex(index casRegistryIndex) error {
	if index.SchemaVersion != casRegistryIndexVersion {
		return fmt.Errorf("%w: distributed lifecycle index schema revision is unsupported", ErrCorruptRegistry)
	}
	for i, ref := range index.Refs {
		if ref.WorkspaceID == "" || ref.ParentSessionID == "" {
			return fmt.Errorf("%w: distributed lifecycle index contains an empty identity", ErrCorruptRegistry)
		}
		if i > 0 && compareRegistryRef(index.Refs[i-1], ref) >= 0 {
			return fmt.Errorf("%w: distributed lifecycle index is not strictly sorted", ErrCorruptRegistry)
		}
	}
	return nil
}

func (r *CASRegistry) readIndex(ctx context.Context) ([]byte, casRegistryIndex, bool, error) {
	raw, found, err := r.blobs.Read(ctx, r.indexKey())
	if err != nil {
		return nil, casRegistryIndex{}, false, fmt.Errorf("subagent: read distributed lifecycle index: %w", err)
	}
	if !found {
		return nil, casRegistryIndex{SchemaVersion: casRegistryIndexVersion, Refs: []casRegistryRef{}}, false, nil
	}
	if len(raw) == 0 {
		return nil, casRegistryIndex{}, false, fmt.Errorf("%w: distributed lifecycle index is empty", ErrCorruptRegistry)
	}
	var index casRegistryIndex
	if err := json.Unmarshal(raw, &index); err != nil {
		return nil, casRegistryIndex{}, false, fmt.Errorf("%w: decode distributed lifecycle index: %v", ErrCorruptRegistry, err)
	}
	if err := validateRegistryIndex(index); err != nil {
		return nil, casRegistryIndex{}, false, err
	}
	return append([]byte(nil), raw...), index, true, nil
}

func (r *CASRegistry) ensureIndexed(ctx context.Context, ref casRegistryRef) error {
	for attempt := 0; attempt < r.maxRetries; attempt++ {
		previous, index, found, err := r.readIndex(ctx)
		if err != nil {
			return err
		}
		position := sort.Search(len(index.Refs), func(i int) bool { return compareRegistryRef(index.Refs[i], ref) >= 0 })
		if position < len(index.Refs) && compareRegistryRef(index.Refs[position], ref) == 0 {
			return nil
		}
		index.Refs = append(index.Refs, casRegistryRef{})
		copy(index.Refs[position+1:], index.Refs[position:])
		index.Refs[position] = ref
		encoded, err := json.Marshal(index)
		if err != nil {
			return fmt.Errorf("subagent: encode distributed lifecycle index: %w", err)
		}
		var committed bool
		if found {
			committed, err = r.blobs.CompareAndSwap(ctx, r.indexKey(), previous, encoded)
		} else {
			committed, err = r.blobs.Create(ctx, r.indexKey(), encoded)
		}
		if err != nil {
			return fmt.Errorf("subagent: update distributed lifecycle index: %w", err)
		}
		if committed {
			return nil
		}
	}
	return fmt.Errorf("%w: index %q after %d attempts", ErrCASRetryLimit, r.indexKey(), r.maxRetries)
}

func (r *CASRegistry) ObserveSubagent(ctx context.Context, event LifecycleEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.WorkspaceID == "" {
		return errors.New("subagent: lifecycle workspace is required")
	}
	record := cloneSubagentRecord(event.Record)
	if record.ParentSessionID == "" {
		return errors.New("subagent: lifecycle parent session is required")
	}
	// Validate before indexing so a malformed event cannot grow the recovery
	// index with an identity that will never have a valid state.
	candidate := newRegistryState(event.WorkspaceID, record.ParentSessionID)
	if err := putSubagentState(&candidate, record); err != nil {
		return err
	}
	if err := r.ensureDeferredRecovery(ctx); err != nil {
		return err
	}
	ref := casRegistryRef{WorkspaceID: event.WorkspaceID, ParentSessionID: record.ParentSessionID}
	// Index first: a crash may leave a harmless empty indexed ref, but can never
	// leave a lifecycle state invisible to later interruption recovery.
	if err := r.ensureIndexed(ctx, ref); err != nil {
		return err
	}
	for attempt := 0; attempt < r.maxRetries; attempt++ {
		previous, state, found, err := r.readState(ctx, ref.WorkspaceID, ref.ParentSessionID)
		if err != nil {
			return err
		}
		if err := putSubagentState(&state, record); err != nil {
			return err
		}
		committed, err := r.commitState(ctx, ref.WorkspaceID, ref.ParentSessionID, previous, found, state)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return fmt.Errorf("%w: observe %q after %d attempts", ErrCASRetryLimit, r.key(ref.WorkspaceID, ref.ParentSessionID), r.maxRetries)
}

func (r *CASRegistry) ListSubagents(ctx context.Context, workspaceID, parentSessionID string) ([]spec.SessionSubagentRecord, error) {
	if err := r.ensureDeferredRecovery(ctx); err != nil {
		return nil, err
	}
	_, state, _, err := r.readState(ctx, workspaceID, parentSessionID)
	if err != nil {
		return nil, err
	}
	records := make([]spec.SessionSubagentRecord, len(state.State.Records))
	for i, record := range state.State.Records {
		records[i] = cloneSubagentRecord(record)
	}
	return records, nil
}

func (r *CASRegistry) RecoverInterrupted(ctx context.Context, at time.Time) error {
	return r.recoverInterrupted(ctx, at, nil)
}

// RecoverInterruptedWithTasks reconciles unfinished projections against the
// durable execution task state. In deferred Aether mode the backend is retained
// with the timestamp and used by the first post-connect registry operation.
func (r *CASRegistry) RecoverInterruptedWithTasks(ctx context.Context, at time.Time, tasks TaskBackend) error {
	if tasks == nil {
		return errors.New("subagent: task-aware recovery requires a task backend")
	}
	return r.recoverInterrupted(ctx, at, tasks)
}

func (r *CASRegistry) recoverInterrupted(ctx context.Context, at time.Time, tasks TaskBackend) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if at.IsZero() {
		return errors.New("subagent: recovery timestamp is required")
	}
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	if r.deferRecovery {
		r.pendingRecover = at
		r.pendingTasks = tasks
		return nil
	}
	return r.recoverAll(ctx, at, tasks)
}

func (r *CASRegistry) ensureDeferredRecovery(ctx context.Context) error {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	if r.pendingRecover.IsZero() {
		return nil
	}
	if err := r.recoverAll(ctx, r.pendingRecover, r.pendingTasks); err != nil {
		return err
	}
	r.pendingRecover = time.Time{}
	r.pendingTasks = nil
	return nil
}

func (r *CASRegistry) recoverAll(ctx context.Context, at time.Time, tasks TaskBackend) error {
	_, index, _, err := r.readIndex(ctx)
	if err != nil {
		return err
	}
	for _, ref := range index.Refs {
		if err := r.recoverRef(ctx, ref, at, tasks); err != nil {
			return err
		}
	}
	return nil
}

func (r *CASRegistry) recoverRef(ctx context.Context, ref casRegistryRef, at time.Time, tasks TaskBackend) error {
	timestamp := at.UTC().Format(time.RFC3339Nano)
	for attempt := 0; attempt < r.maxRetries; attempt++ {
		previous, state, found, err := r.readState(ctx, ref.WorkspaceID, ref.ParentSessionID)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		changed := false
		for i := range state.State.Records {
			record := &state.State.Records[i]
			if record.Status != spec.SessionSubagentAdmitted && record.Status != spec.SessionSubagentRunning {
				continue
			}
			recoveredStatus := spec.SessionSubagentInterrupted
			if tasks != nil && record.TaskID != "" {
				recovery, err := tasks.Recover(ctx, record.TaskID)
				if err != nil {
					return fmt.Errorf("subagent: reconcile execution task %q: %w", record.TaskID, err)
				}
				switch recovery {
				case TaskRecoveryAdmitted:
					// State projection is monotonic. A briefly stale task read must
					// not move a locally observed running child back to admitted.
					if record.Status == spec.SessionSubagentAdmitted || record.Status == spec.SessionSubagentRunning {
						continue
					}
					recoveredStatus = spec.SessionSubagentAdmitted
				case TaskRecoveryRunning:
					if record.Status == spec.SessionSubagentRunning {
						continue
					}
					recoveredStatus = spec.SessionSubagentRunning
				case TaskRecoveryCompleted:
					recoveredStatus = spec.SessionSubagentCompleted
				case TaskRecoveryFailed:
					recoveredStatus = spec.SessionSubagentFailed
				case TaskRecoveryCancelled:
					recoveredStatus = spec.SessionSubagentCancelled
				case TaskRecoveryInterrupted:
					recoveredStatus = spec.SessionSubagentInterrupted
				default:
					return fmt.Errorf("subagent: reconcile execution task %q returned unsupported recovery %q", record.TaskID, recovery)
				}
			}
			record.Status = recoveredStatus
			record.UpdatedAt = timestamp
			if recoveredStatus == spec.SessionSubagentAdmitted || recoveredStatus == spec.SessionSubagentRunning {
				record.CompletedAt = ""
			} else {
				record.CompletedAt = timestamp
			}
			changed = true
		}
		if !changed {
			return nil
		}
		if err := state.State.Validate(ref.ParentSessionID); err != nil {
			return fmt.Errorf("subagent: recover distributed lifecycle registry: %w", err)
		}
		committed, err := r.commitState(ctx, ref.WorkspaceID, ref.ParentSessionID, previous, true, state)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return fmt.Errorf("%w: recover %q after %d attempts", ErrCASRetryLimit, r.key(ref.WorkspaceID, ref.ParentSessionID), r.maxRetries)
}

var _ Registry = (*CASRegistry)(nil)
var _ TaskRecoveryRegistry = (*CASRegistry)(nil)
