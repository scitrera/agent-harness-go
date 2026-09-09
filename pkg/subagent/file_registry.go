// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const fileRegistrySchemaVersion = 1

var (
	ErrCorruptRegistry  = errors.New("subagent: corrupt lifecycle registry")
	ErrDeletedLifecycle = errors.New("subagent: deleted lifecycle cannot be resumed")
)

// Registry is the durable lifecycle surface used by the runner and session
// snapshot projection.
type Registry interface {
	LifecycleObserver
	ListSubagents(ctx context.Context, workspaceID, parentSessionID string) ([]spec.SessionSubagentRecord, error)
	RecoverInterrupted(ctx context.Context, at time.Time) error
}

// TaskRecoveryRegistry is implemented by distributed registries that can
// reconcile incomplete projections with their execution authority during owner
// restart. Local registries deliberately retain process-owner interruption
// recovery and do not need this extension.
type TaskRecoveryRegistry interface {
	Registry
	RecoverInterruptedWithTasks(ctx context.Context, at time.Time, tasks TaskBackend) error
}

// RecoverInterrupted marks children left admitted/running by a prior process as
// interrupted. It validates the complete registry set before writing any file,
// so corrupt state fails startup without silently skipping another session.
func (r *FileRegistry) RecoverInterrupted(ctx context.Context, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if at.IsZero() {
		return errors.New("subagent: recovery timestamp is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	type loadedState struct {
		state fileRegistryState
	}
	var loaded []loadedState
	root := filepath.Join(r.stateDir, "session-state", "workspaces")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" || filepath.Base(filepath.Dir(path)) != "subagents" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var state fileRegistryState
		if err := json.Unmarshal(data, &state); err != nil {
			return fmt.Errorf("%w: decode %s: %v", ErrCorruptRegistry, path, err)
		}
		if state.SchemaVersion != fileRegistrySchemaVersion || state.WorkspaceID == "" || state.ParentSessionID == "" || filepath.Clean(path) != r.path(state.WorkspaceID, state.ParentSessionID) {
			return fmt.Errorf("%w: stored lifecycle registry path, identity, or version does not match", ErrCorruptRegistry)
		}
		if err := state.State.Validate(state.ParentSessionID); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptRegistry, err)
		}
		loaded = append(loaded, loadedState{state: state})
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("subagent: scan lifecycle registries: %w", err)
	}
	timestamp := at.UTC().Format(time.RFC3339Nano)
	for _, item := range loaded {
		changed := false
		for i := range item.state.State.Records {
			record := &item.state.State.Records[i]
			if record.Status != spec.SessionSubagentAdmitted && record.Status != spec.SessionSubagentRunning {
				continue
			}
			record.Status = spec.SessionSubagentInterrupted
			record.UpdatedAt = timestamp
			record.CompletedAt = timestamp
			changed = true
		}
		if changed {
			if err := item.state.State.Validate(item.state.ParentSessionID); err != nil {
				return fmt.Errorf("subagent: recover lifecycle registry: %w", err)
			}
			if err := r.persistLocked(item.state); err != nil {
				return err
			}
		}
	}
	return nil
}

// FileRegistry stores one parent-scoped registry per workspace. It is safe for
// concurrent use in one process; a state directory has one writing process.
type FileRegistry struct {
	stateDir string
	mu       sync.Mutex
}

type fileRegistryState struct {
	SchemaVersion   uint32                     `json:"schema_version"`
	WorkspaceID     string                     `json:"workspace_id"`
	ParentSessionID string                     `json:"parent_session_id"`
	State           spec.SessionSubagentsState `json:"state"`
}

func NewFileRegistry(stateDir string) (*FileRegistry, error) {
	if stateDir == "" {
		return nil, errors.New("subagent: lifecycle registry state directory is required")
	}
	return &FileRegistry{stateDir: filepath.Clean(stateDir)}, nil
}

func (r *FileRegistry) path(workspaceID, parentSessionID string) string {
	return filepath.Join(
		r.stateDir,
		"session-state",
		"workspaces",
		workspacepkg.PathSegment(workspaceID),
		"subagents",
		workspacepkg.PathSegment(parentSessionID)+".json",
	)
}

func (r *FileRegistry) loadLocked(workspaceID, parentSessionID string) (fileRegistryState, error) {
	state := newRegistryState(workspaceID, parentSessionID)
	data, err := os.ReadFile(r.path(workspaceID, parentSessionID))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return fileRegistryState{}, fmt.Errorf("subagent: read lifecycle registry: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return fileRegistryState{}, fmt.Errorf("%w: decode %s: %v", ErrCorruptRegistry, r.path(workspaceID, parentSessionID), err)
	}
	if state.SchemaVersion != fileRegistrySchemaVersion || state.WorkspaceID != workspaceID || state.ParentSessionID != parentSessionID {
		return fileRegistryState{}, fmt.Errorf("%w: stored lifecycle registry identity or version does not match", ErrCorruptRegistry)
	}
	if err := state.State.Validate(parentSessionID); err != nil {
		return fileRegistryState{}, fmt.Errorf("%w: %v", ErrCorruptRegistry, err)
	}
	return state, nil
}

func (r *FileRegistry) persistLocked(state fileRegistryState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("subagent: encode lifecycle registry: %w", err)
	}
	data = append(data, '\n')
	if err := atomicfile.Write(r.path(state.WorkspaceID, state.ParentSessionID), data, 0o600); err != nil {
		return fmt.Errorf("subagent: persist lifecycle registry: %w", err)
	}
	return nil
}

// ObserveSubagent atomically merges one lifecycle transition.
func (r *FileRegistry) ObserveSubagent(ctx context.Context, event LifecycleEvent) error {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.loadLocked(event.WorkspaceID, record.ParentSessionID)
	if err != nil {
		return err
	}
	if err := putSubagentState(&state, record); err != nil {
		return err
	}
	return r.persistLocked(state)
}

func newRegistryState(workspaceID, parentSessionID string) fileRegistryState {
	return fileRegistryState{
		SchemaVersion:   fileRegistrySchemaVersion,
		WorkspaceID:     workspaceID,
		ParentSessionID: parentSessionID,
		State: spec.SessionSubagentsState{
			SchemaRevision: spec.SessionSubagentsStateSchemaRevision,
			Records:        []spec.SessionSubagentRecord{},
		},
	}
}

func putSubagentState(state *fileRegistryState, record spec.SessionSubagentRecord) error {
	index := sort.Search(len(state.State.Records), func(i int) bool { return state.State.Records[i].ID >= record.ID })
	if index < len(state.State.Records) && state.State.Records[index].ID == record.ID {
		if state.State.Records[index].Status == spec.SessionSubagentDeleted && record.Status != spec.SessionSubagentDeleted {
			return ErrDeletedLifecycle
		}
		record = mergeSubagentRecord(state.State.Records[index], record)
		state.State.Records[index] = record
	} else {
		state.State.Records = append(state.State.Records, spec.SessionSubagentRecord{})
		copy(state.State.Records[index+1:], state.State.Records[index:])
		state.State.Records[index] = record
	}
	if err := state.State.Validate(record.ParentSessionID); err != nil {
		return fmt.Errorf("subagent: lifecycle transition: %w", err)
	}
	return nil
}

// ListSubagents returns a deterministic deep copy for snapshot projection.
func (r *FileRegistry) ListSubagents(ctx context.Context, workspaceID, parentSessionID string) ([]spec.SessionSubagentRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workspaceID == "" || parentSessionID == "" {
		return nil, errors.New("subagent: workspace and parent session are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.loadLocked(workspaceID, parentSessionID)
	if err != nil {
		return nil, err
	}
	records := make([]spec.SessionSubagentRecord, len(state.State.Records))
	for i, record := range state.State.Records {
		records[i] = cloneSubagentRecord(record)
	}
	return records, nil
}

func mergeSubagentRecord(existing, next spec.SessionSubagentRecord) spec.SessionSubagentRecord {
	if existing.CreatedAt != "" {
		next.CreatedAt = existing.CreatedAt
	}
	if next.TaskID == "" {
		next.TaskID = existing.TaskID
	}
	if next.Name == "" {
		next.Name = existing.Name
	}
	if next.Kind == "" {
		next.Kind = existing.Kind
	}
	if next.Model == "" {
		next.Model = existing.Model
	}
	if next.ParentUsage == nil {
		next.ParentUsage = existing.ParentUsage
	}
	if next.ChildUsage == nil {
		next.ChildUsage = existing.ChildUsage
	}
	if next.Status == spec.SessionSubagentAdmitted || next.Status == spec.SessionSubagentRunning {
		next.CompletedAt = ""
		next.DeletedAt = ""
	}
	return next
}

func cloneSubagentRecord(record spec.SessionSubagentRecord) spec.SessionSubagentRecord {
	record.ParentUsage = cloneRawMap(record.ParentUsage)
	record.ChildUsage = cloneRawMap(record.ChildUsage)
	record.Extra = cloneRawMap(record.Extra)
	return record
}

func cloneRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	if values == nil {
		return nil
	}
	cloned := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		cloned[key] = append(json.RawMessage(nil), value...)
	}
	return cloned
}

var _ Registry = (*FileRegistry)(nil)
