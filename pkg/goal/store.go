// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package goal provides durable, workspace-isolated goal lifecycle storage.
package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const fileStoreSchemaVersion = 1

var ErrCorruptStore = errors.New("goal: corrupt lifecycle store")

// Store is the typed durable goal surface used by hosts and session snapshots.
type Store interface {
	PutGoal(ctx context.Context, workspaceID, sessionID string, record spec.SessionGoalRecord) error
	ListGoals(ctx context.Context, workspaceID, sessionID string) ([]spec.SessionGoalRecord, error)
}

// Mutation updates one complete goal projection atomically. Implementations may
// retry the callback after an optimistic-concurrency conflict, so it must not
// perform external side effects.
type Mutation func(state *spec.SessionGoalsState) error

// AtomicStore is the additive mutation surface used by the goal service. The
// original Store remains sufficient for read-only snapshot projection and
// compatibility with external backends.
type AtomicStore interface {
	Store
	MutateGoals(ctx context.Context, workspaceID, sessionID string, mutate Mutation) error
	AccountGoalUsage(ctx context.Context, workspaceID, sessionID, goalID, assistantMessageID string, tokenDelta uint64, updatedAt string) (spec.SessionGoalRecord, bool, error)
}

// FileStore stores one goal set per workspace/session. It is concurrency-safe
// within one process; a state directory has one writing process.
type FileStore struct {
	stateDir string
	mu       sync.Mutex
}

type fileStoreState struct {
	SchemaVersion     uint32                 `json:"schema_version"`
	WorkspaceID       string                 `json:"workspace_id"`
	SessionID         string                 `json:"session_id"`
	State             spec.SessionGoalsState `json:"state"`
	AccountedMessages map[string][]string    `json:"accounted_messages,omitempty"`
}

func NewFileStore(stateDir string) (*FileStore, error) {
	if stateDir == "" {
		return nil, errors.New("goal: lifecycle store state directory is required")
	}
	return &FileStore{stateDir: filepath.Clean(stateDir)}, nil
}

func (s *FileStore) path(workspaceID, sessionID string) string {
	return filepath.Join(
		s.stateDir,
		"session-state",
		"workspaces",
		workspacepkg.PathSegment(workspaceID),
		"goals",
		workspacepkg.PathSegment(sessionID)+".json",
	)
}

func (s *FileStore) loadLocked(workspaceID, sessionID string) (fileStoreState, error) {
	state := fileStoreState{
		SchemaVersion: fileStoreSchemaVersion,
		WorkspaceID:   workspaceID,
		SessionID:     sessionID,
		State: spec.SessionGoalsState{
			SchemaRevision: spec.SessionGoalsStateSchemaRevision,
			Records:        []spec.SessionGoalRecord{},
		},
	}
	data, err := os.ReadFile(s.path(workspaceID, sessionID))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return fileStoreState{}, fmt.Errorf("goal: read lifecycle store: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return fileStoreState{}, fmt.Errorf("%w: decode %s: %v", ErrCorruptStore, s.path(workspaceID, sessionID), err)
	}
	if state.SchemaVersion != fileStoreSchemaVersion || state.WorkspaceID != workspaceID || state.SessionID != sessionID {
		return fileStoreState{}, fmt.Errorf("%w: stored lifecycle identity or version does not match", ErrCorruptStore)
	}
	if err := state.State.Validate(); err != nil {
		return fileStoreState{}, fmt.Errorf("%w: %v", ErrCorruptStore, err)
	}
	if err := validateAccountedMessages(state); err != nil {
		return fileStoreState{}, err
	}
	return state, nil
}

func (s *FileStore) persistLocked(state fileStoreState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("goal: encode lifecycle store: %w", err)
	}
	data = append(data, '\n')
	if err := atomicfile.Write(s.path(state.WorkspaceID, state.SessionID), data, 0o600); err != nil {
		return fmt.Errorf("goal: persist lifecycle store: %w", err)
	}
	return nil
}

// PutGoal atomically creates or replaces one stable goal record.
func (s *FileStore) PutGoal(ctx context.Context, workspaceID, sessionID string, record spec.SessionGoalRecord) error {
	record = cloneGoalRecord(record)
	return s.MutateGoals(ctx, workspaceID, sessionID, func(state *spec.SessionGoalsState) error {
		return putGoalState(state, record)
	})
}

// MutateGoals applies one atomic projection mutation under the file store's
// process lock and persists it only after the complete state validates.
func (s *FileStore) MutateGoals(ctx context.Context, workspaceID, sessionID string, mutate Mutation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if workspaceID == "" || sessionID == "" {
		return errors.New("goal: workspace and session are required")
	}
	if mutate == nil {
		return errors.New("goal: mutation is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadLocked(workspaceID, sessionID)
	if err != nil {
		return err
	}
	if err := mutate(&state.State); err != nil {
		return err
	}
	if err := state.State.Validate(); err != nil {
		return fmt.Errorf("goal: lifecycle mutation: %w", err)
	}
	return s.persistLocked(state)
}

// AccountGoalUsage atomically charges one assistant message at most once. The
// message IDs remain private store metadata rather than leaking through the
// portable SessionGoalRecord projection.
func (s *FileStore) AccountGoalUsage(ctx context.Context, workspaceID, sessionID, goalID, assistantMessageID string, tokenDelta uint64, updatedAt string) (spec.SessionGoalRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return spec.SessionGoalRecord{}, false, err
	}
	if workspaceID == "" || sessionID == "" {
		return spec.SessionGoalRecord{}, false, errors.New("goal: workspace and session are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadLocked(workspaceID, sessionID)
	if err != nil {
		return spec.SessionGoalRecord{}, false, err
	}
	record, accounted, err := accountGoalUsageState(&state, goalID, assistantMessageID, tokenDelta, updatedAt)
	if err != nil || !accounted {
		return record, accounted, err
	}
	if err := s.persistLocked(state); err != nil {
		return spec.SessionGoalRecord{}, false, err
	}
	return record, true, nil
}

func putGoalState(state *spec.SessionGoalsState, record spec.SessionGoalRecord) error {
	index := sort.Search(len(state.Records), func(i int) bool { return state.Records[i].ID >= record.ID })
	if index < len(state.Records) && state.Records[index].ID == record.ID {
		if state.Records[index].CreatedAt != "" {
			record.CreatedAt = state.Records[index].CreatedAt
		}
		state.Records[index] = record
	} else {
		state.Records = append(state.Records, spec.SessionGoalRecord{})
		copy(state.Records[index+1:], state.Records[index:])
		state.Records[index] = record
	}
	if err := state.Validate(); err != nil {
		return fmt.Errorf("goal: lifecycle transition: %w", err)
	}
	return nil
}

func accountGoalUsageState(state *fileStoreState, goalID, assistantMessageID string, tokenDelta uint64, updatedAt string) (spec.SessionGoalRecord, bool, error) {
	for i := range state.State.Records {
		record := &state.State.Records[i]
		if record.ID != goalID {
			continue
		}
		refs := state.AccountedMessages[goalID]
		if slices.Contains(refs, assistantMessageID) {
			return cloneGoalRecord(*record), false, nil
		}
		if len(refs) >= maxAccountedMessageRefs {
			return spec.SessionGoalRecord{}, false, errors.New("goal: accounted-message limit reached")
		}
		if tokenDelta > spec.SessionMaxSequence-record.TokenUsage {
			return spec.SessionGoalRecord{}, false, errors.New("goal: token accounting exceeds the JSON-safe range")
		}
		record.TokenUsage += tokenDelta
		record.UpdatedAt = updatedAt
		if state.AccountedMessages == nil {
			state.AccountedMessages = map[string][]string{}
		}
		state.AccountedMessages[goalID] = append(append([]string(nil), refs...), assistantMessageID)
		if err := state.State.Validate(); err != nil {
			return spec.SessionGoalRecord{}, false, fmt.Errorf("goal: usage accounting: %w", err)
		}
		return cloneGoalRecord(*record), true, nil
	}
	return spec.SessionGoalRecord{}, false, ErrNoGoal
}

func validateAccountedMessages(state fileStoreState) error {
	known := make(map[string]struct{}, len(state.State.Records))
	for _, record := range state.State.Records {
		known[record.ID] = struct{}{}
	}
	for goalID, refs := range state.AccountedMessages {
		if _, ok := known[goalID]; !ok {
			return fmt.Errorf("%w: accounted messages reference unknown goal %q", ErrCorruptStore, goalID)
		}
		if len(refs) > maxAccountedMessageRefs {
			return fmt.Errorf("%w: too many accounted messages for goal %q", ErrCorruptStore, goalID)
		}
		seen := make(map[string]struct{}, len(refs))
		for _, messageID := range refs {
			if messageID == "" {
				return fmt.Errorf("%w: empty accounted message ID", ErrCorruptStore)
			}
			if _, duplicate := seen[messageID]; duplicate {
				return fmt.Errorf("%w: duplicate accounted message ID", ErrCorruptStore)
			}
			seen[messageID] = struct{}{}
		}
	}
	return nil
}

// ListGoals returns a deterministic deep copy for snapshot projection.
func (s *FileStore) ListGoals(ctx context.Context, workspaceID, sessionID string) ([]spec.SessionGoalRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workspaceID == "" || sessionID == "" {
		return nil, errors.New("goal: workspace and session are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadLocked(workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	records := make([]spec.SessionGoalRecord, len(state.State.Records))
	for i, record := range state.State.Records {
		records[i] = cloneGoalRecord(record)
	}
	return records, nil
}

func cloneGoalRecord(record spec.SessionGoalRecord) spec.SessionGoalRecord {
	if record.TokenBudget != nil {
		budget := *record.TokenBudget
		record.TokenBudget = &budget
	}
	record.Evidence = append([]string(nil), record.Evidence...)
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

var _ Store = (*FileStore)(nil)
var _ AtomicStore = (*FileStore)(nil)
