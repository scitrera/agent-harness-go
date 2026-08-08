// Package goal provides durable, workspace-isolated goal lifecycle storage.
package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// FileStore stores one goal set per workspace/session. It is concurrency-safe
// within one process; a state directory has one writing process.
type FileStore struct {
	stateDir string
	mu       sync.Mutex
}

type fileStoreState struct {
	SchemaVersion uint32                 `json:"schema_version"`
	WorkspaceID   string                 `json:"workspace_id"`
	SessionID     string                 `json:"session_id"`
	State         spec.SessionGoalsState `json:"state"`
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if workspaceID == "" || sessionID == "" {
		return errors.New("goal: workspace and session are required")
	}
	record = cloneGoalRecord(record)
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadLocked(workspaceID, sessionID)
	if err != nil {
		return err
	}
	if err := putGoalState(&state, record); err != nil {
		return err
	}
	return s.persistLocked(state)
}

func putGoalState(state *fileStoreState, record spec.SessionGoalRecord) error {
	index := sort.Search(len(state.State.Records), func(i int) bool { return state.State.Records[i].ID >= record.ID })
	if index < len(state.State.Records) && state.State.Records[index].ID == record.ID {
		if state.State.Records[index].CreatedAt != "" {
			record.CreatedAt = state.State.Records[index].CreatedAt
		}
		state.State.Records[index] = record
	} else {
		state.State.Records = append(state.State.Records, spec.SessionGoalRecord{})
		copy(state.State.Records[index+1:], state.State.Records[index:])
		state.State.Records[index] = record
	}
	if err := state.State.Validate(); err != nil {
		return fmt.Errorf("goal: lifecycle transition: %w", err)
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
