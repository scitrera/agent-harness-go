package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	defaultCASStorePrefix  = "agent-harness/session-state/goals/v1"
	defaultCASStoreRetries = 32
)

var ErrCASRetryLimit = errors.New("goal: distributed lifecycle update retry limit exceeded")

type CASStoreConfig struct {
	Blobs      casblob.Store
	Prefix     string
	MaxRetries int
}

// CASStore persists one complete goal projection per workspace/session and uses
// optimistic compare-and-swap so independent agent processes cannot overwrite
// concurrent goal updates.
type CASStore struct {
	blobs      casblob.Store
	prefix     string
	maxRetries int
}

func NewCASStore(config CASStoreConfig) (*CASStore, error) {
	if config.Blobs == nil {
		return nil, errors.New("goal: CAS lifecycle store requires a blob store")
	}
	prefix := strings.Trim(strings.TrimSpace(config.Prefix), "/")
	if prefix == "" {
		prefix = defaultCASStorePrefix
	}
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultCASStoreRetries
	}
	return &CASStore{blobs: config.Blobs, prefix: prefix, maxRetries: maxRetries}, nil
}

func (s *CASStore) key(workspaceID, sessionID string) string {
	return strings.Join([]string{
		s.prefix,
		"workspaces", workspacepkg.PathSegment(workspaceID),
		"sessions", workspacepkg.PathSegment(sessionID),
	}, "/")
}

func newGoalState(workspaceID, sessionID string) fileStoreState {
	return fileStoreState{
		SchemaVersion: fileStoreSchemaVersion,
		WorkspaceID:   workspaceID,
		SessionID:     sessionID,
		State: spec.SessionGoalsState{
			SchemaRevision: spec.SessionGoalsStateSchemaRevision,
			Records:        []spec.SessionGoalRecord{},
		},
	}
}

func (s *CASStore) read(ctx context.Context, workspaceID, sessionID string) ([]byte, fileStoreState, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, fileStoreState{}, false, err
	}
	if workspaceID == "" || sessionID == "" {
		return nil, fileStoreState{}, false, errors.New("goal: workspace and session are required")
	}
	raw, found, err := s.blobs.Read(ctx, s.key(workspaceID, sessionID))
	if err != nil {
		return nil, fileStoreState{}, false, fmt.Errorf("goal: read distributed lifecycle store: %w", err)
	}
	if !found {
		return nil, newGoalState(workspaceID, sessionID), false, nil
	}
	if len(raw) == 0 {
		return nil, fileStoreState{}, false, fmt.Errorf("%w: distributed goal value is empty", ErrCorruptStore)
	}
	var state fileStoreState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fileStoreState{}, false, fmt.Errorf("%w: decode distributed goal state: %v", ErrCorruptStore, err)
	}
	if state.SchemaVersion != fileStoreSchemaVersion || state.WorkspaceID != workspaceID || state.SessionID != sessionID {
		return nil, fileStoreState{}, false, fmt.Errorf("%w: distributed goal identity or version does not match", ErrCorruptStore)
	}
	if err := state.State.Validate(); err != nil {
		return nil, fileStoreState{}, false, fmt.Errorf("%w: %v", ErrCorruptStore, err)
	}
	if err := validateAccountedMessages(state); err != nil {
		return nil, fileStoreState{}, false, err
	}
	return append([]byte(nil), raw...), state, true, nil
}

func (s *CASStore) commit(ctx context.Context, workspaceID, sessionID string, previous []byte, existed bool, state fileStoreState) (bool, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return false, fmt.Errorf("goal: encode distributed lifecycle store: %w", err)
	}
	if existed {
		swapped, err := s.blobs.CompareAndSwap(ctx, s.key(workspaceID, sessionID), previous, encoded)
		if err != nil {
			return false, fmt.Errorf("goal: compare-and-swap distributed lifecycle store: %w", err)
		}
		return swapped, nil
	}
	created, err := s.blobs.Create(ctx, s.key(workspaceID, sessionID), encoded)
	if err != nil {
		return false, fmt.Errorf("goal: create distributed lifecycle store: %w", err)
	}
	return created, nil
}

func (s *CASStore) PutGoal(ctx context.Context, workspaceID, sessionID string, record spec.SessionGoalRecord) error {
	record = cloneGoalRecord(record)
	return s.MutateGoals(ctx, workspaceID, sessionID, func(state *spec.SessionGoalsState) error {
		return putGoalState(state, record)
	})
}

// MutateGoals applies one optimistic CAS mutation. The callback may run more
// than once when another replica wins the compare-and-swap.
func (s *CASStore) MutateGoals(ctx context.Context, workspaceID, sessionID string, mutate Mutation) error {
	if mutate == nil {
		return errors.New("goal: mutation is required")
	}
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		previous, state, found, err := s.read(ctx, workspaceID, sessionID)
		if err != nil {
			return err
		}
		if err := mutate(&state.State); err != nil {
			return err
		}
		if err := state.State.Validate(); err != nil {
			return fmt.Errorf("goal: lifecycle mutation: %w", err)
		}
		committed, err := s.commit(ctx, workspaceID, sessionID, previous, found, state)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return fmt.Errorf("%w: put %q after %d attempts", ErrCASRetryLimit, s.key(workspaceID, sessionID), s.maxRetries)
}

func (s *CASStore) AccountGoalUsage(ctx context.Context, workspaceID, sessionID, goalID, assistantMessageID string, tokenDelta uint64, updatedAt string) (spec.SessionGoalRecord, bool, error) {
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		previous, state, found, err := s.read(ctx, workspaceID, sessionID)
		if err != nil {
			return spec.SessionGoalRecord{}, false, err
		}
		record, accounted, err := accountGoalUsageState(&state, goalID, assistantMessageID, tokenDelta, updatedAt)
		if err != nil || !accounted {
			return record, accounted, err
		}
		committed, err := s.commit(ctx, workspaceID, sessionID, previous, found, state)
		if err != nil {
			return spec.SessionGoalRecord{}, false, err
		}
		if committed {
			return record, true, nil
		}
	}
	return spec.SessionGoalRecord{}, false, fmt.Errorf("%w: account usage for %q after %d attempts", ErrCASRetryLimit, s.key(workspaceID, sessionID), s.maxRetries)
}

func (s *CASStore) ListGoals(ctx context.Context, workspaceID, sessionID string) ([]spec.SessionGoalRecord, error) {
	_, state, _, err := s.read(ctx, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	records := make([]spec.SessionGoalRecord, len(state.State.Records))
	for i, record := range state.State.Records {
		records[i] = cloneGoalRecord(record)
	}
	return records, nil
}

var _ Store = (*CASStore)(nil)
var _ AtomicStore = (*CASStore)(nil)
