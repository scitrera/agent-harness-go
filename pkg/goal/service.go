package goal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/ids"
)

const (
	maxObjectiveRunes       = 4000
	defaultGoalIDPrefix     = "goal-"
	verifierEvidencePrefix  = "verifier:"
	maxAccountedMessageRefs = 4096
)

var (
	ErrNoGoal            = errors.New("goal: no matching goal")
	ErrGoalExists        = errors.New("goal: generated goal ID already exists")
	ErrOpenGoalExists    = errors.New("goal: an open goal already exists")
	ErrMultipleOpenGoals = errors.New("goal: multiple open goals violate the session invariant")
	ErrInvalidTransition = errors.New("goal: invalid lifecycle transition")
)

// Service is the portable host API shared by model-facing tools and runtime
// continuation accounting. Mutations require an AtomicStore so one session
// cannot acquire competing open goals under concurrent replicas.
type Service struct {
	store AtomicStore
	now   func() time.Time
	newID func() (string, error)
}

type ServiceConfig struct {
	Store Store
	Now   func() time.Time
	NewID func() (string, error)
}

func NewService(config ServiceConfig) (*Service, error) {
	if config.Store == nil {
		return nil, errors.New("goal: service requires a store")
	}
	store, ok := config.Store.(AtomicStore)
	if !ok {
		return nil, errors.New("goal: service requires an atomic store")
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	newID := config.NewID
	if newID == nil {
		newID = func() (string, error) { return ids.New(defaultGoalIDPrefix) }
	}
	return &Service{store: store, now: now, newID: newID}, nil
}

type CreateInput struct {
	Objective   string
	TokenBudget *uint64
}

// CreateGoal creates the one open goal allowed in a workspace/session. Terminal
// historical goals remain available through ListGoals.
func (s *Service) CreateGoal(ctx context.Context, workspaceID, sessionID string, input CreateInput) (spec.SessionGoalRecord, error) {
	objective := strings.TrimSpace(input.Objective)
	if objective == "" {
		return spec.SessionGoalRecord{}, errors.New("goal: objective is required")
	}
	if utf8.RuneCountInString(objective) > maxObjectiveRunes {
		return spec.SessionGoalRecord{}, fmt.Errorf("goal: objective exceeds %d characters", maxObjectiveRunes)
	}
	if input.TokenBudget != nil && (*input.TokenBudget == 0 || *input.TokenBudget > spec.SessionMaxSequence) {
		return spec.SessionGoalRecord{}, errors.New("goal: token budget must be a positive JSON-safe integer")
	}
	id, err := s.newID()
	if err != nil {
		return spec.SessionGoalRecord{}, err
	}
	now := timestamp(s.now())
	record := spec.SessionGoalRecord{
		ID: id, Objective: objective, Status: spec.SessionGoalActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if input.TokenBudget != nil {
		budget := *input.TokenBudget
		record.TokenBudget = &budget
	}
	err = s.store.MutateGoals(ctx, workspaceID, sessionID, func(state *spec.SessionGoalsState) error {
		for _, existing := range state.Records {
			if existing.ID == record.ID {
				return ErrGoalExists
			}
		}
		if _, err := openGoal(state.Records); err != nil {
			return err
		} else if hasOpenGoal(state.Records) {
			return ErrOpenGoalExists
		}
		return putGoalState(state, record)
	})
	if err != nil {
		return spec.SessionGoalRecord{}, err
	}
	return cloneGoalRecord(record), nil
}

// ListGoals returns the durable deterministic history for one session.
func (s *Service) ListGoals(ctx context.Context, workspaceID, sessionID string) ([]spec.SessionGoalRecord, error) {
	return s.store.ListGoals(ctx, workspaceID, sessionID)
}

// Goal returns a goal by stable ID.
func (s *Service) Goal(ctx context.Context, workspaceID, sessionID, goalID string) (spec.SessionGoalRecord, error) {
	records, err := s.store.ListGoals(ctx, workspaceID, sessionID)
	if err != nil {
		return spec.SessionGoalRecord{}, err
	}
	for _, record := range records {
		if record.ID == goalID {
			return record, nil
		}
	}
	return spec.SessionGoalRecord{}, ErrNoGoal
}

// CurrentGoal returns the unique open goal, or the most recently updated
// terminal goal when the session has no open work. A nil record means the
// session has never had a goal.
func (s *Service) CurrentGoal(ctx context.Context, workspaceID, sessionID string) (*spec.SessionGoalRecord, error) {
	records, err := s.store.ListGoals(ctx, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	if current, err := openGoal(records); err != nil || current != nil {
		return current, err
	}
	var latest *spec.SessionGoalRecord
	var latestAt time.Time
	for i := range records {
		record := records[i]
		updatedAt, parseErr := time.Parse(time.RFC3339Nano, record.UpdatedAt)
		if parseErr != nil {
			return nil, fmt.Errorf("goal: parse validated updated_at: %w", parseErr)
		}
		if latest == nil || updatedAt.After(latestAt) || (updatedAt.Equal(latestAt) && record.ID > latest.ID) {
			copy := cloneGoalRecord(record)
			latest = &copy
			latestAt = updatedAt
		}
	}
	return latest, nil
}

// OpenGoal returns the unique non-terminal goal, if any.
func (s *Service) OpenGoal(ctx context.Context, workspaceID, sessionID string) (*spec.SessionGoalRecord, error) {
	records, err := s.store.ListGoals(ctx, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	return openGoal(records)
}

type UpdateInput struct {
	ID            string
	Status        spec.SessionGoalStatus
	Evidence      []string
	BlockedReason string
}

// UpdateGoal applies a validated explicit lifecycle transition. Objective,
// budget, and usage are host-owned after creation.
func (s *Service) UpdateGoal(ctx context.Context, workspaceID, sessionID string, input UpdateInput) (spec.SessionGoalRecord, error) {
	goalID := strings.TrimSpace(input.ID)
	if goalID == "" {
		current, err := s.OpenGoal(ctx, workspaceID, sessionID)
		if err != nil {
			return spec.SessionGoalRecord{}, err
		}
		if current == nil {
			return spec.SessionGoalRecord{}, ErrNoGoal
		}
		goalID = current.ID
	}
	var updated spec.SessionGoalRecord
	err := s.mutateGoal(ctx, workspaceID, sessionID, goalID, func(record *spec.SessionGoalRecord) error {
		status := input.Status
		if status == "" {
			status = record.Status
		}
		if !validTransition(record.Status, status) {
			return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, record.Status, status)
		}
		if status == spec.SessionGoalBlocked && strings.TrimSpace(input.BlockedReason) == "" {
			return errors.New("goal: blocked status requires blocked_reason")
		}
		record.Status = status
		record.Evidence = mergeEvidence(record.Evidence, input.Evidence)
		record.UpdatedAt = timestamp(s.now())
		switch status {
		case spec.SessionGoalCompleted, spec.SessionGoalCancelled:
			record.CompletedAt = record.UpdatedAt
			record.BlockedReason = ""
		case spec.SessionGoalBlocked:
			record.CompletedAt = ""
			record.BlockedReason = strings.TrimSpace(input.BlockedReason)
		default:
			record.CompletedAt = ""
			record.BlockedReason = ""
		}
		updated = cloneGoalRecord(*record)
		return nil
	})
	if err != nil {
		return spec.SessionGoalRecord{}, err
	}
	return updated, nil
}

// AccountUsage charges one finalized assistant message at most once, including
// a turn that explicitly completed the goal before its terminal response.
func (s *Service) AccountUsage(ctx context.Context, workspaceID, sessionID, goalID, assistantMessageID string, tokenDelta uint64) (spec.SessionGoalRecord, bool, error) {
	if strings.TrimSpace(assistantMessageID) == "" {
		return spec.SessionGoalRecord{}, false, errors.New("goal: assistant message id is required for idempotent accounting")
	}
	return s.store.AccountGoalUsage(
		ctx, workspaceID, sessionID, goalID, assistantMessageID, tokenDelta, timestamp(s.now()),
	)
}

func (s *Service) mutateGoal(ctx context.Context, workspaceID, sessionID, goalID string, mutate func(*spec.SessionGoalRecord) error) error {
	return s.store.MutateGoals(ctx, workspaceID, sessionID, func(state *spec.SessionGoalsState) error {
		for i := range state.Records {
			if state.Records[i].ID == goalID {
				return mutate(&state.Records[i])
			}
		}
		return ErrNoGoal
	})
}

func openGoal(records []spec.SessionGoalRecord) (*spec.SessionGoalRecord, error) {
	var current *spec.SessionGoalRecord
	for _, record := range records {
		if isTerminal(record.Status) {
			continue
		}
		if current != nil {
			return nil, ErrMultipleOpenGoals
		}
		copy := cloneGoalRecord(record)
		current = &copy
	}
	return current, nil
}

func hasOpenGoal(records []spec.SessionGoalRecord) bool {
	for _, record := range records {
		if !isTerminal(record.Status) {
			return true
		}
	}
	return false
}

func isTerminal(status spec.SessionGoalStatus) bool {
	return status == spec.SessionGoalCompleted || status == spec.SessionGoalCancelled
}

func validTransition(from, to spec.SessionGoalStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case spec.SessionGoalPending:
		return to == spec.SessionGoalActive || to == spec.SessionGoalBlocked || to == spec.SessionGoalCancelled
	case spec.SessionGoalActive:
		return to == spec.SessionGoalCompleted || to == spec.SessionGoalBlocked || to == spec.SessionGoalCancelled
	case spec.SessionGoalBlocked:
		return to == spec.SessionGoalActive || to == spec.SessionGoalCancelled
	default:
		return false
	}
}

func mergeEvidence(existing, added []string) []string {
	out := append([]string(nil), existing...)
	for _, evidence := range added {
		evidence = strings.TrimSpace(evidence)
		if evidence != "" && !slices.Contains(out, evidence) {
			out = append(out, evidence)
		}
	}
	return out
}

func timestamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
