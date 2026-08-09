package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const continuationLedgerSchemaVersion = 1

var ErrCorruptLedger = errors.New("goal: corrupt continuation ledger")

type DecisionAction string

const (
	DecisionContinuationPlanned  DecisionAction = "continuation_planned"
	DecisionContinuationEnqueued DecisionAction = "continuation_enqueued"
	DecisionStopped              DecisionAction = "stopped"
	DecisionCompleted            DecisionAction = "completed"
)

type DecisionReason string

const (
	ReasonActiveGoal         DecisionReason = "active_goal"
	ReasonVerifierRevision   DecisionReason = "verifier_revision"
	ReasonVerifierSatisfied  DecisionReason = "verifier_satisfied"
	ReasonVerifierFailed     DecisionReason = "verifier_failed"
	ReasonTokenBudget        DecisionReason = "token_budget"
	ReasonMaxContinuations   DecisionReason = "max_continuations"
	ReasonGoalTerminal       DecisionReason = "goal_terminal"
	ReasonEnqueueUnavailable DecisionReason = "enqueue_unavailable"
	ReasonEnqueueFailed      DecisionReason = "enqueue_failed"
)

// DecisionRecord is one immutable explanation of why goal execution continued
// or stopped. Sequence is assigned atomically by the ledger.
type DecisionRecord struct {
	SchemaVersion     uint32         `json:"schema_version"`
	Sequence          uint64         `json:"sequence"`
	WorkspaceID       string         `json:"workspace_id"`
	SessionID         string         `json:"session_id"`
	GoalID            string         `json:"goal_id"`
	TurnMessageID     string         `json:"turn_message_id"`
	Action            DecisionAction `json:"action"`
	Reason            DecisionReason `json:"reason"`
	Detail            string         `json:"detail,omitempty"`
	CreatedAt         string         `json:"created_at"`
	TokenUsage        uint64         `json:"token_usage,omitempty"`
	TokenBudget       *uint64        `json:"token_budget,omitempty"`
	ContinuationsUsed uint32         `json:"continuations_used,omitempty"`
	Evidence          []string       `json:"evidence,omitempty"`
}

type ContinuationLedger interface {
	Append(ctx context.Context, workspaceID, sessionID string, record DecisionRecord) (DecisionRecord, error)
	// AppendFirstDecision atomically admits the first decision for one
	// goal/assistant-turn pair. It returns the existing decision and false when
	// another process already decided that turn. Follow-up audit records for an
	// admitted plan use Append.
	AppendFirstDecision(ctx context.Context, workspaceID, sessionID string, record DecisionRecord) (DecisionRecord, bool, error)
	List(ctx context.Context, workspaceID, sessionID string) ([]DecisionRecord, error)
}

type ledgerState struct {
	SchemaVersion uint32           `json:"schema_version"`
	WorkspaceID   string           `json:"workspace_id"`
	SessionID     string           `json:"session_id"`
	NextSequence  uint64           `json:"next_sequence"`
	Records       []DecisionRecord `json:"records"`
}

type FileLedger struct {
	stateDir string
	now      func() time.Time
	mu       sync.Mutex
}

func NewFileLedger(stateDir string, now func() time.Time) (*FileLedger, error) {
	if stateDir == "" {
		return nil, errors.New("goal: continuation ledger state directory is required")
	}
	if now == nil {
		now = time.Now
	}
	return &FileLedger{stateDir: filepath.Clean(stateDir), now: now}, nil
}

func (l *FileLedger) path(workspaceID, sessionID string) string {
	return filepath.Join(
		l.stateDir, "session-state", "workspaces", workspacepkg.PathSegment(workspaceID),
		"continuations", workspacepkg.PathSegment(sessionID)+".json",
	)
}

func newLedgerState(workspaceID, sessionID string) ledgerState {
	return ledgerState{
		SchemaVersion: continuationLedgerSchemaVersion,
		WorkspaceID:   workspaceID, SessionID: sessionID,
		NextSequence: 1, Records: []DecisionRecord{},
	}
}

func (l *FileLedger) loadLocked(workspaceID, sessionID string) (ledgerState, error) {
	state := newLedgerState(workspaceID, sessionID)
	data, err := os.ReadFile(l.path(workspaceID, sessionID))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return ledgerState{}, fmt.Errorf("goal: read continuation ledger: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return ledgerState{}, fmt.Errorf("%w: decode %s: %v", ErrCorruptLedger, l.path(workspaceID, sessionID), err)
	}
	if err := validateLedgerState(state, workspaceID, sessionID); err != nil {
		return ledgerState{}, err
	}
	return state, nil
}

func (l *FileLedger) Append(ctx context.Context, workspaceID, sessionID string, record DecisionRecord) (DecisionRecord, error) {
	if err := validateLedgerIdentity(ctx, workspaceID, sessionID); err != nil {
		return DecisionRecord{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	state, err := l.loadLocked(workspaceID, sessionID)
	if err != nil {
		return DecisionRecord{}, err
	}
	return l.appendLocked(state, record, workspaceID, sessionID)
}

func (l *FileLedger) AppendFirstDecision(ctx context.Context, workspaceID, sessionID string, record DecisionRecord) (DecisionRecord, bool, error) {
	if err := validateLedgerIdentity(ctx, workspaceID, sessionID); err != nil {
		return DecisionRecord{}, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	state, err := l.loadLocked(workspaceID, sessionID)
	if err != nil {
		return DecisionRecord{}, false, err
	}
	if existing, ok := firstDecisionForTurn(state.Records, record.GoalID, record.TurnMessageID); ok {
		return existing, false, nil
	}
	appended, err := l.appendLocked(state, record, workspaceID, sessionID)
	return appended, err == nil, err
}

func (l *FileLedger) appendLocked(state ledgerState, record DecisionRecord, workspaceID, sessionID string) (DecisionRecord, error) {
	record = prepareDecisionRecord(record, state.NextSequence, workspaceID, sessionID, l.now())
	if err := validateDecisionRecord(record); err != nil {
		return DecisionRecord{}, err
	}
	state.Records = append(state.Records, cloneDecisionRecord(record))
	state.NextSequence++
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return DecisionRecord{}, fmt.Errorf("goal: encode continuation ledger: %w", err)
	}
	if err := atomicfile.Write(l.path(workspaceID, sessionID), append(data, '\n'), 0o600); err != nil {
		return DecisionRecord{}, fmt.Errorf("goal: persist continuation ledger: %w", err)
	}
	return cloneDecisionRecord(record), nil
}

func (l *FileLedger) List(ctx context.Context, workspaceID, sessionID string) ([]DecisionRecord, error) {
	if err := validateLedgerIdentity(ctx, workspaceID, sessionID); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	state, err := l.loadLocked(workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	return cloneDecisionRecords(state.Records), nil
}

func validateLedgerIdentity(ctx context.Context, workspaceID, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if workspaceID == "" || sessionID == "" {
		return errors.New("goal: continuation ledger requires workspace and session")
	}
	return nil
}

func prepareDecisionRecord(record DecisionRecord, sequence uint64, workspaceID, sessionID string, now time.Time) DecisionRecord {
	record.SchemaVersion = continuationLedgerSchemaVersion
	record.Sequence = sequence
	record.WorkspaceID = workspaceID
	record.SessionID = sessionID
	if record.CreatedAt == "" {
		record.CreatedAt = timestamp(now)
	}
	if record.TokenBudget != nil {
		budget := *record.TokenBudget
		record.TokenBudget = &budget
	}
	record.Evidence = append([]string(nil), record.Evidence...)
	return record
}

func validateLedgerState(state ledgerState, workspaceID, sessionID string) error {
	if state.SchemaVersion != continuationLedgerSchemaVersion || state.WorkspaceID != workspaceID || state.SessionID != sessionID {
		return fmt.Errorf("%w: stored continuation identity or version does not match", ErrCorruptLedger)
	}
	wantSequence := uint64(1)
	for _, record := range state.Records {
		if record.Sequence != wantSequence || record.WorkspaceID != workspaceID || record.SessionID != sessionID {
			return fmt.Errorf("%w: continuation sequence or identity does not match", ErrCorruptLedger)
		}
		if err := validateDecisionRecord(record); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptLedger, err)
		}
		wantSequence++
	}
	if state.NextSequence != wantSequence {
		return fmt.Errorf("%w: next continuation sequence does not match", ErrCorruptLedger)
	}
	return nil
}

func validateDecisionRecord(record DecisionRecord) error {
	if record.SchemaVersion != continuationLedgerSchemaVersion || record.Sequence == 0 {
		return errors.New("goal: continuation decision requires schema version and sequence")
	}
	if record.WorkspaceID == "" || record.SessionID == "" || record.GoalID == "" || record.TurnMessageID == "" {
		return errors.New("goal: continuation decision requires workspace, session, goal, and turn message IDs")
	}
	if !slices.Contains([]DecisionAction{
		DecisionContinuationPlanned, DecisionContinuationEnqueued, DecisionStopped, DecisionCompleted,
	}, record.Action) {
		return fmt.Errorf("goal: invalid continuation action %q", record.Action)
	}
	if !slices.Contains([]DecisionReason{
		ReasonActiveGoal, ReasonVerifierRevision, ReasonVerifierSatisfied, ReasonVerifierFailed,
		ReasonTokenBudget, ReasonMaxContinuations, ReasonGoalTerminal,
		ReasonEnqueueUnavailable, ReasonEnqueueFailed,
	}, record.Reason) {
		return fmt.Errorf("goal: invalid continuation reason %q", record.Reason)
	}
	if _, err := time.Parse(time.RFC3339Nano, record.CreatedAt); err != nil {
		return errors.New("goal: continuation decision requires an RFC 3339 timestamp")
	}
	return nil
}

func cloneDecisionRecord(record DecisionRecord) DecisionRecord {
	if record.TokenBudget != nil {
		budget := *record.TokenBudget
		record.TokenBudget = &budget
	}
	record.Evidence = append([]string(nil), record.Evidence...)
	return record
}

func cloneDecisionRecords(records []DecisionRecord) []DecisionRecord {
	out := make([]DecisionRecord, len(records))
	for i, record := range records {
		out[i] = cloneDecisionRecord(record)
	}
	return out
}

func firstDecisionForTurn(records []DecisionRecord, goalID, turnMessageID string) (DecisionRecord, bool) {
	for _, record := range records {
		if record.GoalID == goalID && record.TurnMessageID == turnMessageID {
			return cloneDecisionRecord(record), true
		}
	}
	return DecisionRecord{}, false
}

var _ ContinuationLedger = (*FileLedger)(nil)
