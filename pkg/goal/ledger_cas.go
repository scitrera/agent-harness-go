package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	defaultCASLedgerPrefix  = "agent-harness/session-state/continuations/v1"
	defaultCASLedgerRetries = 32
)

var ErrLedgerCASRetryLimit = errors.New("goal: distributed continuation ledger retry limit exceeded")

type CASLedgerConfig struct {
	Blobs      casblob.Store
	Prefix     string
	MaxRetries int
	Now        func() time.Time
}

type CASLedger struct {
	blobs      casblob.Store
	prefix     string
	maxRetries int
	now        func() time.Time
}

func NewCASLedger(config CASLedgerConfig) (*CASLedger, error) {
	if config.Blobs == nil {
		return nil, errors.New("goal: CAS continuation ledger requires a blob store")
	}
	prefix := strings.Trim(strings.TrimSpace(config.Prefix), "/")
	if prefix == "" {
		prefix = defaultCASLedgerPrefix
	}
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultCASLedgerRetries
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &CASLedger{blobs: config.Blobs, prefix: prefix, maxRetries: maxRetries, now: now}, nil
}

func (l *CASLedger) key(workspaceID, sessionID string) string {
	return strings.Join([]string{
		l.prefix, "workspaces", workspacepkg.PathSegment(workspaceID),
		"sessions", workspacepkg.PathSegment(sessionID),
	}, "/")
}

func (l *CASLedger) read(ctx context.Context, workspaceID, sessionID string) ([]byte, ledgerState, bool, error) {
	if err := validateLedgerIdentity(ctx, workspaceID, sessionID); err != nil {
		return nil, ledgerState{}, false, err
	}
	raw, found, err := l.blobs.Read(ctx, l.key(workspaceID, sessionID))
	if err != nil {
		return nil, ledgerState{}, false, fmt.Errorf("goal: read distributed continuation ledger: %w", err)
	}
	if !found {
		return nil, newLedgerState(workspaceID, sessionID), false, nil
	}
	if len(raw) == 0 {
		return nil, ledgerState{}, false, fmt.Errorf("%w: distributed continuation value is empty", ErrCorruptLedger)
	}
	var state ledgerState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, ledgerState{}, false, fmt.Errorf("%w: decode distributed continuation state: %v", ErrCorruptLedger, err)
	}
	if err := validateLedgerState(state, workspaceID, sessionID); err != nil {
		return nil, ledgerState{}, false, err
	}
	return append([]byte(nil), raw...), state, true, nil
}

func (l *CASLedger) commit(ctx context.Context, workspaceID, sessionID string, previous []byte, existed bool, state ledgerState) (bool, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return false, fmt.Errorf("goal: encode distributed continuation ledger: %w", err)
	}
	if existed {
		swapped, err := l.blobs.CompareAndSwap(ctx, l.key(workspaceID, sessionID), previous, encoded)
		if err != nil {
			return false, fmt.Errorf("goal: compare-and-swap continuation ledger: %w", err)
		}
		return swapped, nil
	}
	created, err := l.blobs.Create(ctx, l.key(workspaceID, sessionID), encoded)
	if err != nil {
		return false, fmt.Errorf("goal: create distributed continuation ledger: %w", err)
	}
	return created, nil
}

func (l *CASLedger) Append(ctx context.Context, workspaceID, sessionID string, record DecisionRecord) (DecisionRecord, error) {
	appended, err := l.append(ctx, workspaceID, sessionID, record, false)
	return appended.record, err
}

func (l *CASLedger) AppendFirstDecision(ctx context.Context, workspaceID, sessionID string, record DecisionRecord) (DecisionRecord, bool, error) {
	appended, err := l.append(ctx, workspaceID, sessionID, record, true)
	if err != nil {
		return DecisionRecord{}, false, err
	}
	return appended.record, appended.admitted, nil
}

type casAppendResult struct {
	record   DecisionRecord
	admitted bool
}

func (l *CASLedger) append(ctx context.Context, workspaceID, sessionID string, record DecisionRecord, firstOnly bool) (casAppendResult, error) {
	createdAt := l.now()
	for attempt := 0; attempt < l.maxRetries; attempt++ {
		previous, state, found, err := l.read(ctx, workspaceID, sessionID)
		if err != nil {
			return casAppendResult{}, err
		}
		if firstOnly {
			if existing, ok := firstDecisionForTurn(state.Records, record.GoalID, record.TurnMessageID); ok {
				return casAppendResult{record: existing}, nil
			}
		}
		prepared := prepareDecisionRecord(record, state.NextSequence, workspaceID, sessionID, createdAt)
		if err := validateDecisionRecord(prepared); err != nil {
			return casAppendResult{}, err
		}
		state.Records = append(state.Records, cloneDecisionRecord(prepared))
		state.NextSequence++
		committed, err := l.commit(ctx, workspaceID, sessionID, previous, found, state)
		if err != nil {
			return casAppendResult{}, err
		}
		if committed {
			return casAppendResult{record: cloneDecisionRecord(prepared), admitted: true}, nil
		}
	}
	return casAppendResult{}, fmt.Errorf("%w: append %q after %d attempts", ErrLedgerCASRetryLimit, l.key(workspaceID, sessionID), l.maxRetries)
}

func (l *CASLedger) List(ctx context.Context, workspaceID, sessionID string) ([]DecisionRecord, error) {
	_, state, _, err := l.read(ctx, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	return cloneDecisionRecords(state.Records), nil
}

var _ ContinuationLedger = (*CASLedger)(nil)
