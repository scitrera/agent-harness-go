package executionledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	defaultCASPrefix  = "agent-harness/execution-ledger/v1"
	defaultCASRetries = 32
)

var ErrCASRetryLimit = errors.New("executionledger: distributed update retry limit exceeded")

type CASStoreConfig struct {
	Blobs      casblob.Store
	Prefix     string
	MaxEvents  int
	MaxRetries int
	Now        func() time.Time
	NewID      func() (string, error)
}

type CASStore struct {
	blobs      casblob.Store
	prefix     string
	maxEvents  int
	maxRetries int
	now        func() time.Time
	newID      func() (string, error)
}

func NewCASStore(config CASStoreConfig) (*CASStore, error) {
	if config.Blobs == nil {
		return nil, errors.New("executionledger: CAS store requires a blob store")
	}
	config.Prefix = strings.Trim(strings.TrimSpace(config.Prefix), "/")
	if config.Prefix == "" {
		config.Prefix = defaultCASPrefix
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = DefaultMaxEvents
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = defaultCASRetries
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewID == nil {
		config.NewID = func() (string, error) { return ids.New("exec-") }
	}
	return &CASStore{blobs: config.Blobs, prefix: config.Prefix, maxEvents: config.MaxEvents, maxRetries: config.MaxRetries, now: config.Now, newID: config.NewID}, nil
}

func (s *CASStore) key(ref Ref) string {
	return strings.Join([]string{s.prefix, "workspaces", workspacepkg.PathSegment(ref.WorkspaceID), "sessions", workspacepkg.PathSegment(ref.SessionID)}, "/")
}

func (s *CASStore) read(ctx context.Context, ref Ref) ([]byte, state, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, state{}, false, err
	}
	if err := ref.validate(); err != nil {
		return nil, state{}, false, err
	}
	raw, found, err := s.blobs.Read(ctx, s.key(ref))
	if err != nil {
		return nil, state{}, false, fmt.Errorf("executionledger: read distributed state: %w", err)
	}
	if !found {
		return nil, newState(ref), false, nil
	}
	if len(raw) == 0 {
		return nil, state{}, false, fmt.Errorf("%w: distributed state is empty", ErrCorrupt)
	}
	var current state
	if err := json.Unmarshal(raw, &current); err != nil {
		return nil, state{}, false, fmt.Errorf("%w: decode distributed state: %v", ErrCorrupt, err)
	}
	if err := validateState(current, ref, s.maxEvents); err != nil {
		return nil, state{}, false, err
	}
	return append([]byte(nil), raw...), current, true, nil
}

func (s *CASStore) commit(ctx context.Context, ref Ref, previous []byte, found bool, next state) (bool, error) {
	encoded, err := json.Marshal(next)
	if err != nil {
		return false, fmt.Errorf("executionledger: encode distributed state: %w", err)
	}
	if found {
		swapped, err := s.blobs.CompareAndSwap(ctx, s.key(ref), previous, encoded)
		if err != nil {
			return false, fmt.Errorf("executionledger: compare-and-swap distributed state: %w", err)
		}
		return swapped, nil
	}
	created, err := s.blobs.Create(ctx, s.key(ref), encoded)
	if err != nil {
		return false, fmt.Errorf("executionledger: create distributed state: %w", err)
	}
	return created, nil
}

func (s *CASStore) Append(ctx context.Context, ref Ref, operationID string, request AppendRequest) (AppendResult, error) {
	eventID, err := s.newID()
	if err != nil {
		return AppendResult{}, fmt.Errorf("executionledger: create event id: %w", err)
	}
	createdAt := s.now()
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		previous, current, found, err := s.read(ctx, ref)
		if err != nil {
			return AppendResult{}, err
		}
		next, result, err := prepareAppend(current, ref, operationID, request, eventID, createdAt, s.maxEvents)
		if err != nil {
			return AppendResult{}, err
		}
		if result.Replayed {
			return result, nil
		}
		committed, err := s.commit(ctx, ref, previous, found, next)
		if err != nil {
			return AppendResult{}, err
		}
		if committed {
			return result, nil
		}
	}
	return AppendResult{}, fmt.Errorf("%w: append %q after %d attempts", ErrCASRetryLimit, s.key(ref), s.maxRetries)
}

func (s *CASStore) Query(ctx context.Context, ref Ref, query Query) (Page, error) {
	_, current, _, err := s.read(ctx, ref)
	if err != nil {
		return Page{}, err
	}
	return queryState(current, ref, query)
}

func (s *CASStore) PinnedModel(ctx context.Context, ref Ref) (string, error) {
	_, current, _, err := s.read(ctx, ref)
	if err != nil {
		return "", err
	}
	return current.PinnedModel, nil
}

var _ Store = (*CASStore)(nil)
