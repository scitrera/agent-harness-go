package executionledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type FileStoreConfig struct {
	StateDir  string
	MaxEvents int
	Now       func() time.Time
	NewID     func() (string, error)
}

type FileStore struct {
	stateDir  string
	maxEvents int
	now       func() time.Time
	newID     func() (string, error)
	mu        sync.Mutex
}

func NewFileStore(config FileStoreConfig) (*FileStore, error) {
	if strings.TrimSpace(config.StateDir) == "" {
		return nil, fmt.Errorf("%w: state directory is required", ErrInvalid)
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = DefaultMaxEvents
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewID == nil {
		config.NewID = func() (string, error) { return ids.New("exec-") }
	}
	return &FileStore{stateDir: filepath.Clean(config.StateDir), maxEvents: config.MaxEvents, now: config.Now, newID: config.NewID}, nil
}

func (s *FileStore) path(ref Ref) string {
	return filepath.Join(s.stateDir, "execution-ledger", "workspaces", workspacepkg.PathSegment(ref.WorkspaceID), "sessions", workspacepkg.PathSegment(ref.SessionID)+".json")
}

func (s *FileStore) read(ref Ref) (state, error) {
	if err := ref.validate(); err != nil {
		return state{}, err
	}
	data, err := os.ReadFile(s.path(ref))
	if errors.Is(err, os.ErrNotExist) {
		return newState(ref), nil
	}
	if err != nil {
		return state{}, fmt.Errorf("executionledger: read file state: %w", err)
	}
	var current state
	if err := json.Unmarshal(data, &current); err != nil {
		return state{}, fmt.Errorf("%w: decode %s: %v", ErrCorrupt, s.path(ref), err)
	}
	if err := validateState(current, ref, s.maxEvents); err != nil {
		return state{}, err
	}
	return current, nil
}

func (s *FileStore) write(ref Ref, current state) error {
	encoded, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("executionledger: encode file state: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := atomicfile.Write(s.path(ref), encoded, 0o600); err != nil {
		return fmt.Errorf("executionledger: persist file state: %w", err)
	}
	return nil
}

func (s *FileStore) Append(ctx context.Context, ref Ref, operationID string, request AppendRequest) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	eventID, err := s.newID()
	if err != nil {
		return AppendResult{}, fmt.Errorf("executionledger: create event id: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.read(ref)
	if err != nil {
		return AppendResult{}, err
	}
	next, result, err := prepareAppend(current, ref, operationID, request, eventID, s.now(), s.maxEvents)
	if err != nil {
		return AppendResult{}, err
	}
	if result.Replayed {
		return result, nil
	}
	if err := s.write(ref, next); err != nil {
		return AppendResult{}, err
	}
	return result, nil
}

func (s *FileStore) Query(ctx context.Context, ref Ref, query Query) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	if err := ref.validate(); err != nil {
		return Page{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.read(ref)
	if err != nil {
		return Page{}, err
	}
	return queryState(current, ref, query)
}

func (s *FileStore) PinnedModel(ctx context.Context, ref Ref) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := ref.validate(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.read(ref)
	if err != nil {
		return "", err
	}
	return current.PinnedModel, nil
}

var _ Store = (*FileStore)(nil)
