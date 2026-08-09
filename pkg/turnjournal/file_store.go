package turnjournal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// FileStore persists one complete record per parent task. It is safe for
// concurrent use within one process; a state directory has one writing process.
// Distributed hosts use the same Store contract over CAS storage.
type FileStore struct {
	stateDir string
	now      func() time.Time
	mu       sync.Mutex
}

func NewFileStore(stateDir string) (*FileStore, error) {
	if stateDir == "" {
		return nil, errors.New("turnjournal: state directory is required")
	}
	return &FileStore{stateDir: filepath.Clean(stateDir), now: time.Now}, nil
}

func (s *FileStore) path(workspaceID, taskID string) string {
	return filepath.Join(
		s.stateDir,
		"turn-executions",
		"workspaces",
		workspacepkg.PathSegment(workspaceID),
		"tasks",
		workspacepkg.PathSegment(taskID)+".json",
	)
}

func (s *FileStore) root() string {
	return filepath.Join(s.stateDir, "turn-executions", "workspaces")
}

func (s *FileStore) Create(ctx context.Context, record Record) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if record.Revision != 0 {
		return Record{}, fmt.Errorf("%w: create requires zero revision", ErrInvalidRecord)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.path(record.WorkspaceID, record.TaskID)
	if _, err := os.Stat(path); err == nil {
		return Record{}, ErrAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return Record{}, fmt.Errorf("turnjournal: inspect execution: %w", err)
	}
	now := s.now().UTC()
	record.Schema = Schema
	record.SchemaRevision = SchemaRevision
	record.Revision = 1
	record.CreatedAt = now
	record.UpdatedAt = now
	record.CompletedAt = nil
	record.FailureReason = ""
	if record.Phase == "" {
		record.Phase = PhasePrepared
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	if err := s.persistLocked(record); err != nil {
		return Record{}, err
	}
	return cloneRecord(record), nil
}

func (s *FileStore) Get(ctx context.Context, workspaceID, taskID string) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if err := validateLookup(workspaceID, taskID); err != nil {
		return Record{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.loadLocked(workspaceID, taskID)
	return cloneRecord(record), err
}

func (s *FileStore) Update(ctx context.Context, record Record, expectedRevision uint64) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if expectedRevision == 0 || record.Revision != expectedRevision {
		return Record{}, ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.loadLocked(record.WorkspaceID, record.TaskID)
	if err != nil {
		return Record{}, err
	}
	if current.Revision != expectedRevision {
		return Record{}, ErrConflict
	}
	next := cloneRecord(record)
	next.Schema = Schema
	next.SchemaRevision = SchemaRevision
	next.Revision = expectedRevision + 1
	next.CreatedAt = current.CreatedAt
	next.UpdatedAt = s.now().UTC()
	if next.UpdatedAt.Before(current.UpdatedAt) {
		next.UpdatedAt = current.UpdatedAt
	}
	if next.Terminal() {
		completed := next.UpdatedAt
		next.CompletedAt = &completed
	} else {
		next.CompletedAt = nil
		next.FailureReason = ""
	}
	if err := validateTransition(current, next); err != nil {
		return Record{}, err
	}
	if err := next.Validate(); err != nil {
		return Record{}, err
	}
	if err := s.persistLocked(next); err != nil {
		return Record{}, err
	}
	return cloneRecord(next), nil
}

func (s *FileStore) ListActive(ctx context.Context, ownerIdentity string) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateIdentifier("owner_identity", ownerIdentity); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var records []Record
	err := filepath.WalkDir(s.root(), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" || filepath.Base(filepath.Dir(path)) != "tasks" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		record, err := decodeRecord(data)
		if err != nil {
			return fmt.Errorf("%w: decode %s: %v", ErrCorruptStore, path, err)
		}
		if filepath.Clean(path) != s.path(record.WorkspaceID, record.TaskID) {
			return fmt.Errorf("%w: stored execution path does not match identity", ErrCorruptStore)
		}
		if record.OwnerIdentity == ownerIdentity && !record.Terminal() {
			records = append(records, cloneRecord(record))
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return []Record{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("turnjournal: scan executions: %w", err)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].WorkspaceID != records[j].WorkspaceID {
			return records[i].WorkspaceID < records[j].WorkspaceID
		}
		if records[i].SessionID != records[j].SessionID {
			return records[i].SessionID < records[j].SessionID
		}
		return records[i].TaskID < records[j].TaskID
	})
	return records, nil
}

func (s *FileStore) loadLocked(workspaceID, taskID string) (Record, error) {
	data, err := os.ReadFile(s.path(workspaceID, taskID))
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, fmt.Errorf("turnjournal: read execution: %w", err)
	}
	record, err := decodeRecord(data)
	if err != nil {
		return Record{}, fmt.Errorf("%w: decode %s: %v", ErrCorruptStore, s.path(workspaceID, taskID), err)
	}
	if record.WorkspaceID != workspaceID || record.TaskID != taskID {
		return Record{}, fmt.Errorf("%w: stored execution identity does not match", ErrCorruptStore)
	}
	return record, nil
}

func decodeRecord(data []byte) (Record, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record Record
	if err := decoder.Decode(&record); err != nil {
		return Record{}, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Record{}, err
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	return record, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func (s *FileStore) persistLocked(record Record) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("turnjournal: encode execution: %w", err)
	}
	data = append(data, '\n')
	if err := atomicfile.Write(s.path(record.WorkspaceID, record.TaskID), data, 0o600); err != nil {
		return fmt.Errorf("turnjournal: persist execution: %w", err)
	}
	return nil
}

func validateLookup(workspaceID, taskID string) error {
	if err := validateIdentifier("workspace_id", workspaceID); err != nil {
		return err
	}
	return validateIdentifier("task_id", taskID)
}

func cloneRecord(record Record) Record {
	if record.CompletedAt != nil {
		completed := *record.CompletedAt
		record.CompletedAt = &completed
	}
	if record.Tool != nil {
		tool := *record.Tool
		if tool.External != nil {
			external := *tool.External
			external.Descriptor = append(json.RawMessage(nil), external.Descriptor...)
			tool.External = &external
		}
		if tool.Result != nil {
			result := *tool.Result
			tool.Result = &result
		}
		record.Tool = &tool
	}
	return record
}

var _ Store = (*FileStore)(nil)
