package refinement

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/ids"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const maxAuditLineBytes = 1 << 20

type FileStore struct {
	stateDir string
	now      func() time.Time
	mu       sync.Mutex
}

type fileEntry struct {
	OperationID string `json:"operation_id"`
	RequestHash string `json:"request_hash"`
	Record      Record `json:"record"`
}

func NewFileStore(stateDir string, now func() time.Time) (*FileStore, error) {
	if strings.TrimSpace(stateDir) == "" {
		return nil, fmt.Errorf("%w: state directory is required", ErrInvalid)
	}
	if now == nil {
		now = time.Now
	}
	return &FileStore{stateDir: stateDir, now: now}, nil
}

func (s *FileStore) Path(workspaceID string) string {
	return filepath.Join(workspacepkg.StateDir(s.stateDir, workspaceID), "refinement-records.jsonl")
}

func (s *FileStore) Append(ctx context.Context, workspaceID, operationID string, request AppendRequest) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	if strings.TrimSpace(operationID) == "" || len(operationID) > 200 {
		return AppendResult{}, fmt.Errorf("%w: operation id is required and must not exceed 200 bytes", ErrInvalid)
	}
	if request.SchemaVersion == 0 {
		request.SchemaVersion = 1
	}
	if err := request.Validate(); err != nil {
		return AppendResult{}, err
	}
	// Normalize interface-valued JSON documents before hashing so a typed
	// metadata value and its decoded map representation have one stable hash.
	request = cloneRequest(request)
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return AppendResult{}, fmt.Errorf("refinement: encode request: %w", err)
	}
	requestHash := hashBytes(requestBytes)

	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(workspaceID)
	if err != nil {
		return AppendResult{}, err
	}
	for _, entry := range entries {
		if entry.OperationID == operationID {
			if entry.RequestHash != requestHash {
				return AppendResult{}, fmt.Errorf("%w: operation id %q was used for another request", ErrConflict, operationID)
			}
			return AppendResult{Record: entry.Record, Replayed: true}, nil
		}
		if entry.Record.Key == request.Key {
			return AppendResult{}, fmt.Errorf("%w: record key %q already exists", ErrConflict, request.Key)
		}
	}
	id, err := ids.New("ref-")
	if err != nil {
		return AppendResult{}, fmt.Errorf("refinement: create id: %w", err)
	}
	record := Record{
		ID: id, WorkspaceID: workspaceID, AppendRequest: cloneRequest(request), Revision: 1,
		ETag:      `"ref-` + hashBytes(append([]byte(id), requestBytes...)) + `"`,
		CreatedAt: s.now().UTC().Format(time.RFC3339Nano),
	}
	entry := fileEntry{OperationID: operationID, RequestHash: requestHash, Record: record}
	if err := s.appendEntry(workspaceID, entry); err != nil {
		return AppendResult{}, err
	}
	return AppendResult{Record: record}, nil
}

func (s *FileStore) Get(ctx context.Context, workspaceID, recordID string) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(workspaceID)
	if err != nil {
		return Record{}, err
	}
	for _, entry := range entries {
		if entry.Record.ID == recordID {
			return cloneRecord(entry.Record), nil
		}
	}
	return Record{}, fmt.Errorf("%w: %s", ErrNotFound, recordID)
}

func (s *FileStore) List(ctx context.Context, workspaceID string) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(workspaceID)
	if err != nil {
		return nil, err
	}
	records := make([]Record, len(entries))
	for i := range entries {
		records[i] = cloneRecord(entries[len(entries)-1-i].Record)
	}
	return records, nil
}

func (s *FileStore) load(workspaceID string) ([]fileEntry, error) {
	path := s.Path(workspaceID)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("refinement: open %s: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), maxAuditLineBytes)
	var entries []fileEntry
	seenOperations := map[string]struct{}{}
	seenKeys := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	line := 0
	for scanner.Scan() {
		line++
		var entry fileEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("refinement: decode %s line %d: %w", path, line, err)
		}
		if entry.OperationID == "" || entry.RequestHash == "" || entry.Record.WorkspaceID != workspaceID || entry.Record.ID == "" || entry.Record.Revision != 1 {
			return nil, fmt.Errorf("refinement: invalid %s line %d", path, line)
		}
		if err := entry.Record.AppendRequest.Validate(); err != nil {
			return nil, fmt.Errorf("refinement: validate %s line %d: %w", path, line, err)
		}
		requestBytes, err := json.Marshal(entry.Record.AppendRequest)
		if err != nil || entry.RequestHash != hashBytes(requestBytes) {
			return nil, fmt.Errorf("refinement: request hash mismatch in %s line %d", path, line)
		}
		expectedETag := `"ref-` + hashBytes(append([]byte(entry.Record.ID), requestBytes...)) + `"`
		if entry.Record.ETag != expectedETag {
			return nil, fmt.Errorf("refinement: ETag mismatch in %s line %d", path, line)
		}
		if _, duplicate := seenOperations[entry.OperationID]; duplicate {
			return nil, fmt.Errorf("refinement: duplicate operation in %s line %d", path, line)
		}
		if _, duplicate := seenKeys[entry.Record.Key]; duplicate {
			return nil, fmt.Errorf("refinement: duplicate key in %s line %d", path, line)
		}
		if _, duplicate := seenIDs[entry.Record.ID]; duplicate {
			return nil, fmt.Errorf("refinement: duplicate id in %s line %d", path, line)
		}
		seenOperations[entry.OperationID] = struct{}{}
		seenKeys[entry.Record.Key] = struct{}{}
		seenIDs[entry.Record.ID] = struct{}{}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("refinement: read %s: %w", path, err)
	}
	return entries, nil
}

func (s *FileStore) appendEntry(workspaceID string, entry fileEntry) error {
	path := s.Path(workspaceID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("refinement: create audit directory: %w", err)
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("refinement: encode audit entry: %w", err)
	}
	if len(encoded) > maxAuditLineBytes {
		return fmt.Errorf("%w: encoded audit entry exceeds %d bytes", ErrInvalid, maxAuditLineBytes)
	}
	encoded = append(encoded, '\n')
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("refinement: open audit log: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return fmt.Errorf("refinement: append audit log: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("refinement: sync audit log: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("refinement: close audit log: %w", err)
	}
	return nil
}

func cloneRequest(request AppendRequest) AppendRequest {
	request.Plan.Evidence = append([]Evidence(nil), request.Plan.Evidence...)
	edits := request.Plan.Edits
	request.Plan.Edits = make([]Edit, len(edits))
	for i := range edits {
		request.Plan.Edits[i] = cloneEdit(edits[i])
	}
	if request.Metadata != nil {
		request.Metadata = cloneMap(request.Metadata)
	}
	if request.TaskRef != nil {
		ref := *request.TaskRef
		request.TaskRef = &ref
	}
	if request.ApprovalRef != nil {
		ref := *request.ApprovalRef
		request.ApprovalRef = &ref
	}
	return request
}

func cloneEdit(edit Edit) Edit {
	edit.Content = cloneMap(edit.Content)
	if edit.Before != nil {
		before := *edit.Before
		before.Content = cloneMap(before.Content)
		before.Metadata = cloneMap(before.Metadata)
		edit.Before = &before
	}
	if edit.After != nil {
		after := *edit.After
		after.Content = cloneMap(after.Content)
		after.Metadata = cloneMap(after.Metadata)
		edit.After = &after
	}
	return edit
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		// Validation and persistence will reject non-JSON values. Returning a
		// shallow clone here keeps clone helpers total for callers inspecting an
		// invalid request before Append.
		output := make(map[string]any, len(input))
		for key, value := range input {
			output[key] = value
		}
		return output
	}
	var output map[string]any
	if err := json.Unmarshal(encoded, &output); err != nil {
		return nil
	}
	return output
}

func cloneRecord(record Record) Record {
	record.AppendRequest = cloneRequest(record.AppendRequest)
	return record
}

func hashBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

var _ Store = (*FileStore)(nil)
