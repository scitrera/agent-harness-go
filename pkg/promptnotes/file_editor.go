package promptnotes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	"github.com/scitrera/agent-harness-go/pkg/refinement"
)

const (
	// The local editor keeps complete accepted results in the same atomic file as
	// the resource heads. At the high-water mark it discards the oldest receipts
	// down to the low-water mark before accepting the next mutation. Exact result
	// replay is therefore bounded; resource CAS and retained key tombstones remain
	// the safety floor after an old receipt expires.
	maxLocalRefinementOperations      = 512
	retainedLocalRefinementOperations = 384
	localOperationJournalPolicy       = 1
)

type fileOperation struct {
	OperationID string              `json:"operation_id"`
	RequestHash string              `json:"request_hash"`
	Mutation    refinement.Mutation `json:"mutation"`
}

type fileOperationJournal struct {
	PolicyVersion       int    `json:"policy_version"`
	Generation          uint64 `json:"generation"`
	CompactedOperations uint64 `json:"compacted_operations"`
}

// FileEditor adds idempotent CAS mutations to the same workspace-isolated
// prompt-notes.json authority read by FileProvider. Atomic replacement means a
// reader observes the old or new complete authority document, never a partial
// mutation. One process must own writes to a state directory.
type FileEditor struct {
	provider *FileProvider
	mu       sync.Mutex
}

func NewFileEditor(stateDir string) (*FileEditor, error) {
	provider, err := NewFileProvider(stateDir)
	if err != nil {
		return nil, err
	}
	return &FileEditor{provider: provider}, nil
}

func (e *FileEditor) Apply(ctx context.Context, workspaceID, operationID string, edit refinement.Edit) (refinement.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return refinement.Mutation{}, err
	}
	if edit.ResourceKind != refinement.ResourcePromptNote {
		return refinement.Mutation{}, fmt.Errorf("%w: %s", refinement.ErrUnsupportedResource, edit.ResourceKind)
	}
	if strings.TrimSpace(operationID) == "" || len(operationID) > 200 {
		return refinement.Mutation{}, fmt.Errorf("%w: operation id is required and must not exceed 200 bytes", refinement.ErrInvalid)
	}
	requestBytes, err := json.Marshal(edit)
	if err != nil {
		return refinement.Mutation{}, fmt.Errorf("%w: encode prompt-note edit: %v", refinement.ErrInvalid, err)
	}
	requestHash := localHash(requestBytes)

	e.mu.Lock()
	defer e.mu.Unlock()
	document, _, err := e.provider.loadDocument(ctx, workspaceID)
	if err != nil {
		return refinement.Mutation{}, err
	}
	seenIDs := make(map[string]struct{}, len(document.Notes))
	seenKeys := make(map[string]struct{}, len(document.Notes))
	for i := range document.Notes {
		document.Notes[i] = normalizeFileNote(workspaceID, document.Notes[i])
		note := document.Notes[i]
		if note.ID == "" || strings.TrimSpace(note.Key) == "" {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note authority contains an empty id or key", refinement.ErrInvalid)
		}
		if _, duplicate := seenIDs[note.ID]; duplicate {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note authority contains duplicate id %q", refinement.ErrInvalid, note.ID)
		}
		if _, duplicate := seenKeys[note.Key]; duplicate {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note authority contains duplicate key %q", refinement.ErrInvalid, note.Key)
		}
		seenIDs[note.ID] = struct{}{}
		seenKeys[note.Key] = struct{}{}
	}
	for _, operation := range document.Operations {
		if operation.OperationID == operationID {
			if operation.RequestHash != requestHash {
				return refinement.Mutation{}, fmt.Errorf("%w: operation id %q was used for a different prompt-note edit", refinement.ErrConflict, operationID)
			}
			mutation := cloneMutation(operation.Mutation)
			mutation.Replayed = true
			return mutation, nil
		}
	}

	mutation, err := applyFileEdit(workspaceID, &document, edit)
	if err != nil {
		return refinement.Mutation{}, err
	}
	compactFileOperationJournal(&document)
	document.Operations = append(document.Operations, fileOperation{OperationID: operationID, RequestHash: requestHash, Mutation: cloneMutation(mutation)})
	if err := e.persist(document); err != nil {
		return refinement.Mutation{}, err
	}
	return cloneMutation(mutation), nil
}

func validateFileOperationJournal(document fileDocument) error {
	if len(document.Operations) > maxLocalRefinementOperations {
		return fmt.Errorf("%w: prompt-note operation journal retains %d results, maximum is %d", refinement.ErrInvalid, len(document.Operations), maxLocalRefinementOperations)
	}
	if document.OperationJournal != nil {
		compactionWidth := uint64(maxLocalRefinementOperations - retainedLocalRefinementOperations)
		if document.OperationJournal.PolicyVersion != localOperationJournalPolicy ||
			document.OperationJournal.Generation == 0 ||
			document.OperationJournal.CompactedOperations == 0 ||
			document.OperationJournal.CompactedOperations%compactionWidth != 0 ||
			document.OperationJournal.CompactedOperations/compactionWidth != document.OperationJournal.Generation ||
			len(document.Operations) < retainedLocalRefinementOperations+1 {
			return fmt.Errorf("%w: prompt-note authority contains invalid operation-journal compaction metadata", refinement.ErrInvalid)
		}
	}

	seenOperations := make(map[string]struct{}, len(document.Operations))
	for _, operation := range document.Operations {
		if strings.TrimSpace(operation.OperationID) == "" || len(operation.OperationID) > 200 || !validLocalHash(operation.RequestHash) || operation.Mutation.After == nil {
			return fmt.Errorf("%w: prompt-note authority contains an invalid refinement operation", refinement.ErrInvalid)
		}
		if operation.Mutation.After.ResourceID == "" || strings.TrimSpace(operation.Mutation.After.ResourceKey) == "" || operation.Mutation.After.ETag == "" {
			return fmt.Errorf("%w: prompt-note authority contains an incomplete refinement result", refinement.ErrInvalid)
		}
		if _, duplicate := seenOperations[operation.OperationID]; duplicate {
			return fmt.Errorf("%w: prompt-note authority contains duplicate operation %q", refinement.ErrInvalid, operation.OperationID)
		}
		seenOperations[operation.OperationID] = struct{}{}
	}
	return nil
}

func compactFileOperationJournal(document *fileDocument) {
	if len(document.Operations) < maxLocalRefinementOperations {
		return
	}
	drop := len(document.Operations) - retainedLocalRefinementOperations
	document.Operations = append([]fileOperation(nil), document.Operations[drop:]...)
	if document.OperationJournal == nil {
		document.OperationJournal = &fileOperationJournal{PolicyVersion: localOperationJournalPolicy}
	}
	document.OperationJournal.Generation++
	document.OperationJournal.CompactedOperations += uint64(drop)
}

func validLocalHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func applyFileEdit(workspaceID string, document *fileDocument, edit refinement.Edit) (refinement.Mutation, error) {
	switch edit.Action {
	case refinement.ActionCreate:
		if strings.TrimSpace(edit.ResourceKey) == "" || edit.ResourceID != "" || edit.ExpectedETag != "" {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note create requires a key and no resource id or expected ETag", refinement.ErrInvalid)
		}
		content, err := decodeFileNoteContent(edit.Content)
		if err != nil {
			return refinement.Mutation{}, err
		}
		for _, note := range document.Notes {
			if note.Key == edit.ResourceKey {
				return refinement.Mutation{}, fmt.Errorf("%w: prompt-note key %q already exists", refinement.ErrConflict, edit.ResourceKey)
			}
		}
		note := normalizeFileNote(workspaceID, fileNote{
			ID: localNoteID(workspaceID, edit.ResourceKey), Key: edit.ResourceKey, Title: content.Title, Content: content.Content,
			Enabled: content.Enabled, SchemaVersion: content.SchemaVersion, Metadata: content.Metadata, Revision: 1,
		})
		document.Notes = append(document.Notes, note)
		return refinement.Mutation{After: fileNoteSnapshot(note)}, nil
	case refinement.ActionReplace, refinement.ActionDelete:
		if edit.ResourceID == "" || edit.ExpectedETag == "" {
			return refinement.Mutation{}, fmt.Errorf("%w: prompt-note %s requires resource_id and expected_etag", refinement.ErrInvalid, edit.Action)
		}
		for i := range document.Notes {
			note := document.Notes[i]
			if note.ID != edit.ResourceID {
				continue
			}
			if note.Key != edit.ResourceKey {
				return refinement.Mutation{}, fmt.Errorf("%w: prompt-note id %s has key %q, not %q", refinement.ErrConflict, edit.ResourceID, note.Key, edit.ResourceKey)
			}
			if note.Deleted {
				return refinement.Mutation{}, fmt.Errorf("%w: prompt note %s is deleted", refinement.ErrNotFound, edit.ResourceID)
			}
			if note.ETag != edit.ExpectedETag {
				return refinement.Mutation{}, fmt.Errorf("%w: prompt-note ETag does not match current revision", refinement.ErrConflict)
			}
			before := fileNoteSnapshot(note)
			if edit.Action == refinement.ActionReplace {
				content, err := decodeFileNoteContent(edit.Content)
				if err != nil {
					return refinement.Mutation{}, err
				}
				note.Title, note.Content, note.Enabled = content.Title, content.Content, content.Enabled
				note.SchemaVersion, note.Metadata = content.SchemaVersion, content.Metadata
			} else {
				note.Deleted = true
			}
			note.Revision++
			note.ETag = fileNoteETag(note)
			document.Notes[i] = note
			return refinement.Mutation{Before: before, After: fileNoteSnapshot(note)}, nil
		}
		return refinement.Mutation{}, fmt.Errorf("%w: prompt note %s", refinement.ErrNotFound, edit.ResourceID)
	case refinement.ActionRestore:
		return refinement.Mutation{}, fmt.Errorf("%w: local prompt-note tombstone restoration is not supported", refinement.ErrUnsupportedResource)
	default:
		return refinement.Mutation{}, fmt.Errorf("%w: unsupported prompt-note action %q", refinement.ErrInvalid, edit.Action)
	}
}

type fileNoteContent struct {
	Title         string         `json:"title"`
	Content       string         `json:"content"`
	Enabled       *bool          `json:"enabled,omitempty"`
	SchemaVersion int            `json:"schema_version,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

func decodeFileNoteContent(content map[string]any) (fileNoteContent, error) {
	encoded, err := json.Marshal(content)
	if err != nil {
		return fileNoteContent{}, fmt.Errorf("%w: encode prompt-note content: %v", refinement.ErrInvalid, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var output fileNoteContent
	if err := decoder.Decode(&output); err != nil {
		return fileNoteContent{}, fmt.Errorf("%w: decode prompt-note content: %v", refinement.ErrInvalid, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fileNoteContent{}, fmt.Errorf("%w: prompt-note content contains trailing JSON", refinement.ErrInvalid)
	}
	if strings.TrimSpace(output.Content) == "" || len(output.Content) > MaxNoteContentBytes {
		return fileNoteContent{}, fmt.Errorf("%w: prompt-note content is blank or exceeds %d bytes", refinement.ErrInvalid, MaxNoteContentBytes)
	}
	if output.SchemaVersion == 0 {
		output.SchemaVersion = CurrentSchemaVersion
	}
	if output.SchemaVersion != CurrentSchemaVersion {
		return fileNoteContent{}, fmt.Errorf("%w: unsupported prompt-note schema version %d", refinement.ErrInvalid, output.SchemaVersion)
	}
	if output.Enabled == nil {
		enabled := true
		output.Enabled = &enabled
	}
	output.Metadata = cloneJSONMap(output.Metadata)
	return output, nil
}

func normalizeFileNote(workspaceID string, note fileNote) fileNote {
	if note.ID == "" {
		note.ID = localNoteID(workspaceID, note.Key)
	}
	if note.Enabled == nil {
		enabled := true
		note.Enabled = &enabled
	}
	if note.SchemaVersion == 0 {
		note.SchemaVersion = CurrentSchemaVersion
	}
	if note.Revision == 0 {
		note.Revision = 1
	}
	note.Metadata = cloneJSONMap(note.Metadata)
	if note.ETag == "" {
		note.ETag = fileNoteETag(note)
	}
	return note
}

func localNoteID(workspaceID, key string) string {
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + key))
	return "local-note-" + hex.EncodeToString(sum[:12])
}

func fileNoteETag(note fileNote) string {
	state := struct {
		ID            string         `json:"id"`
		Key           string         `json:"key"`
		Title         string         `json:"title"`
		Content       string         `json:"content"`
		Enabled       bool           `json:"enabled"`
		SchemaVersion int            `json:"schema_version"`
		Metadata      map[string]any `json:"metadata"`
		Revision      int            `json:"revision"`
		Deleted       bool           `json:"deleted"`
	}{
		ID: note.ID, Key: note.Key, Title: note.Title, Content: note.Content, Enabled: note.Enabled != nil && *note.Enabled,
		SchemaVersion: note.SchemaVersion, Metadata: note.Metadata, Revision: note.Revision, Deleted: note.Deleted,
	}
	encoded, _ := json.Marshal(state)
	return `"local-note-` + localHash(encoded) + `"`
}

func fileNoteSnapshot(note fileNote) *refinement.ResourceSnapshot {
	enabled := note.Enabled != nil && *note.Enabled
	return &refinement.ResourceSnapshot{
		ResourceID: note.ID, ResourceKey: note.Key, ETag: note.ETag, SchemaVersion: note.SchemaVersion,
		Content: map[string]any{
			"title": note.Title, "content": note.Content, "enabled": enabled,
			"schema_version": note.SchemaVersion, "metadata": cloneJSONMap(note.Metadata),
		},
		Metadata: cloneJSONMap(note.Metadata), Deleted: note.Deleted,
	}
}

func (e *FileEditor) persist(document fileDocument) error {
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("promptnotes: encode authority: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxFileBytes {
		return fmt.Errorf("%w: prompt-note authority exceeds %d bytes", refinement.ErrInvalid, maxFileBytes)
	}
	if err := atomicfile.Write(e.provider.Path(document.WorkspaceID), encoded, 0o600); err != nil {
		return fmt.Errorf("promptnotes: persist authority: %w", err)
	}
	return nil
}

func cloneMutation(mutation refinement.Mutation) refinement.Mutation {
	encoded, _ := json.Marshal(mutation)
	var clone refinement.Mutation
	_ = json.Unmarshal(encoded, &clone)
	return clone
}

func cloneJSONMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	encoded, _ := json.Marshal(input)
	var clone map[string]any
	_ = json.Unmarshal(encoded, &clone)
	return clone
}

func localHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

var _ refinement.ResourceEditor = (*FileEditor)(nil)
