// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package promptnotes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const maxFileBytes = 16 << 20

// FileProvider reads the standalone prompt-note authority from the harness
// state directory. Each logical workspace gets its own prompt-notes.json file.
type FileProvider struct {
	stateDir string
}

// NewFileProvider constructs a workspace-isolated local provider.
func NewFileProvider(stateDir string) (*FileProvider, error) {
	if stateDir == "" {
		return nil, errors.New("promptnotes: state directory required")
	}
	return &FileProvider{stateDir: stateDir}, nil
}

// Path returns the authoritative JSON file for workspaceID. It is exposed so
// local operators and UIs can tell users exactly which file to edit.
func (p *FileProvider) Path(workspaceID string) string {
	return filepath.Join(workspacepkg.StateDir(p.stateDir, workspaceID), "prompt-notes.json")
}

type fileDocument struct {
	SchemaVersion    int                   `json:"schema_version"`
	WorkspaceID      string                `json:"workspace_id,omitempty"`
	Notes            []fileNote            `json:"notes"`
	Operations       []fileOperation       `json:"refinement_operations,omitempty"`
	OperationJournal *fileOperationJournal `json:"refinement_operation_journal,omitempty"`
}

type fileNote struct {
	ID            string         `json:"id,omitempty"`
	Key           string         `json:"key"`
	Title         string         `json:"title,omitempty"`
	Content       string         `json:"content"`
	Enabled       *bool          `json:"enabled,omitempty"`
	SchemaVersion int            `json:"schema_version,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Revision      int            `json:"revision,omitempty"`
	ETag          string         `json:"etag,omitempty"`
	Deleted       bool           `json:"deleted,omitempty"`
}

// LoadWorkspace reads and validates one workspace's local authority. A missing
// file means that workspace has no prompt notes; malformed or mismatched data is
// an error so it cannot silently change the system prompt.
func (p *FileProvider) LoadWorkspace(ctx context.Context, workspaceID string) ([]Note, error) {
	document, exists, err := p.loadDocument(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	notes := make([]Note, 0, len(document.Notes))
	for _, stored := range document.Notes {
		stored = normalizeFileNote(workspaceID, stored)
		if stored.Deleted {
			continue
		}
		enabled := true
		if stored.Enabled != nil {
			enabled = *stored.Enabled
		}
		notes = append(notes, Note{
			ID: stored.ID, Key: stored.Key, Title: stored.Title, Content: stored.Content,
			Enabled: enabled, SchemaVersion: stored.SchemaVersion, Metadata: stored.Metadata,
			Revision: stored.Revision, ETag: stored.ETag,
		})
	}
	return Enabled(notes)
}

func (p *FileProvider) loadDocument(ctx context.Context, workspaceID string) (fileDocument, bool, error) {
	if err := ctx.Err(); err != nil {
		return fileDocument{}, false, err
	}
	path := p.Path(workspaceID)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return fileDocument{SchemaVersion: CurrentSchemaVersion, WorkspaceID: workspaceID, Notes: []fileNote{}}, false, nil
	}
	if err != nil {
		return fileDocument{}, false, fmt.Errorf("promptnotes: open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	limited := io.LimitReader(file, maxFileBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return fileDocument{}, false, fmt.Errorf("promptnotes: read %s: %w", path, err)
	}
	if len(raw) > maxFileBytes {
		return fileDocument{}, false, fmt.Errorf("promptnotes: %s exceeds %d bytes", path, maxFileBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document fileDocument
	if err := decoder.Decode(&document); err != nil {
		return fileDocument{}, false, fmt.Errorf("promptnotes: decode %s: %w", path, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fileDocument{}, false, fmt.Errorf("promptnotes: decode %s: %w", path, err)
	}
	if document.SchemaVersion != CurrentSchemaVersion {
		return fileDocument{}, false, fmt.Errorf("promptnotes: %s uses unsupported document schema version %d", path, document.SchemaVersion)
	}
	if document.WorkspaceID != "" && document.WorkspaceID != workspaceID {
		return fileDocument{}, false, fmt.Errorf("promptnotes: %s belongs to workspace %q, not %q", path, document.WorkspaceID, workspaceID)
	}
	if document.WorkspaceID == "" {
		document.WorkspaceID = workspaceID
	}
	if err := validateFileOperationJournal(document); err != nil {
		return fileDocument{}, false, fmt.Errorf("promptnotes: validate %s: %w", path, err)
	}
	return document, true, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

var _ WorkspaceProvider = (*FileProvider)(nil)
