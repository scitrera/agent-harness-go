// Package promptnotes defines the backend-neutral read contract for reusable,
// workspace-scoped prompt supplements.
package promptnotes

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const (
	// CurrentSchemaVersion is the prompt-note content schema understood by this
	// harness. Backends must fail closed on an enabled note with another version.
	CurrentSchemaVersion = 1
	// MaxEnabledNotes and MaxTotalContentBytes keep authoritative reusable
	// guidance from consuming the model's entire context window.
	MaxEnabledNotes      = 128
	MaxTotalContentBytes = 64 << 10
	MaxNoteContentBytes  = 16 << 10
)

// Note is a reusable prompt supplement. Persistence-specific provenance is
// retained so callers can diagnose exactly which authoritative revision was
// assembled, while prompt rendering uses Key, Title, and Content.
type Note struct {
	ID            string         `json:"id,omitempty"`
	Key           string         `json:"key"`
	Title         string         `json:"title,omitempty"`
	Content       string         `json:"content"`
	Enabled       bool           `json:"enabled"`
	SchemaVersion int            `json:"schema_version"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Revision      int            `json:"revision,omitempty"`
	ETag          string         `json:"etag,omitempty"`
}

// WorkspaceProvider loads the current prompt-note heads for one logical
// workspace. The selected implementation is authoritative: an error must be
// surfaced, not replaced with data from another provider.
type WorkspaceProvider interface {
	LoadWorkspace(ctx context.Context, workspaceID string) ([]Note, error)
}

// Enabled validates, bounds, copies, and deterministically orders enabled notes.
// Disabled notes do not affect prompt construction and are ignored.
func Enabled(notes []Note) ([]Note, error) {
	out := make([]Note, 0, len(notes))
	seen := make(map[string]struct{}, len(notes))
	totalBytes := 0
	for _, note := range notes {
		if !note.Enabled {
			continue
		}
		note.Key = strings.TrimSpace(note.Key)
		note.Title = strings.TrimSpace(note.Title)
		if note.SchemaVersion == 0 {
			note.SchemaVersion = CurrentSchemaVersion
		}
		if note.Key == "" {
			return nil, fmt.Errorf("promptnotes: enabled note has an empty key")
		}
		if _, duplicate := seen[note.Key]; duplicate {
			return nil, fmt.Errorf("promptnotes: duplicate enabled note key %q", note.Key)
		}
		seen[note.Key] = struct{}{}
		if note.SchemaVersion != CurrentSchemaVersion {
			return nil, fmt.Errorf("promptnotes: note %q uses unsupported schema version %d", note.Key, note.SchemaVersion)
		}
		if strings.TrimSpace(note.Content) == "" {
			return nil, fmt.Errorf("promptnotes: enabled note %q has empty content", note.Key)
		}
		if len(note.Content) > MaxNoteContentBytes {
			return nil, fmt.Errorf("promptnotes: note %q exceeds %d content bytes", note.Key, MaxNoteContentBytes)
		}
		totalBytes += len(note.Content)
		if totalBytes > MaxTotalContentBytes {
			return nil, fmt.Errorf("promptnotes: enabled notes exceed %d total content bytes", MaxTotalContentBytes)
		}
		if len(out) == MaxEnabledNotes {
			return nil, fmt.Errorf("promptnotes: more than %d enabled notes", MaxEnabledNotes)
		}
		note.Metadata = cloneMetadata(note.Metadata)
		out = append(out, note)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func cloneMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	out := make(map[string]any, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

// boundWorkspaceProvider maps only the process-selected logical workspace to a
// backend-specific default. Explicitly addressed other workspaces pass through.
type boundWorkspaceProvider struct {
	base               WorkspaceProvider
	logicalWorkspace   string
	backendWorkspaceID string
}

// BindWorkspaceProvider preserves logical multi-workspace routing when a
// backend default differs from the selected logical workspace.
func BindWorkspaceProvider(base WorkspaceProvider, logicalWorkspace, backendWorkspace string) WorkspaceProvider {
	if base == nil || logicalWorkspace == "" || logicalWorkspace == backendWorkspace {
		return base
	}
	return &boundWorkspaceProvider{base: base, logicalWorkspace: logicalWorkspace, backendWorkspaceID: backendWorkspace}
}

func (p *boundWorkspaceProvider) LoadWorkspace(ctx context.Context, workspaceID string) ([]Note, error) {
	if workspaceID == "" || workspaceID == p.logicalWorkspace {
		workspaceID = p.backendWorkspaceID
	}
	return p.base.LoadWorkspace(ctx, workspaceID)
}
