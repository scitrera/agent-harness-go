// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package promptnotes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFileProviderIsolatesWorkspacesAndOrdersEnabledNotes(t *testing.T) {
	stateDir := t.TempDir()
	provider, err := NewFileProvider(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	workspaceA := provider.Path("project-a")
	if err := os.MkdirAll(filepath.Dir(workspaceA), 0o755); err != nil {
		t.Fatal(err)
	}
	document := `{"schema_version":1,"workspace_id":"project-a","notes":[` +
		`{"key":"z-last","content":"last"},` +
		`{"key":"disabled","content":"off","enabled":false},` +
		`{"key":"a-first","title":"First","content":"first"}]}`
	if err := os.WriteFile(workspaceA, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}

	notes, err := provider.LoadWorkspace(context.Background(), "project-a")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{notes[0].Key, notes[1].Key}; !reflect.DeepEqual(got, []string{"a-first", "z-last"}) {
		t.Fatalf("ordered keys = %v", got)
	}
	if notes[0].SchemaVersion != CurrentSchemaVersion || !notes[0].Enabled {
		t.Fatalf("defaulted note = %#v", notes[0])
	}
	if other, err := provider.LoadWorkspace(context.Background(), "project-b"); err != nil || len(other) != 0 {
		t.Fatalf("other workspace notes = %#v, err = %v", other, err)
	}
	if provider.Path("project-a") == provider.Path("project-b") {
		t.Fatal("workspace paths must be isolated")
	}
}

func TestFileProviderFailsClosedOnMismatchedOrMalformedAuthority(t *testing.T) {
	provider, err := NewFileProvider(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := provider.Path("project-a")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"workspace_id":"project-b","notes":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.LoadWorkspace(context.Background(), "project-a"); err == nil || !strings.Contains(err.Error(), "belongs to workspace") {
		t.Fatalf("mismatch error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"notes":[],"surprise":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.LoadWorkspace(context.Background(), "project-a"); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("malformed error = %v", err)
	}
}

func TestEnabledRejectsDuplicateAndUnsupportedEnabledNotes(t *testing.T) {
	if _, err := Enabled([]Note{
		{Key: "same", Content: "one", Enabled: true, SchemaVersion: 1},
		{Key: "same", Content: "two", Enabled: true, SchemaVersion: 1},
	}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := Enabled([]Note{{Key: "future", Content: "body", Enabled: true, SchemaVersion: 2}}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("schema error = %v", err)
	}
	// A disabled future note is not assembled and must not break current turns.
	if notes, err := Enabled([]Note{{Key: "future", Enabled: false, SchemaVersion: 2}}); err != nil || len(notes) != 0 {
		t.Fatalf("disabled future note = %#v, err = %v", notes, err)
	}
}

type recordingProvider struct {
	workspace string
	err       error
}

func (p *recordingProvider) LoadWorkspace(_ context.Context, workspaceID string) ([]Note, error) {
	p.workspace = workspaceID
	return nil, p.err
}

func TestBindWorkspaceProviderMapsOnlySelectedLogicalDefault(t *testing.T) {
	base := &recordingProvider{}
	bound := BindWorkspaceProvider(base, "logical", "backend")
	if _, err := bound.LoadWorkspace(context.Background(), "logical"); err != nil || base.workspace != "backend" {
		t.Fatalf("selected maps to %q, err = %v", base.workspace, err)
	}
	if _, err := bound.LoadWorkspace(context.Background(), "other"); err != nil || base.workspace != "other" {
		t.Fatalf("explicit other maps to %q, err = %v", base.workspace, err)
	}
	base.err = errors.New("authority unavailable")
	if _, err := bound.LoadWorkspace(context.Background(), "logical"); !errors.Is(err, base.err) {
		t.Fatalf("authority error = %v", err)
	}
}
