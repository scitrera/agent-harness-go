package promptnotes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/refinement"
)

func TestFileEditorCreateReplayReplaceDeleteUsesSameProviderAuthority(t *testing.T) {
	stateDir := t.TempDir()
	editor, err := NewFileEditor(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	provider, _ := NewFileProvider(stateDir)
	create := refinement.Edit{
		Action: refinement.ActionCreate, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "coding/conventions",
		Content: map[string]any{"title": "Conventions", "content": "Run tests.", "metadata": map[string]any{"source": "refinement"}},
	}
	created, err := editor.Apply(context.Background(), "project-a", "create-op", create)
	if err != nil {
		t.Fatal(err)
	}
	if created.Before != nil || created.After == nil || created.After.ResourceID == "" || created.After.ETag == "" {
		t.Fatalf("created = %#v", created)
	}
	replay, err := editor.Apply(context.Background(), "project-a", "create-op", create)
	if err != nil || !replay.Replayed || replay.After.ETag != created.After.ETag {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	notes, err := provider.LoadWorkspace(context.Background(), "project-a")
	if err != nil || len(notes) != 1 || notes[0].ID != created.After.ResourceID || notes[0].ETag != created.After.ETag {
		t.Fatalf("provider notes = %#v, %v", notes, err)
	}

	replace := refinement.Edit{
		Action: refinement.ActionReplace, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "coding/conventions",
		ResourceID: created.After.ResourceID, ExpectedETag: created.After.ETag,
		Content: map[string]any{"title": "Conventions", "content": "Run all tests.", "enabled": true, "schema_version": 1},
	}
	replaced, err := editor.Apply(context.Background(), "project-a", "replace-op", replace)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Before.ETag != created.After.ETag || replaced.After.ETag == created.After.ETag || replaced.After.Content["content"] != "Run all tests." {
		t.Fatalf("replaced = %#v", replaced)
	}
	stale := replace
	stale.Content = map[string]any{"title": "Stale", "content": "Stale"}
	if _, err := editor.Apply(context.Background(), "project-a", "stale-op", stale); !errors.Is(err, refinement.ErrConflict) {
		t.Fatalf("stale replace error = %v", err)
	}

	deleted, err := editor.Apply(context.Background(), "project-a", "delete-op", refinement.Edit{
		Action: refinement.ActionDelete, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "coding/conventions",
		ResourceID: created.After.ResourceID, ExpectedETag: replaced.After.ETag,
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Before == nil || deleted.After == nil || !deleted.After.Deleted || deleted.After.ETag == replaced.After.ETag {
		t.Fatalf("deleted = %#v", deleted)
	}
	if notes, err := provider.LoadWorkspace(context.Background(), "project-a"); err != nil || len(notes) != 0 {
		t.Fatalf("notes after delete = %#v, %v", notes, err)
	}
	deleteReplay, err := editor.Apply(context.Background(), "project-a", "delete-op", refinement.Edit{
		Action: refinement.ActionDelete, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "coding/conventions",
		ResourceID: created.After.ResourceID, ExpectedETag: replaced.After.ETag,
	})
	if err != nil || !deleteReplay.Replayed || !deleteReplay.After.Deleted {
		t.Fatalf("delete replay = %#v, %v", deleteReplay, err)
	}
}

func TestFileProviderSynthesizesCASIdentityForLegacyNotes(t *testing.T) {
	provider, _ := NewFileProvider(t.TempDir())
	path := provider.Path("project")
	writePromptNoteTestFile(t, path, `{"schema_version":1,"workspace_id":"project","notes":[{"key":"legacy","content":"Legacy note"}]}`)
	notes, err := provider.LoadWorkspace(context.Background(), "project")
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || notes[0].ID == "" || notes[0].ETag == "" || notes[0].Revision != 1 {
		t.Fatalf("legacy note = %#v", notes)
	}
	second, err := provider.LoadWorkspace(context.Background(), "project")
	if err != nil || second[0].ID != notes[0].ID || second[0].ETag != notes[0].ETag {
		t.Fatalf("synthetic identity is not deterministic: %#v, %v", second, err)
	}
}

func writePromptNoteTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
