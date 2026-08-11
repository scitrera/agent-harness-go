package promptnotes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	conflictingCreate := create
	conflictingCreate.Content = map[string]any{"content": "different request"}
	if _, err := editor.Apply(context.Background(), "project-a", "create-op", conflictingCreate); !errors.Is(err, refinement.ErrConflict) {
		t.Fatalf("conflicting operation reuse error = %v", err)
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
	if _, err := editor.Apply(context.Background(), "project-a", "restore-op", refinement.Edit{
		Action: refinement.ActionRestore, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "coding/conventions",
		ResourceID: created.After.ResourceID, ExpectedETag: deleted.After.ETag,
	}); !errors.Is(err, refinement.ErrUnsupportedResource) {
		t.Fatalf("local restore error = %v", err)
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

func TestFileEditorCompactsOperationReceiptsAndRetainsRecentReplay(t *testing.T) {
	stateDir := t.TempDir()
	editor, err := NewFileEditor(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	provider, _ := NewFileProvider(stateDir)
	create := refinement.Edit{
		Action: refinement.ActionCreate, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "rolling",
		Content: map[string]any{"content": "revision-0"},
	}
	current, err := editor.Apply(context.Background(), "project-a", "create-op", create)
	if err != nil {
		t.Fatal(err)
	}

	var firstEdit, latestEdit refinement.Edit
	for i := 1; i <= 640; i++ {
		latestEdit = refinement.Edit{
			Action: refinement.ActionReplace, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "rolling",
			ResourceID: current.After.ResourceID, ExpectedETag: current.After.ETag,
			Content: map[string]any{"content": fmt.Sprintf("revision-%d", i)},
		}
		if i == 1 {
			firstEdit = latestEdit
		}
		current, err = editor.Apply(context.Background(), "project-a", fmt.Sprintf("replace-%03d", i), latestEdit)
		if err != nil {
			t.Fatalf("replace %d: %v", i, err)
		}
		if i == 512 {
			document := readFileDocument(t, provider.Path("project-a"))
			assertOperationJournal(t, document, 1, 128, "replace-128", "replace-512")

			restarted, restartErr := NewFileEditor(stateDir)
			if restartErr != nil {
				t.Fatal(restartErr)
			}
			replayed, replayErr := restarted.Apply(context.Background(), "project-a", "replace-512", latestEdit)
			if replayErr != nil || !replayed.Replayed || replayed.After.ETag != current.After.ETag {
				t.Fatalf("recent replay after restart = %#v, %v", replayed, replayErr)
			}
		}
	}

	document := readFileDocument(t, provider.Path("project-a"))
	assertOperationJournal(t, document, 2, 256, "replace-256", "replace-640")
	if len(document.Notes) != 1 || document.Notes[0].ETag != current.After.ETag || document.Notes[0].Content != "revision-640" {
		t.Fatalf("resource head changed during compaction: %#v", document.Notes)
	}

	beforeExpiredRetry, err := os.ReadFile(provider.Path("project-a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := editor.Apply(context.Background(), "project-a", "create-op", create); !errors.Is(err, refinement.ErrConflict) {
		t.Fatalf("expired create retry error = %v", err)
	}
	if _, err := editor.Apply(context.Background(), "project-a", "replace-001", firstEdit); !errors.Is(err, refinement.ErrConflict) {
		t.Fatalf("expired replace retry error = %v", err)
	}
	afterExpiredRetry, err := os.ReadFile(provider.Path("project-a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(afterExpiredRetry) != string(beforeExpiredRetry) {
		t.Fatal("expired stale retry changed the authority document")
	}

	other, err := editor.Apply(context.Background(), "project-b", "create-op", refinement.Edit{
		Action: refinement.ActionCreate, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "rolling",
		Content: map[string]any{"content": "other workspace"},
	})
	if err != nil || other.After == nil || other.After.ResourceID == current.After.ResourceID {
		t.Fatalf("other workspace mutation = %#v, %v", other, err)
	}
	otherDocument := readFileDocument(t, provider.Path("project-b"))
	if len(otherDocument.Operations) != 1 || otherDocument.OperationJournal != nil {
		t.Fatalf("other workspace journal = %#v", otherDocument)
	}
}

func TestFileEditorRejectsCorruptOperationJournal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fileDocument)
	}{
		{
			name: "unsupported policy",
			mutate: func(document *fileDocument) {
				document.OperationJournal = &fileOperationJournal{PolicyVersion: 99, Generation: 1, CompactedOperations: 128}
			},
		},
		{
			name: "inconsistent generation",
			mutate: func(document *fileDocument) {
				document.OperationJournal = &fileOperationJournal{PolicyVersion: 1, Generation: 2, CompactedOperations: 128}
			},
		},
		{
			name: "too many retained results",
			mutate: func(document *fileDocument) {
				for len(document.Operations) <= maxLocalRefinementOperations {
					document.Operations = append(document.Operations, document.Operations[0])
				}
			},
		},
		{
			name:   "invalid request hash",
			mutate: func(document *fileDocument) { document.Operations[0].RequestHash = "not-a-hash" },
		},
		{
			name: "duplicate operation",
			mutate: func(document *fileDocument) {
				document.Operations = append(document.Operations, document.Operations[0])
			},
		},
		{
			name:   "missing accepted result",
			mutate: func(document *fileDocument) { document.Operations[0].Mutation.After = nil },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stateDir := t.TempDir()
			editor, _ := NewFileEditor(stateDir)
			provider, _ := NewFileProvider(stateDir)
			created, err := editor.Apply(context.Background(), "project", "create-op", refinement.Edit{
				Action: refinement.ActionCreate, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "note",
				Content: map[string]any{"content": "before"},
			})
			if err != nil {
				t.Fatal(err)
			}
			document := readFileDocument(t, provider.Path("project"))
			test.mutate(&document)
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			writePromptNoteTestFile(t, provider.Path("project"), string(encoded))
			if _, loadErr := provider.LoadWorkspace(context.Background(), "project"); !errors.Is(loadErr, refinement.ErrInvalid) {
				t.Fatalf("corrupt journal read error = %v", loadErr)
			}

			_, err = editor.Apply(context.Background(), "project", "replace-op", refinement.Edit{
				Action: refinement.ActionReplace, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "note",
				ResourceID: created.After.ResourceID, ExpectedETag: created.After.ETag,
				Content: map[string]any{"content": "after"},
			})
			if !errors.Is(err, refinement.ErrInvalid) {
				t.Fatalf("corrupt journal error = %v", err)
			}
		})
	}
}

func TestFileEditorSerializesConcurrentCreates(t *testing.T) {
	editor, err := NewFileEditor(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const count = 32
	errorsCh := make(chan error, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			key := fmt.Sprintf("note-%02d", index)
			_, applyErr := editor.Apply(context.Background(), "project", "create-"+key, refinement.Edit{
				Action: refinement.ActionCreate, ResourceKind: refinement.ResourcePromptNote, ResourceKey: key,
				Content: map[string]any{"content": key},
			})
			errorsCh <- applyErr
		}(i)
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	notes, err := editor.provider.LoadWorkspace(context.Background(), "project")
	if err != nil || len(notes) != count {
		t.Fatalf("concurrent notes = %d, %v", len(notes), err)
	}
}

func assertOperationJournal(t *testing.T, document fileDocument, generation, compacted uint64, firstOperation, lastOperation string) {
	t.Helper()
	if document.OperationJournal == nil || document.OperationJournal.PolicyVersion != localOperationJournalPolicy ||
		document.OperationJournal.Generation != generation || document.OperationJournal.CompactedOperations != compacted {
		t.Fatalf("operation journal metadata = %#v", document.OperationJournal)
	}
	if len(document.Operations) != retainedLocalRefinementOperations+1 ||
		document.Operations[0].OperationID != firstOperation ||
		document.Operations[len(document.Operations)-1].OperationID != lastOperation {
		t.Fatalf("retained operations: count=%d first=%q last=%q", len(document.Operations), document.Operations[0].OperationID, document.Operations[len(document.Operations)-1].OperationID)
	}
}

func readFileDocument(t *testing.T, path string) fileDocument {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document fileDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
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
