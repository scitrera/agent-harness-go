package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestFileStoreHistoryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, dir)
	ctx := context.Background()

	if got, err := s.LoadHistory(ctx, "missing"); err != nil || len(got) != 0 {
		t.Fatalf("missing thread should be empty: %v %d", err, len(got))
	}

	part, _ := protocol.NewTextPart("hi")
	msg := protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
	if err := s.SaveHistory(ctx, "t1", []protocol.ChatMessage{msg}); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.LoadHistory(ctx, "t1")
	if err != nil || len(got) != 1 || got[0].ID != "u1" {
		t.Fatalf("round-trip failed: %v %#v", err, got)
	}
}

func TestFileStoreWorkspaceHistoryIsolation(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, dir)
	ctx := context.Background()
	partA, _ := protocol.NewTextPart("from a")
	partB, _ := protocol.NewTextPart("from b")
	messageA := protocol.ChatMessage{ID: "a", Role: protocol.RoleUser, Content: []protocol.ContentPart{partA}}
	messageB := protocol.ChatMessage{ID: "b", Role: protocol.RoleUser, Content: []protocol.ContentPart{partB}}

	if err := s.SaveWorkspaceHistory(ctx, "project/a", "shared", []protocol.ChatMessage{messageA}); err != nil {
		t.Fatalf("save project a: %v", err)
	}
	if err := s.SaveWorkspaceHistory(ctx, "project\\a", "shared", []protocol.ChatMessage{messageB}); err != nil {
		t.Fatalf("save project b: %v", err)
	}
	gotA, err := s.LoadWorkspaceHistory(ctx, "project/a", "shared")
	if err != nil {
		t.Fatalf("load project a: %v", err)
	}
	gotB, err := s.LoadWorkspaceHistory(ctx, "project\\a", "shared")
	if err != nil {
		t.Fatalf("load project b: %v", err)
	}
	legacy, err := s.LoadHistory(ctx, "shared")
	if err != nil {
		t.Fatalf("load legacy: %v", err)
	}
	if len(gotA) != 1 || gotA[0].ID != "a" || len(gotB) != 1 || gotB[0].ID != "b" || len(legacy) != 0 {
		t.Fatalf("histories leaked: a=%+v b=%+v legacy=%+v", gotA, gotB, legacy)
	}

	if err := s.DeleteWorkspaceHistory(ctx, "project/a", "shared"); err != nil {
		t.Fatalf("delete project a: %v", err)
	}
	gotA, err = s.LoadWorkspaceHistory(ctx, "project/a", "shared")
	if err != nil || len(gotA) != 0 {
		t.Fatalf("project a after delete: %v %+v", err, gotA)
	}
	gotB, err = s.LoadWorkspaceHistory(ctx, "project\\a", "shared")
	if err != nil || len(gotB) != 1 || gotB[0].ID != "b" {
		t.Fatalf("project b after project a delete: %v %+v", err, gotB)
	}
}

func TestFileStoreListsWorkspaceHistoryWithoutCrossWorkspaceLeakage(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, dir)
	ctx := context.Background()
	part, _ := protocol.NewTextPart("hello")
	for _, threadID := range []string{"thread-b", "thread-a"} {
		message := protocol.ChatMessage{
			ID:      "message-" + threadID,
			Role:    protocol.RoleUser,
			Addr:    protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: threadID},
			Content: []protocol.ContentPart{part},
		}
		if err := s.SaveWorkspaceHistory(ctx, "project-a", threadID, []protocol.ChatMessage{message}); err != nil {
			t.Fatalf("save %s: %v", threadID, err)
		}
	}
	other := protocol.ChatMessage{ID: "other", Role: protocol.RoleUser, Addr: protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "thread-a"}, Content: []protocol.ContentPart{part}}
	if err := s.SaveWorkspaceHistory(ctx, "project-b", "thread-a", []protocol.ChatMessage{other}); err != nil {
		t.Fatalf("save other workspace: %v", err)
	}

	histories, err := s.ListWorkspaceHistory(ctx, "project-a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(histories) != 2 || histories[0].ThreadID != "thread-a" || histories[1].ThreadID != "thread-b" {
		t.Fatalf("histories = %+v", histories)
	}
	for _, history := range histories {
		if history.Messages[0].Addr.WorkspaceID != "project-a" {
			t.Fatalf("cross-workspace history leaked: %+v", history)
		}
	}
}

func TestFileStoreSanitizesThreadID(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, dir)
	ctx := context.Background()
	// A thread id with path separators must not escape the history dir.
	if err := s.SaveHistory(ctx, "../../etc/x", nil); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "history")); err != nil {
		t.Fatalf("history dir not created in place: %v", err)
	}
}

func TestFileStoreBootstrapReadsWorkspaceFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "SOUL.md"), []byte("name: Test\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	s := NewFileStore(dir, dir)
	files, err := s.LoadBootstrap(context.Background())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if len(files) != 1 || files[0].Name != "SOUL.md" || files[0].Content != "name: Test\n" {
		t.Fatalf("unexpected bootstrap: %#v", files)
	}
}

func TestFileStoreDeleteHistory_whenTranscriptExists(t *testing.T) {
	// Given
	dir := t.TempDir()
	store := NewFileStore(dir, dir)
	ctx := context.Background()
	part, _ := protocol.NewTextPart("delete me")
	msg := protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
	if err := store.SaveHistory(ctx, "t1", []protocol.ChatMessage{msg}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// When
	if err := store.DeleteHistory(ctx, "t1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := store.LoadHistory(ctx, "t1")

	// Then
	if err != nil {
		t.Fatalf("load after delete: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("history should be empty after delete: %+v", got)
	}
}
