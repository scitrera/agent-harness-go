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
