package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func TestSendCurrentNormalizesFileReferencesAndAttachesReferencedImages(t *testing.T) {
	root := t.TempDir()
	subdirectory := filepath.Join(root, "src")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subdirectory, "main.go"), []byte("package main"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subdirectory, "pixel.png"), tinyPNG, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatalf("touch thread: %v", err)
	}
	channel := NewChannel()
	m := model{
		ctx:              context.Background(),
		channel:          channel,
		index:            index,
		workspaceRoot:    root,
		cwd:              subdirectory,
		threadID:         "thread-1",
		viewport:         viewport.New(),
		composer:         newComposer(),
		turns:            map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		tailing:          true,
	}
	m.resize(80, 20)
	m.composer.SetValue("Review @main.go and describe @pixel.png.")

	next, cmd := m.sendCurrent()
	if cmd == nil {
		t.Fatalf("referenced message did not emit a send command: model=%+v", next.(model).rows)
	}
	updated := next.(model)
	if got := updated.rows[len(updated.rows)-2].Text; got != "Review @src/main.go and describe @src/pixel.png." {
		t.Fatalf("normalized optimistic row = %q", got)
	}
	if result := cmd(); result.(sendResultMsg).Err != nil {
		t.Fatalf("send referenced message: %+v", result)
	}
	inbound, fetchErr := channel.FetchTask(context.Background())
	if fetchErr != nil {
		t.Fatalf("fetch inbound: %v", fetchErr)
	}
	if got := messageText(inbound.Message); !strings.Contains(got, "Review @src/main.go and describe @src/pixel.png.") {
		t.Fatalf("inbound reference text = %q", got)
	}
	if len(inbound.Message.Content) != 2 {
		t.Fatalf("inbound content parts = %d, want text + referenced image", len(inbound.Message.Content))
	}
	if _, ok := inbound.Message.Content[1].AsImage(); !ok {
		t.Fatalf("referenced image part = %+v", inbound.Message.Content[1])
	}
}

func TestSendCurrentKeepsComposerWhenReferenceIsInvalid(t *testing.T) {
	root := t.TempDir()
	m := model{
		ctx:              context.Background(),
		channel:          NewChannel(),
		workspaceRoot:    root,
		cwd:              root,
		viewport:         viewport.New(),
		composer:         newComposer(),
		turns:            map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		tailing:          true,
	}
	m.composer.SetValue("Review @missing.go")

	next, cmd := m.sendCurrent()
	if cmd != nil {
		t.Fatal("invalid reference should not enqueue")
	}
	updated := next.(model)
	if got := updated.composer.Value(); got != "Review @missing.go" {
		t.Fatalf("composer after invalid reference = %q", got)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "reference failed") {
		t.Fatalf("invalid reference error = %q", got)
	}
}

func TestResolveAtReferencesSupportsQuotedPaths(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "notes file.txt"), []byte("notes"), 0o600); err != nil {
		t.Fatalf("write notes: %v", err)
	}
	m := model{workspaceRoot: root, cwd: root}

	text, images, err := m.resolveAtReferences(`Summarize @"notes file.txt".`)
	if err != nil {
		t.Fatalf("resolve quoted reference: %v", err)
	}
	if text != `Summarize @"notes file.txt".` {
		t.Fatalf("quoted normalized text = %q", text)
	}
	if len(images) != 0 {
		t.Fatalf("text file became image reference: %+v", images)
	}
}
