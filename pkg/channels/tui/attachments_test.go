package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

var tinyPNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
	0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
}

func TestLoadWorkspaceImage_buildsInlineMultimodalPart(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sample.png")
	if err := os.WriteFile(path, tinyPNG, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}

	attachment, err := loadWorkspaceImage(context.Background(), root, "sample.png")
	if err != nil {
		t.Fatalf("load image: %v", err)
	}

	if attachment.Name != "sample.png" || attachment.Mime != "image/png" {
		t.Fatalf("attachment metadata = %+v", attachment)
	}
	image, ok := attachment.Part.AsImage()
	if !ok {
		t.Fatalf("attachment part is not an image: %+v", attachment.Part)
	}
	if !strings.HasPrefix(image.DataURI, "data:image/png;base64,") || image.AltText != "sample.png" {
		t.Fatalf("image payload metadata = %+v", image)
	}
}

func TestLoadWorkspaceImage_rejectsPathOutsideWorkspace(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.png")
	if err := os.WriteFile(outside, tinyPNG, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}

	_, err := loadWorkspaceImage(context.Background(), root, outside)
	if err == nil || !strings.Contains(err.Error(), "outside the workspace") {
		t.Fatalf("outside path error = %v", err)
	}
}

func TestSendCurrent_includesAndClearsQueuedImage(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sample.png")
	if err := os.WriteFile(path, tinyPNG, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	attachment, err := loadWorkspaceImage(context.Background(), root, path)
	if err != nil {
		t.Fatalf("load image: %v", err)
	}
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatalf("touch thread: %v", err)
	}
	ch := NewChannel()
	m := model{
		ctx:              context.Background(),
		channel:          ch,
		index:            index,
		threadID:         "thread-1",
		viewport:         viewport.New(),
		composer:         newComposer(),
		attachments:      []pendingAttachment{attachment},
		turns:            map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		tailing:          true,
	}
	m.resize(80, 20)
	m.composer.SetValue("Describe this image.")

	next, cmd := m.sendCurrent()
	if cmd == nil {
		t.Fatal("send should enqueue a multimodal message")
	}
	updated := next.(model)
	if len(updated.attachments) != 0 {
		t.Fatalf("attachments were not cleared after send: %+v", updated.attachments)
	}
	if len(updated.rows) < 2 || !strings.Contains(updated.rows[len(updated.rows)-2].Text, "[image: sample.png]") {
		t.Fatalf("optimistic row omitted attachment marker: %+v", updated.rows)
	}
	if result := cmd(); result.(sendResultMsg).Err != nil {
		t.Fatalf("send command failed: %+v", result)
	}
	inbound, err := ch.FetchTask(context.Background())
	if err != nil {
		t.Fatalf("fetch inbound: %v", err)
	}
	if len(inbound.Message.Content) != 2 {
		t.Fatalf("message content count = %d, want text + image", len(inbound.Message.Content))
	}
	if _, ok := inbound.Message.Content[1].AsImage(); !ok {
		t.Fatalf("second content part is not image: %+v", inbound.Message.Content[1])
	}
}

func TestUpdatePaste_queuesDroppedWorkspaceImage(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sample.png")
	if err := os.WriteFile(path, tinyPNG, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	m := model{
		ctx:           context.Background(),
		channel:       NewChannel(),
		workspaceRoot: root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}
	m.resize(60, 20)

	next, cmd := m.updatePaste(tea.PasteMsg{Content: "sample.png"})
	if cmd == nil {
		t.Fatal("pasted workspace image should start attachment loading")
	}
	updated := next.(model)
	if got := updated.composer.Value(); got != "" {
		t.Fatalf("dropped image path leaked into composer: %q", got)
	}
	updated.applyAttachmentLoaded(cmd().(attachmentLoadedMsg))
	if len(updated.attachments) != 1 || updated.attachments[0].Name != "sample.png" {
		t.Fatalf("queued attachments = %+v", updated.attachments)
	}
}

func TestUpdatePaste_preservesOrdinaryText(t *testing.T) {
	m := model{
		ctx:           context.Background(),
		channel:       NewChannel(),
		workspaceRoot: t.TempDir(),
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}
	m.resize(60, 20)

	next, _ := m.updatePaste(tea.PasteMsg{Content: "ordinary pasted text"})
	updated := next.(model)
	if got := updated.composer.Value(); got != "ordinary pasted text" {
		t.Fatalf("composer value = %q, want ordinary paste preserved", got)
	}
	if len(updated.attachments) != 0 {
		t.Fatalf("ordinary paste created attachments: %+v", updated.attachments)
	}
}
