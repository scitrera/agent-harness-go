package tui

import (
	"os"
	"path/filepath"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
)

func TestAtPathCompletionCompletesFilesAndDescendsDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "src"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "src", "main.go"), []byte("package main"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "summary.txt"), []byte("summary"), 0o600); err != nil {
		t.Fatalf("write summary: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}
	m.resize(80, 20)
	m.composer.SetValue("Review @su")
	m.refreshInputSurface()

	if m.selector.kind != selectionPath || len(m.selector.items) != 1 {
		t.Fatalf("@ path selector = kind %v items %+v", m.selector.kind, m.selector.items)
	}
	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	if cmd != nil {
		t.Fatal("file completion should not emit a command")
	}
	updated := next.(model)
	if got := updated.composer.Value(); got != "Review @summary.txt " {
		t.Fatalf("completed file value = %q", got)
	}
	if updated.selector.active() {
		t.Fatal("file completion should close the path selector")
	}

	updated.composer.SetValue("Review @sr")
	updated.refreshInputSurface()
	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	updated = next.(model)
	if got := updated.composer.Value(); got != "Review @src/" {
		t.Fatalf("completed directory value = %q", got)
	}
	if updated.selector.kind != selectionPath || len(updated.selector.items) != 1 {
		t.Fatalf("nested path selector = kind %v items %+v", updated.selector.kind, updated.selector.items)
	}
	if got := updated.selector.items[0].Label; got != "@src/main.go" {
		t.Fatalf("nested suggestion label = %q", got)
	}
}

func TestPathCompletionQuotesSpacesAndCdOnlySuggestsDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "docs and notes"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "data.txt"), []byte("data"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}
	m.resize(80, 20)
	m.composer.SetValue("Use @do")
	m.refreshInputSurface()

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	updated := next.(model)
	if got := updated.composer.Value(); got != `Use @"docs and notes/` {
		t.Fatalf("space-containing directory completion = %q", got)
	}

	updated.composer.SetValue("/cd ")
	updated.refreshInputSurface()
	if updated.selector.kind != selectionPath {
		t.Fatalf("/cd selector kind = %v, want path", updated.selector.kind)
	}
	for _, item := range updated.selector.items {
		if item.Description != "directory" {
			t.Fatalf("/cd suggested non-directory: %+v", item)
		}
	}
	if len(updated.selector.items) != 1 {
		t.Fatalf("/cd suggestions = %+v, want one directory", updated.selector.items)
	}
}

func TestAtPathCompletionIsRelativeToVirtualCWD(t *testing.T) {
	root := t.TempDir()
	subdirectory := filepath.Join(root, "src")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subdirectory, "main.go"), []byte("package main"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           subdirectory,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}
	m.composer.SetValue("@ma")
	m.refreshInputSurface()

	if m.selector.kind != selectionPath || len(m.selector.items) != 1 {
		t.Fatalf("cwd-relative suggestions = kind %v items %+v", m.selector.kind, m.selector.items)
	}
	if got := m.selector.items[0].Value; got != "@main.go " {
		t.Fatalf("cwd-relative completion value = %q", got)
	}
}
