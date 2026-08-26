package tui

import (
	"path/filepath"
	"testing"
)

func TestFileShellPreferencesResolveHierarchyAndPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shell.json")
	store, err := NewFileShellPreferenceStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Resolve("drew", "ws", "thread", true); !got.Effective || got.Source != "global" {
		t.Fatalf("global resolution = %#v", got)
	}
	off := false
	if err := store.SetUser("drew", &off); err != nil {
		t.Fatal(err)
	}
	on := true
	if err := store.SetThread("drew", "ws", "thread", &on); err != nil {
		t.Fatal(err)
	}
	if got := store.Resolve("drew", "ws", "thread", true); !got.Effective || got.Source != "thread" {
		t.Fatalf("thread resolution = %#v", got)
	}
	if got := store.Resolve("drew", "ws", "other", true); got.Effective || got.Source != "user" {
		t.Fatalf("user resolution = %#v", got)
	}
	reloaded, err := NewFileShellPreferenceStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Resolve("drew", "ws", "thread", false); !got.Effective || got.Source != "thread" {
		t.Fatalf("persisted resolution = %#v", got)
	}
	if err := reloaded.SetThread("drew", "ws", "thread", nil); err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Resolve("drew", "ws", "thread", true); got.Effective || got.Source != "user" {
		t.Fatalf("cleared thread resolution = %#v", got)
	}
}
