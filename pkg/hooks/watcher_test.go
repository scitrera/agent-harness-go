package hooks

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func Test_PollingWatcher_detects_add_change_and_unlink(t *testing.T) {
	// Given
	ctx := context.Background()
	dir := t.TempDir()
	watcher, err := NewPollingWatcher([]string{dir})
	if err != nil {
		t.Fatalf("new watcher: %v", err)
	}
	filePath := filepath.Join(dir, "settings.json")

	// When add
	if err := os.WriteFile(filePath, []byte(`{"v":1}`), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	events, err := watcher.Poll(ctx)

	// Then add
	if err != nil {
		t.Fatalf("poll add: %v", err)
	}
	requireOneFileEvent(t, events, filePath, FileChangeAdd)

	// When change
	if err := os.WriteFile(filePath, []byte(`{"v":2,"changed":true}`), 0o600); err != nil {
		t.Fatalf("rewrite file: %v", err)
	}
	events, err = watcher.Poll(ctx)

	// Then change
	if err != nil {
		t.Fatalf("poll change: %v", err)
	}
	requireOneFileEvent(t, events, filePath, FileChangeChange)

	// When unlink
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("remove file: %v", err)
	}
	events, err = watcher.Poll(ctx)

	// Then unlink
	if err != nil {
		t.Fatalf("poll unlink: %v", err)
	}
	requireOneFileEvent(t, events, filePath, FileChangeUnlink)
}

func Test_CWDWatcher_detects_working_directory_change(t *testing.T) {
	// Given
	start, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(start); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
	watcher, err := NewCWDWatcher()
	if err != nil {
		t.Fatalf("new cwd watcher: %v", err)
	}
	next := t.TempDir()

	// When
	if err := os.Chdir(next); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	event, changed, err := watcher.Poll(context.Background())

	// Then
	if err != nil {
		t.Fatalf("poll cwd: %v", err)
	}
	if !changed || event.OldCWD != start || event.NewCWD != next {
		t.Fatalf("unexpected cwd event changed=%v event=%#v", changed, event)
	}
}

func requireOneFileEvent(t *testing.T, events []FileEvent, path string, kind FileChangeKind) {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("expected one file event, got %#v", events)
	}
	if events[0].Path != path || events[0].Kind != kind {
		t.Fatalf("unexpected file event: %#v", events[0])
	}
}
