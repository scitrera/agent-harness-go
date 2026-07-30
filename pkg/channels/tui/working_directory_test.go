package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
)

func TestWorkingDirectoryCommandsStayInsideWorkspace(t *testing.T) {
	root := t.TempDir()
	subdirectory := filepath.Join(root, "src")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	processCWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get process cwd: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}

	next, cmd := m.handleSlash("/cd src")
	if cmd != nil {
		t.Fatal("/cd should be local")
	}
	updated := next.(model)
	if updated.cwd != subdirectory {
		t.Fatalf("cwd = %q, want %q", updated.cwd, subdirectory)
	}
	if current, getErr := os.Getwd(); getErr != nil || current != processCWD {
		t.Fatalf("process cwd changed: got %q error %v, want %q", current, getErr, processCWD)
	}

	next, cmd = updated.handleSlash("/pwd")
	if cmd != nil {
		t.Fatal("/pwd should be local")
	}
	updated = next.(model)
	if got := updated.rows[len(updated.rows)-1].Text; got != subdirectory {
		t.Fatalf("/pwd output = %q, want %q", got, subdirectory)
	}

	next, _ = updated.handleSlash("/cd ..")
	updated = next.(model)
	if updated.cwd != root {
		t.Fatalf("cwd after .. = %q, want workspace root %q", updated.cwd, root)
	}
}

func TestWorkingDirectoryCommandRejectsOutsideAndNonDirectoryPaths(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	file := filepath.Join(root, "file.txt")
	if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}

	next, _ := m.handleSlash("/cd " + outside)
	updated := next.(model)
	if updated.cwd != root {
		t.Fatalf("outside /cd changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "outside the workspace") {
		t.Fatalf("outside /cd error = %q", got)
	}

	next, _ = updated.handleSlash("/cd file.txt")
	updated = next.(model)
	if updated.cwd != root {
		t.Fatalf("file /cd changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "not a directory") {
		t.Fatalf("file /cd error = %q", got)
	}
}

func TestWorkingDirectoryRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "outside-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}

	next, _ := m.handleSlash("/cd outside-link")
	updated := next.(model)
	if updated.cwd != root {
		t.Fatalf("symlink escape changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "outside the workspace") {
		t.Fatalf("symlink escape error = %q", got)
	}
}
