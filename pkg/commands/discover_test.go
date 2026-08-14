package commands

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestDiscover(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "commands", "commit.md"), `---
description: Make a commit
argument-hint: <message>
model: scitrera-fast
allowed-tools: shell, read_file
---
Commit the staged changes with message: $ARGUMENTS`)
	writeFile(t, filepath.Join(root, "commands", "git", "rebase.md"), "Rebase onto $1")
	// Reserved name must be ignored even if present on disk.
	writeFile(t, filepath.Join(root, "commands", "help.md"), "should be ignored")
	// Non-markdown ignored.
	writeFile(t, filepath.Join(root, "commands", "notes.txt"), "nope")

	cmds, err := Discover(root, []string{"commands", "missing-dir"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	byName := map[string]Command{}
	for _, c := range cmds {
		byName[c.Name] = c
	}
	if _, ok := byName["help"]; ok {
		t.Fatal("reserved /help must not be discovered from disk")
	}
	if _, ok := byName["notes"]; ok {
		t.Fatal("non-markdown file must not be discovered")
	}

	commit, ok := byName["commit"]
	if !ok {
		t.Fatalf("commit not discovered: %#v", byName)
	}
	if commit.Description != "Make a commit" || commit.ArgumentHint != "<message>" || commit.Model != "scitrera-fast" {
		t.Fatalf("commit frontmatter wrong: %#v", commit)
	}
	if len(commit.AllowedTools) != 2 || commit.AllowedTools[0] != "shell" || commit.AllowedTools[1] != "read_file" {
		t.Fatalf("commit allowed-tools wrong: %#v", commit.AllowedTools)
	}
	if commit.Body != "Commit the staged changes with message: $ARGUMENTS" {
		t.Fatalf("commit body wrong: %q", commit.Body)
	}

	rebase, ok := byName["git:rebase"]
	if !ok {
		t.Fatalf("namespaced git:rebase not discovered: %#v", byName)
	}
	if rebase.Body != "Rebase onto $1" || rebase.Description != "(no description)" {
		t.Fatalf("rebase wrong: %#v", rebase)
	}
}

func TestDiscoverEmptyRoot(t *testing.T) {
	cmds, err := Discover("", []string{"commands"})
	if err != nil || cmds != nil {
		t.Fatalf("empty root should yield nil, got %#v err=%v", cmds, err)
	}
}

func TestDiscoverAbsoluteRootAndWorkspacePrecedence(t *testing.T) {
	workspace := t.TempDir()
	system := t.TempDir()
	writeFile(t, filepath.Join(workspace, "commands", "review.md"), "workspace review")
	writeFile(t, filepath.Join(system, "review.md"), "system review")
	writeFile(t, filepath.Join(system, "host.md"), "host command")

	cmds, err := Discover(workspace, []string{"commands", system})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	byName := map[string]Command{}
	for _, cmd := range cmds {
		byName[cmd.Name] = cmd
	}
	if got := byName["review"].Body; got != "workspace review" {
		t.Fatalf("workspace command did not shadow system command: %q", got)
	}
	host, ok := byName["host"]
	if !ok || host.Body != "host command" {
		t.Fatalf("absolute host command = %#v", host)
	}
	if want := filepath.Join(system, "host.md"); host.Path != want {
		t.Fatalf("absolute command path = %q, want %q", host.Path, want)
	}
}
