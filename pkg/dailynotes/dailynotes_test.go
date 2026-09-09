// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package dailynotes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeNote(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestLoadFormatsUntrustedBlocksInRange(t *testing.T) {
	root := t.TempDir()
	mem := filepath.Join(root, "memory")
	if err := os.MkdirAll(mem, 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	writeNote(t, mem, "2026-06-22.md", "met with Drew about billing")
	writeNote(t, mem, "2026-06-21-standup.md", "standup notes")
	writeNote(t, mem, "2026-01-01.md", "old note out of range")

	out, err := Load(root, now, 2)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, want := range []string{
		"UNTRUSTED",
		"[Untrusted daily memory: memory/2026-06-22.md]",
		"met with Drew about billing",
		"[Untrusted daily memory: memory/2026-06-21-standup.md]",
		"standup notes",
		"BEGIN_QUOTED_NOTES",
		"END_QUOTED_NOTES",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("daily notes missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "out of range") {
		t.Fatalf("out-of-range note should be excluded:\n%s", out)
	}
	// Most recent first.
	if strings.Index(out, "2026-06-22.md") > strings.Index(out, "2026-06-21-standup.md") {
		t.Fatalf("expected most-recent-first ordering:\n%s", out)
	}
}

func TestLoadNoMemoryDir(t *testing.T) {
	out, err := Load(t.TempDir(), time.Now(), 2)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if out != "" {
		t.Fatalf("expected empty result when no memory dir, got %q", out)
	}
}

func TestLoadEmptyWorkspaceRoot(t *testing.T) {
	if out, err := Load("", time.Now(), 2); err != nil || out != "" {
		t.Fatalf("expected empty/no-error for empty root, got %q / %v", out, err)
	}
}
