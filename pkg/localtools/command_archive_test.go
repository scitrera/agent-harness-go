// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"context"
	"strings"
	"testing"
	"time"
)

func Test_Workspace_RunCommand_retains_head_and_tail_for_the_archive(t *testing.T) {
	// Given: output far larger than both the visible cap and the archive budget,
	// with distinguishable ends so we can prove which windows survived.
	ctx := context.Background()
	ws := newTestWorkspace(t)
	script := "printf 'HEAD'; for i in $(seq 1 500); do printf 'xxxxxxxxxx'; done; printf 'TAIL'"

	// When
	got, err := ws.RunCommand(ctx, CommandSpec{
		Name:         "sh",
		Args:         []string{"-c", script},
		Timeout:      10 * time.Second,
		MaxOutput:    16,
		ArchiveLimit: 200,
	})

	// Then
	if err != nil {
		t.Fatalf("run command: %v", err)
	}
	if !got.OutputTruncated {
		t.Fatalf("expected truncation, got %#v", got)
	}
	if len(got.Output) != 16 {
		t.Fatalf("visible output = %d bytes, want 16", len(got.Output))
	}
	if got.Archive == "" {
		t.Fatal("expected a retained archive")
	}
	if len(got.Archive) > 200+64 {
		t.Fatalf("archive = %d bytes, want <= archive limit plus the gap marker", len(got.Archive))
	}
	if !strings.HasPrefix(got.Archive, "HEAD") {
		t.Fatalf("archive lost the head window: %q", first(got.Archive, 40))
	}
	if !strings.HasSuffix(got.Archive, "TAIL") {
		t.Fatalf("archive lost the tail window: %q", last(got.Archive, 40))
	}
	if !strings.Contains(got.Archive, "bytes of output dropped") {
		t.Fatalf("archive is not contiguous but carries no gap marker: %q", got.Archive)
	}
}

func Test_Workspace_RunCommand_archive_is_contiguous_when_nothing_was_dropped(t *testing.T) {
	// Given: output that overflows the visible cap but fits inside the archive.
	ctx := context.Background()
	ws := newTestWorkspace(t)

	// When
	got, err := ws.RunCommand(ctx, CommandSpec{
		Name:         "printf",
		Args:         []string{"abcdefghij"},
		Timeout:      5 * time.Second,
		MaxOutput:    3,
		ArchiveLimit: 200,
	})

	// Then
	if err != nil {
		t.Fatalf("run command: %v", err)
	}
	if got.Output != "abc" {
		t.Fatalf("visible output = %q, want abc", got.Output)
	}
	if got.Archive != "abcdefghij" {
		t.Fatalf("archive = %q, want the whole output with no gap marker", got.Archive)
	}
}

func Test_Workspace_RunCommand_archives_nothing_when_output_fits(t *testing.T) {
	// Given
	ctx := context.Background()
	ws := newTestWorkspace(t)

	// When
	got, err := ws.RunCommand(ctx, CommandSpec{
		Name:         "printf",
		Args:         []string{"short"},
		Timeout:      5 * time.Second,
		MaxOutput:    64,
		ArchiveLimit: 200,
	})

	// Then: the visible output is already the whole story, so an archive would
	// only be a second copy of it.
	if err != nil {
		t.Fatalf("run command: %v", err)
	}
	if got.Archive != "" {
		t.Fatalf("archive = %q, want empty for untruncated output", got.Archive)
	}
}

func Test_Workspace_RunCommand_archive_disabled_keeps_truncation_visible(t *testing.T) {
	// Given: archiving off (ArchiveLimit 0).
	ctx := context.Background()
	ws := newTestWorkspace(t)

	// When
	got, err := ws.RunCommand(ctx, CommandSpec{
		Name:      "printf",
		Args:      []string{"abcdef"},
		Timeout:   5 * time.Second,
		MaxOutput: 3,
	})

	// Then: no archive, but the caller can still tell the output was cut.
	if err != nil {
		t.Fatalf("run command: %v", err)
	}
	if got.Archive != "" {
		t.Fatalf("archive = %q, want empty when archiving is off", got.Archive)
	}
	if !got.OutputTruncated {
		t.Fatal("truncation must stay reported even with archiving off")
	}
}

func first(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func last(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
