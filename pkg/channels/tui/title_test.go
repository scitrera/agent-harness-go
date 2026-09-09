// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func TestSanitizeTerminalTitle_stripsControlSequences(t *testing.T) {
	// Given: a title containing terminal control characters.
	input := "Hello\x1b]2;bad\x07\nWorld"

	// When: it is prepared for OSC title output.
	got := sanitizeTerminalTitle(input)

	// Then: control characters are removed, printable text remains.
	if got != "Hello]2;badWorld" {
		t.Fatalf("title = %q", got)
	}
}

func TestModelTerminalTitle_usesActiveThreadTitle(t *testing.T) {
	// Given: a selected thread with a display title.
	m := model{threadID: "thread-1", threads: []threadindex.Session{{ID: "thread-1", Title: "Design review"}}}

	// When / Then.
	if got := m.terminalTitle(); got != "Project Sahara - Design review" {
		t.Fatalf("terminal title = %q", got)
	}
}
