// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"fmt"
	"testing"

	"charm.land/bubbles/v2/viewport"
)

func TestModelRefreshViewport_preservesScrollbackWhenNotTailing(t *testing.T) {
	// Given: a viewport at the bottom of a long transcript.
	m := model{viewport: viewport.New(), tailing: true}
	m.viewport.SetWidth(80)
	m.viewport.SetHeight(4)
	for i := 0; i < 20; i++ {
		m.rows = append(m.rows, chatRow{Kind: rowAssistant, Text: fmt.Sprintf("row %02d", i)})
	}
	m.refreshViewportToBottom()
	if !m.viewport.AtBottom() {
		t.Fatal("viewport should start at bottom")
	}

	// When: the user scrolls up and a new row arrives.
	m.viewport.PageUp()
	m.tailing = false
	offset := m.viewport.YOffset()
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, Text: "new row"})
	m.refreshViewport()

	// Then: refresh does not snap back to the bottom.
	if got := m.viewport.YOffset(); got != offset {
		t.Fatalf("offset changed: got %d want %d", got, offset)
	}
	if m.tailing {
		t.Fatal("tailing should stay disabled after scrollback refresh")
	}
}

func TestModelRefreshViewport_tailsWhenAtBottom(t *testing.T) {
	// Given: a viewport tailing a long transcript.
	m := model{viewport: viewport.New(), tailing: true}
	m.viewport.SetWidth(80)
	m.viewport.SetHeight(4)
	for i := 0; i < 20; i++ {
		m.rows = append(m.rows, chatRow{Kind: rowAssistant, Text: fmt.Sprintf("row %02d", i)})
	}
	m.refreshViewportToBottom()

	// When: a new row arrives while tailing.
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, Text: "new row"})
	m.refreshViewport()

	// Then: the viewport stays at the live bottom.
	if !m.viewport.AtBottom() {
		t.Fatal("viewport should stay at bottom")
	}
}
