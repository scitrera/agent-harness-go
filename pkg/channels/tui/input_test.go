// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
)

func TestComposerStartsAtOneLineAndExpandsToThree(t *testing.T) {
	// Given
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(20, 10)

	// Then
	if got := m.composer.Height(); got != minComposerHeight {
		t.Fatalf("initial composer height = %d, want %d", got, minComposerHeight)
	}
	if got, want := m.viewport.Height(), 10-minComposerHeight-statusHeight; got != want {
		t.Fatalf("initial viewport height = %d, want %d", got, want)
	}

	// When
	m.composer.SetValue(strings.Repeat("x", 80))
	m.refreshInputSurface()

	// Then
	if got := m.composer.Height(); got != maxComposerHeight {
		t.Fatalf("expanded composer height = %d, want %d", got, maxComposerHeight)
	}
	if got, want := m.viewport.Height(), 10-maxComposerHeight-statusHeight; got != want {
		t.Fatalf("expanded viewport height = %d, want %d", got, want)
	}
}

func TestSlashSuggestionsMatchPrefixAndCompleteWithTab(t *testing.T) {
	// Given
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(80, 12)
	m.composer.SetValue("/th")
	m.refreshInputSurface()

	// Then
	if got := m.selector.kind; got != selectionSlash {
		t.Fatalf("selector kind = %v, want slash", got)
	}
	if got := len(m.selector.items); got != 2 {
		t.Fatalf("suggestion count = %d, want 2: %+v", got, m.selector.items)
	}
	if got := m.selector.items[0].Value; got != "/thread" {
		t.Fatalf("first suggestion = %q, want /thread", got)
	}

	// When
	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	if cmd != nil {
		t.Fatalf("tab completion should not emit command")
	}
	updated, ok := next.(model)
	if !ok {
		t.Fatalf("updated model has type %T", next)
	}

	// Then
	if got := updated.composer.Value(); got != "/thread " {
		t.Fatalf("completed composer value = %q, want /thread ", got)
	}
	if !updated.selector.active() {
		t.Fatal("subcommand suggestions should open after command completion")
	}
	if got := updated.selector.items[0].Value; got != "/thread clear" {
		t.Fatalf("first subcommand suggestion = %q, want /thread clear", got)
	}
}

func TestSlashSuggestionsUseArrowSelection(t *testing.T) {
	// Given
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(80, 12)
	m.composer.SetValue("/t")
	m.refreshInputSurface()
	if len(m.selector.items) < 2 {
		t.Fatalf("test setup needs multiple suggestions: %+v", m.selector.items)
	}

	// When
	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	if cmd != nil {
		t.Fatalf("down selection should not emit command")
	}
	updated, ok := next.(model)
	if !ok {
		t.Fatalf("updated model has type %T", next)
	}

	// Then
	if got := updated.selector.selected; got != 1 {
		t.Fatalf("selected suggestion = %d, want 1", got)
	}

	// When
	next, cmd = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	if cmd != nil {
		t.Fatalf("up selection should not emit command")
	}
	updated, ok = next.(model)
	if !ok {
		t.Fatalf("updated model has type %T", next)
	}

	// Then
	if got := updated.selector.selected; got != 0 {
		t.Fatalf("selected suggestion after up = %d, want 0", got)
	}
}

func TestViewCursorAccountsForSlashSuggestions(t *testing.T) {
	// Given
	m := model{channel: NewChannel(), viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.resize(80, 12)
	m.composer.SetValue("/t")
	m.refreshInputSurface()

	// When
	view := m.View()

	// Then
	if view.Cursor == nil {
		t.Fatal("composer cursor should be exported")
	}
	if got, want := view.Cursor.Y, m.viewport.Height()+m.selector.height(); got != want {
		t.Fatalf("cursor row = %d, want %d", got, want)
	}
}

func TestEmptyComposerArrowEventsScrollHistoryByLines(t *testing.T) {
	m := model{viewport: viewport.New(), composer: newComposer(), tailing: true}
	m.viewport.SetWidth(40)
	m.viewport.SetHeight(3)
	m.viewport.SetContent(strings.Join([]string{
		"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
	}, "\n"))
	m.viewport.GotoBottom()
	bottom := m.viewport.YOffset()

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	updated := next.(model)
	if got := updated.viewport.YOffset(); got != bottom-historyScrollLines {
		t.Fatalf("up scroll offset = %d, want %d", got, bottom-historyScrollLines)
	}
	if updated.tailing {
		t.Fatal("scrolling up should stop tailing")
	}

	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	updated = next.(model)
	if got := updated.viewport.YOffset(); got != bottom {
		t.Fatalf("down scroll offset = %d, want bottom %d", got, bottom)
	}
	if !updated.tailing {
		t.Fatal("scrolling to bottom should resume tailing")
	}
}

func TestMouseWheelScrollsTranscriptWithoutRecallingComposerHistory(t *testing.T) {
	m := model{
		viewport:     viewport.New(),
		composer:     newComposer(),
		tailing:      true,
		inputHistory: []string{"previous message"},
	}
	m.viewport.SetWidth(40)
	m.viewport.SetHeight(3)
	m.viewport.SetContent(strings.Join([]string{
		"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
	}, "\n"))
	m.viewport.GotoBottom()
	bottom := m.viewport.YOffset()

	next, _ := m.Update(tea.MouseWheelMsg(tea.Mouse{Button: tea.MouseWheelUp}))
	updated := next.(model)
	if got := updated.viewport.YOffset(); got != bottom-updated.viewport.MouseWheelDelta {
		t.Fatalf("wheel scroll offset = %d, want %d", got, bottom-updated.viewport.MouseWheelDelta)
	}
	if got := updated.composer.Value(); got != "" {
		t.Fatalf("mouse wheel recalled composer history: %q", got)
	}
	if updated.historyIndex != 0 {
		t.Fatalf("mouse wheel changed composer history index to %d", updated.historyIndex)
	}
}
