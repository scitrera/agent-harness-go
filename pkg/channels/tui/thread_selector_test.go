package tui

import (
	"context"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func TestModelHandleSlash_threadsOpensThreadSelector(t *testing.T) {
	// Given
	m := model{
		channel:  NewChannel(),
		threadID: "thread-beta",
		threads: []threadindex.Session{
			{ID: "thread-alpha", Title: "Alpha title"},
			{ID: "thread-beta", Title: "Beta title"},
		},
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(80, 12)

	// When
	next, cmd := m.handleSlash("/threads")
	if cmd != nil {
		t.Fatalf("/threads selector should not emit command")
	}
	updated, ok := next.(model)
	if !ok {
		t.Fatalf("expected model, got %T", next)
	}

	// Then
	if got := updated.selector.kind; got != selectionThread {
		t.Fatalf("selector kind = %v, want thread", got)
	}
	if got := len(updated.selector.items); got != 2 {
		t.Fatalf("thread selector count = %d, want 2", got)
	}
	item, ok := updated.selector.selectedItem()
	if !ok || item.Value != "thread-beta" {
		t.Fatalf("selected thread = %+v ok=%v, want thread-beta", item, ok)
	}
	if !strings.Contains(updated.renderSelection(), "Beta title") {
		t.Fatalf("thread selector should render titles: %q", updated.renderSelection())
	}
}

func TestModelHandleSlash_threadListUsesThreadSelector(t *testing.T) {
	// Given
	m := model{
		channel:  NewChannel(),
		threadID: "thread-beta",
		threads: []threadindex.Session{
			{ID: "thread-alpha", Title: "Alpha title"},
			{ID: "thread-beta", Title: "Beta title"},
		},
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(80, 12)

	// When
	next, cmd := m.handleSlash("/thread list")
	if cmd != nil {
		t.Fatalf("/thread list selector should not emit command")
	}
	updated, ok := next.(model)
	if !ok {
		t.Fatalf("expected model, got %T", next)
	}

	// Then
	if got := updated.selector.kind; got != selectionThread {
		t.Fatalf("selector kind = %v, want thread", got)
	}
	if updated.drawer != drawerNone {
		t.Fatalf("/thread list should not open old drawer: %v", updated.drawer)
	}
}

func TestModelUpdateKey_threadSelectorTabLoadsSelectedThread(t *testing.T) {
	// Given
	history := store.NewFileStore("", t.TempDir())
	m := model{
		ctx:      context.Background(),
		channel:  NewChannel(),
		store:    history,
		threadID: "thread-alpha",
		threads: []threadindex.Session{
			{ID: "thread-alpha", Title: "Alpha title"},
			{ID: "thread-beta", Title: "Beta title"},
		},
		viewport: viewport.New(),
		composer: newComposer(),
		tailing:  true,
	}
	m.resize(80, 12)
	m.openThreadSelector()
	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	if cmd != nil {
		t.Fatalf("thread selector down should not emit command")
	}
	updated, ok := next.(model)
	if !ok {
		t.Fatalf("expected model, got %T", next)
	}

	// When
	next, cmd = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	if cmd == nil {
		t.Fatalf("thread selector tab should load selected thread")
	}
	updated, ok = next.(model)
	if !ok {
		t.Fatalf("expected model, got %T", next)
	}
	msg := cmd()
	loaded, ok := msg.(historyLoadedMsg)
	if !ok {
		t.Fatalf("expected historyLoadedMsg, got %T", msg)
	}

	// Then
	if updated.threadID != "thread-beta" {
		t.Fatalf("threadID = %q, want thread-beta", updated.threadID)
	}
	if updated.selector.active() {
		t.Fatalf("selector should close after thread selection: %+v", updated.selector.items)
	}
	if loaded.ThreadID != "thread-beta" {
		t.Fatalf("loaded thread = %q, want thread-beta", loaded.ThreadID)
	}
}
