package tui

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func (m model) handleThread(fields []string) (tea.Model, tea.Cmd) {
	if shouldOpenThreadSelector(fields) {
		m.openThreadSelector()
		return m, nil
	}
	switch fields[1] {
	case "current":
		m.showDrawer(drawerThreads, m.currentThreadSummary())
		return m, nil
	case "new", "create":
		title := strings.Join(fields[2:], " ")
		m.status = "creating thread"
		return m, createThreadCmd(m.index, title)
	case "switch", "use", "open":
		return m.switchThread(fields)
	case "delete", "rm":
		return m.deleteThread(fields)
	case "clear":
		return m.clearThread(fields)
	case "rename", "title":
		return m.renameThread(fields)
	}
	m.addSystem("unknown /thread command")
	return m, nil
}

func shouldOpenThreadSelector(fields []string) bool {
	if len(fields) == 0 {
		return false
	}
	if fields[0] != "/thread" && fields[0] != "/threads" {
		return false
	}
	return len(fields) < 2 || fields[1] == "list" || fields[1] == "ls"
}

func (m *model) openThreadSelector() {
	if len(m.threads) == 0 && m.index != nil {
		m.threads = m.index.List()
	}
	items := threadSelectionItems(m.threads, m.threadID)
	if len(items) == 0 {
		m.addSystem("no threads")
		return
	}
	m.drawer = drawerNone
	m.drawerContent = ""
	m.selector = newSelection(selectionThread, items, m.threadID, maxSelectionRows)
	m.status = "select thread"
	m.reflowSurfaces()
	m.refreshViewport()
}

func threadSelectionItems(threads []threadindex.Session, currentID string) []selectionItem {
	items := make([]selectionItem, 0, len(threads))
	for _, session := range threads {
		description := strings.TrimSpace(session.Title)
		if session.ID == currentID {
			if description == "" {
				description = "current"
			} else {
				description = "current  " + description
			}
		}
		items = append(items, selectionItem{Value: session.ID, Label: shortID(session.ID), Description: description})
	}
	return items
}

func (m model) selectThread(id string) (tea.Model, tea.Cmd) {
	m.selector.clear()
	m.reflowSurfaces()
	if id == m.threadID {
		m.status = "thread " + shortID(id)
		m.refreshViewport()
		return m, nil
	}
	m.threadID = id
	m.status = "loading " + id
	m.tailing = true
	m.refreshViewport()
	return m, loadHistoryCmd(m.ctx, m.store, id)
}

func (m model) switchThread(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 {
		m.addSystem("usage: /thread switch <id-or-prefix>")
		return m, nil
	}
	id, ok := m.matchThread(fields[2])
	if !ok {
		m.addSystem("thread not found: " + fields[2])
		return m, nil
	}
	m.threadID = id
	m.status = "loading " + id
	m.tailing = true
	return m, loadHistoryCmd(m.ctx, m.store, id)
}

func (m model) deleteThread(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 {
		m.addSystem("usage: /thread delete <id-or-prefix>")
		return m, nil
	}
	id, ok := m.matchThread(fields[2])
	if !ok {
		m.addSystem("thread not found: " + fields[2])
		return m, nil
	}
	nextID := m.nextThreadAfterDelete(id)
	return m, deleteThreadCmd(m.ctx, m.index, m.store, id, nextID)
}

func (m model) clearThread(fields []string) (tea.Model, tea.Cmd) {
	id := m.threadID
	if len(fields) > 2 {
		matched, ok := m.matchThread(fields[2])
		if !ok {
			m.addSystem("thread not found: " + fields[2])
			return m, nil
		}
		id = matched
	}
	if id == "" {
		m.addSystem("no thread selected")
		return m, nil
	}
	return m, clearThreadCmd(m.ctx, m.store, id)
}

func (m model) renameThread(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 {
		m.addSystem("usage: /thread rename [id-or-prefix] <title>")
		return m, nil
	}
	id := m.threadID
	titleStart := 2
	if len(fields) > 3 {
		if matched, ok := m.matchThread(fields[2]); ok {
			id = matched
			titleStart = 3
		}
	}
	title := strings.TrimSpace(strings.Join(fields[titleStart:], " "))
	if title == "" {
		m.addSystem("usage: /thread rename [id-or-prefix] <title>")
		return m, nil
	}
	return m, renameThreadCmd(m.index, id, title)
}

func loadHistoryCmd(ctx context.Context, store HistoryStore, threadID string) tea.Cmd {
	return func() tea.Msg {
		messages, err := store.LoadHistory(ctx, threadID)
		return historyLoadedMsg{ThreadID: threadID, Messages: messages, Err: err}
	}
}

func createThreadCmd(index *threadindex.Index, title string) tea.Cmd {
	return func() tea.Msg {
		session, err := index.Create()
		if err != nil {
			return threadCreatedMsg{Session: session, Err: err}
		}
		if strings.TrimSpace(title) != "" {
			if err := index.Rename(session.ID, title); err != nil {
				return threadCreatedMsg{Session: session, Err: fmt.Errorf("rename thread: %w", err)}
			}
			for _, current := range index.List() {
				if current.ID == session.ID {
					session = current
					break
				}
			}
		}
		return threadCreatedMsg{Session: session}
	}
}

func deleteThreadCmd(ctx context.Context, index *threadindex.Index, store HistoryStore, id string, nextID string) tea.Cmd {
	return func() tea.Msg {
		if err := index.Delete(id); err != nil {
			return threadDeletedMsg{DeletedID: id, NextID: nextID, Err: fmt.Errorf("delete index: %w", err)}
		}
		if err := store.DeleteHistory(ctx, id); err != nil {
			return threadDeletedMsg{DeletedID: id, NextID: nextID, Err: fmt.Errorf("delete history: %w", err)}
		}
		return threadDeletedMsg{DeletedID: id, NextID: nextID}
	}
}

func clearThreadCmd(ctx context.Context, store HistoryStore, id string) tea.Cmd {
	return func() tea.Msg {
		err := store.DeleteHistory(ctx, id)
		return clearThreadMsg{ThreadID: id, Err: err}
	}
}

func renameThreadCmd(index *threadindex.Index, id string, title string) tea.Cmd {
	return func() tea.Msg {
		err := index.Rename(id, title)
		return threadRenamedMsg{ThreadID: id, Err: err}
	}
}
