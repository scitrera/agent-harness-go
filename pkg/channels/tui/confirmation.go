package tui

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
)

func (m *model) requestConfirmation(kind confirmationKind, threadID string) {
	action := "clear"
	if kind == confirmationDeleteThread {
		action = "delete"
	}
	m.confirmation = pendingConfirmation{Kind: kind, WorkspaceID: m.workspaceID, ThreadID: threadID}
	m.selector.clear()
	m.showDrawer(
		drawerConfirmation,
		fmt.Sprintf("%s thread %s?\nPress y to confirm or n/Esc to cancel.", action, shortID(threadID)),
	)
	m.status = "confirm " + action
}

func (m *model) cancelConfirmation() {
	m.confirmation = pendingConfirmation{}
	m.drawer = drawerNone
	m.drawerContent = ""
	m.status = "cancelled"
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m model) confirmPending() (tea.Model, tea.Cmd) {
	pending := m.confirmation
	m.confirmation = pendingConfirmation{}
	m.drawer = drawerNone
	m.drawerContent = ""
	m.reflowSurfaces()
	m.refreshViewport()
	switch pending.Kind {
	case confirmationClearThread:
		if m.clearingThreads == nil {
			m.clearingThreads = map[string]struct{}{}
		}
		m.clearingThreads[workspaceKey(pending.WorkspaceID, pending.ThreadID)] = struct{}{}
		m.status = "clearing " + shortID(pending.ThreadID)
		return m, clearThreadCmd(m.ctx, m.store, m.initialWorkspaceID, pending.WorkspaceID, pending.ThreadID)
	case confirmationDeleteThread:
		return m, deleteThreadCmd(m.ctx, m.index, m.store, m.initialWorkspaceID, pending.WorkspaceID, pending.ThreadID, m.nextThreadAfterDelete(pending.ThreadID))
	default:
		return m, nil
	}
}
