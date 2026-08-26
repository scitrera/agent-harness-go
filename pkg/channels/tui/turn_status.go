package tui

import (
	"fmt"
	"strings"
)

func (m *model) markTurn(taskID, threadID, phase string) {
	m.markTurnAt(taskID, m.workspaceID, threadID, phase)
}

func (m *model) markTurnAt(taskID, workspaceID, threadID, phase string) {
	if taskID == "" {
		m.status = phase
		return
	}
	if m.turns == nil {
		m.turns = map[string]turnActivity{}
	}
	if workspaceID == "" {
		if current, ok := m.turns[taskID]; ok {
			workspaceID = current.WorkspaceID
		}
	}
	m.turns[taskID] = turnActivity{WorkspaceID: workspaceID, ThreadID: threadID, Phase: phase}
	m.status = m.activeTurnStatus()
}

func (m *model) finishTurn(taskID string) {
	if taskID != "" {
		delete(m.turns, taskID)
		if m.cancelPendingID == taskID {
			m.cancelPendingID = ""
		}
	}
	m.status = m.activeTurnStatus()
}

func (m model) activeTurnStatus() string {
	pending := len(m.currentPendingIndexes())
	if len(m.turns) == 0 && pending == 0 {
		return "ready"
	}
	priorities := []string{"approval needed", "tool failed", "subagent working", "tool", "responding", "reasoning", "thinking", "saving context", "queued"}
	for _, priority := range priorities {
		for _, activity := range m.turns {
			if !m.activityOnCurrentThread(activity) {
				continue
			}
			matches := activity.Phase == priority
			if priority == "tool" {
				matches = strings.HasPrefix(activity.Phase, "tool ")
			}
			if !matches {
				continue
			}
			status := activity.Phase
			if pending > 0 {
				status += fmt.Sprintf(" (+%d queued)", pending)
			}
			return status
		}
	}
	if pending > 0 {
		return fmt.Sprintf("queued %d", pending)
	}
	return "ready"
}

func (m model) activityOnCurrentThread(activity turnActivity) bool {
	workspaceMatches := m.workspaceID == "" || activity.WorkspaceID == "" || activity.WorkspaceID == m.workspaceID
	return workspaceMatches && (activity.ThreadID == "" || activity.ThreadID == m.threadID)
}
