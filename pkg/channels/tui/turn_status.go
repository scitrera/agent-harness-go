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
	}
	m.status = m.activeTurnStatus()
}

func (m model) activeTurnStatus() string {
	if len(m.turns) == 0 {
		return "ready"
	}
	priorities := []string{"approval needed", "tool failed", "subagent working", "tool", "responding", "thinking", "queued"}
	queued := 0
	for _, activity := range m.turns {
		if !m.activityOnCurrentThread(activity) {
			continue
		}
		if activity.Phase == "queued" {
			queued++
		}
	}
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
			if queued > 0 && priority != "queued" {
				status += fmt.Sprintf(" (+%d queued)", queued)
			} else if priority == "queued" && queued > 1 {
				status = fmt.Sprintf("queued %d", queued)
			}
			return status
		}
	}
	return "ready"
}

func (m model) activityOnCurrentThread(activity turnActivity) bool {
	workspaceMatches := m.workspaceID == "" || activity.WorkspaceID == "" || activity.WorkspaceID == m.workspaceID
	return workspaceMatches && (activity.ThreadID == "" || activity.ThreadID == m.threadID)
}
