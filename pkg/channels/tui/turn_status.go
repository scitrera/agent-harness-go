package tui

import (
	"fmt"
	"strings"
)

func (m *model) markTurn(taskID, threadID, phase string) {
	if taskID == "" {
		m.status = phase
		return
	}
	if m.turns == nil {
		m.turns = map[string]turnActivity{}
	}
	m.turns[taskID] = turnActivity{ThreadID: threadID, Phase: phase}
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
	priorities := []string{"approval needed", "tool failed", "tool", "responding", "thinking", "queued"}
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
	return activity.ThreadID == "" || activity.ThreadID == m.threadID
}
