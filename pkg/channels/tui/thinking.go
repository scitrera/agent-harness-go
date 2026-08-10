package tui

import (
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
)

const thinkingInterval = 350 * time.Millisecond

const thinkingCancelHint = " (Press Esc to Cancel)"

var thinkingFrames = [...]string{
	"Thinking ·" + thinkingCancelHint,
	"Thinking ··" + thinkingCancelHint,
	"Thinking ···" + thinkingCancelHint,
	"Thinking" + thinkingCancelHint,
}

func thinkingRowID(taskID string) string {
	return "thinking:" + taskID
}

func (m *model) addThinking(taskID string) {
	if taskID == "" {
		return
	}
	m.rows = append(m.rows, chatRow{
		Kind:   rowThinking,
		ID:     thinkingRowID(taskID),
		TaskID: taskID,
		Text:   thinkingFrames[m.thinkingFrame%len(thinkingFrames)],
	})
}

func (m *model) removeThinking(taskID string) {
	for i := 0; i < len(m.rows); {
		row := m.rows[i]
		matches := row.Kind == rowThinking && (taskID == "" || row.TaskID == taskID)
		if !matches {
			i++
			continue
		}
		m.rows = append(m.rows[:i], m.rows[i+1:]...)
	}
}

func (m model) hasThinking() bool {
	for _, row := range m.rows {
		if row.Kind == rowThinking {
			return true
		}
	}
	for _, activity := range m.subagents {
		if activity.Phase == "working" {
			return true
		}
	}
	return false
}

func (m *model) ensureThinkingTick(cmd tea.Cmd) tea.Cmd {
	if m.thinkingTicking || !m.hasThinking() {
		return cmd
	}
	m.thinkingTicking = true
	return tea.Batch(cmd, thinkingTickCmd())
}

func (m *model) advanceThinking() bool {
	if !m.hasThinking() {
		return false
	}
	m.thinkingFrame = (m.thinkingFrame + 1) % len(thinkingFrames)
	text := thinkingFrames[m.thinkingFrame]
	for i := range m.rows {
		if m.rows[i].Kind == rowThinking {
			m.rows[i].Text = text
		}
	}
	for callID, activity := range m.subagents {
		if activity.Phase == "working" {
			m.upsertToolishRow(callID, m.renderSubagentActivity(activity))
		}
	}
	m.refreshViewport()
	return true
}

func thinkingTickCmd() tea.Cmd {
	return tea.Tick(thinkingInterval, func(time.Time) tea.Msg {
		return thinkingTickMsg{}
	})
}

func rowStructureKey(rows []chatRow) string {
	var key strings.Builder
	for _, row := range rows {
		key.WriteString(string(row.Kind))
		key.WriteByte(':')
		key.WriteString(row.ID)
		key.WriteByte('\x00')
	}
	return key.String()
}
