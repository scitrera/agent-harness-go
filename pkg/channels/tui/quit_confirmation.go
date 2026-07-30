package tui

import (
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
)

const quitConfirmationWindow = time.Second

type pendingQuitConfirmation struct {
	Key         string
	Until       time.Time
	Token       uint64
	PriorStatus string
	Prompt      string
}

func (q pendingQuitConfirmation) active(now time.Time) bool {
	return q.Key != "" && !now.After(q.Until)
}

func (m model) handleQuitKey(key string, now time.Time) (tea.Model, tea.Cmd) {
	if m.quitConfirmation.Key == key && m.quitConfirmation.active(now) {
		if m.status == m.quitConfirmation.Prompt {
			m.status = m.quitConfirmation.PriorStatus
		}
		m.quitConfirmation = pendingQuitConfirmation{}
		return m, tea.Quit
	}
	m.quitToken++
	priorStatus := m.status
	if m.quitConfirmation.Key != "" {
		priorStatus = m.quitConfirmation.PriorStatus
	}
	label := strings.ToUpper(strings.TrimPrefix(key, "ctrl+"))
	prompt := fmt.Sprintf("press Ctrl+%s again within 1s to quit", label)
	m.quitConfirmation = pendingQuitConfirmation{
		Key:         key,
		Until:       now.Add(quitConfirmationWindow),
		Token:       m.quitToken,
		PriorStatus: priorStatus,
		Prompt:      prompt,
	}
	m.status = prompt
	m.refreshViewport()
	return m, quitConfirmationTimeoutCmd(m.quitToken)
}

func (m *model) cancelQuitConfirmation() {
	if m.quitConfirmation.Key == "" {
		return
	}
	if m.status == m.quitConfirmation.Prompt {
		m.status = m.quitConfirmation.PriorStatus
	}
	m.quitConfirmation = pendingQuitConfirmation{}
}

func (m *model) expireQuitConfirmation(msg quitConfirmationExpiredMsg) {
	if m.quitConfirmation.Key == "" || msg.Token != m.quitConfirmation.Token {
		return
	}
	m.cancelQuitConfirmation()
	m.refreshViewport()
}

func quitConfirmationTimeoutCmd(token uint64) tea.Cmd {
	return tea.Tick(quitConfirmationWindow, func(time.Time) tea.Msg {
		return quitConfirmationExpiredMsg{Token: token}
	})
}
