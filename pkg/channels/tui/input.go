package tui

import (
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func (m model) updateKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "enter":
		return m.sendCurrent()
	case "esc":
		m.drawer = drawerNone
		m.drawerContent = ""
		m.reflowSurfaces()
		m.refreshViewport()
		return m, nil
	case "pgup":
		m.viewport.PageUp()
		m.tailing = false
		return m, nil
	case "pgdown":
		m.viewport.PageDown()
		m.tailing = m.viewport.AtBottom()
		return m, nil
	case "ctrl+u":
		m.viewport.HalfPageUp()
		m.tailing = false
		return m, nil
	case "ctrl+d":
		m.viewport.HalfPageDown()
		m.tailing = m.viewport.AtBottom()
		return m, nil
	case "home":
		m.viewport.GotoTop()
		m.tailing = false
		return m, nil
	case "end":
		m.viewport.GotoBottom()
		m.tailing = true
		return m, nil
	}
	var cmd tea.Cmd
	m.composer, cmd = m.composer.Update(msg)
	return m, cmd
}

func (m model) sendCurrent() (tea.Model, tea.Cmd) {
	text := strings.TrimSpace(m.composer.Value())
	if text == "" {
		return m, nil
	}
	m.composer.Reset()
	if strings.HasPrefix(text, "/") {
		return m.handleSlash(text)
	}
	taskID, err := threadindex.NewID("task-")
	if err != nil {
		m.addSystem("could not create task id: " + err.Error())
		return m, nil
	}
	part, err := protocol.NewTextPart(text)
	if err != nil {
		m.addSystem("could not create message: " + err.Error())
		return m, nil
	}
	addr := protocol.MessageAddress{ThreadID: m.threadID, TaskID: taskID}
	message := protocol.ChatMessage{ID: "user-" + taskID, Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{part}}
	m.lastTaskID = taskID
	m.rows = append(m.rows, chatRow{Kind: rowUser, ID: message.ID, TaskID: taskID, Text: text})
	m.status = "sent " + taskID
	m.refreshViewportToBottom()
	return m, sendMessageCmd(m.ctx, m.channel, m.index, addr, message, text)
}
