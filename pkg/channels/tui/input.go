package tui

import (
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const historyScrollLines = 3

func (m model) updateKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "ctrl+d":
		return m.handleQuitKey(msg.String(), time.Now())
	default:
		m.cancelQuitConfirmation()
	}
	if m.confirmation.active() {
		switch msg.String() {
		case "y", "Y":
			return m.confirmPending()
		case "n", "N", "esc":
			m.cancelConfirmation()
			return m, nil
		default:
			return m, nil
		}
	}
	if msg.String() == "f1" {
		if m.drawer == drawerHelp {
			m.drawer = drawerNone
			m.drawerContent = ""
			m.status = m.activeTurnStatus()
			m.reflowSurfaces()
			m.refreshViewport()
			return m, nil
		}
		m.openHelp()
		return m, nil
	}
	if m.selector.active() {
		if m.selector.filtering {
			switch msg.String() {
			case "up":
				m.selector.move(-1)
				m.refreshSelectionSurface()
				return m, nil
			case "down":
				m.selector.move(1)
				m.refreshSelectionSurface()
				return m, nil
			case "backspace", "ctrl+h":
				m.selector.backspaceFilter()
				m.refreshSelectionSurface()
				return m, nil
			case "ctrl+u":
				m.selector.query = ""
				m.selector.applyFilter("")
				m.refreshSelectionSurface()
				return m, nil
			case "esc":
				m.selector.clearFilter()
				m.status = "select " + m.selector.kind.String()
				m.refreshSelectionSurface()
				return m, nil
			case "enter":
				m.selector.filtering = false
				m.reflowSurfaces()
			default:
				if text := msg.Key().Text; text != "" {
					m.selector.appendFilter(text)
					m.refreshSelectionSurface()
				}
				return m, nil
			}
		}
		switch msg.String() {
		case "tab":
			if m.selector.kind == selectionApproval || m.selector.kind == selectionTool {
				return m, nil
			}
			return m.completeSelection()
		case "up":
			m.selector.move(-1)
			return m, nil
		case "down":
			m.selector.move(1)
			return m, nil
		case "/":
			if m.selector.searchable() {
				m.selector.beginFilter()
				m.status = "filter " + m.selector.kind.String()
				m.refreshSelectionSurface()
				return m, nil
			}
		case "esc":
			m.selector.clear()
			if m.drawer == drawerApprovals || m.drawer == drawerTools {
				m.drawer = drawerNone
				m.drawerContent = ""
			}
			m.reflowSurfaces()
			m.refreshViewport()
			return m, nil
		case "a":
			if m.selector.kind == selectionApproval {
				return m.resolveSelectedApproval(true, "once")
			}
		case "s":
			if m.selector.kind == selectionApproval {
				return m.resolveSelectedApproval(true, "session")
			}
		case "A", "shift+a":
			if m.selector.kind == selectionApproval {
				return m.resolveSelectedApproval(true, "always")
			}
		case "d":
			if m.selector.kind == selectionApproval {
				return m.resolveSelectedApproval(false, "once")
			}
		case "enter":
			if m.selector.kind == selectionPath {
				if m.pathInputHasExactTarget() {
					m.selector.clear()
					return m.sendCurrent()
				}
				return m.completeSelection()
			}
			if m.selector.kind == selectionThread {
				return m.completeSelection()
			}
			if m.selector.kind == selectionTool {
				return m.completeSelection()
			}
			if m.selector.kind == selectionSlash {
				value := m.composer.Value()
				if strings.HasSuffix(value, " ") || strings.HasSuffix(value, "\t") {
					return m.sendCurrent()
				}
				if item, ok := m.selector.selectedItem(); ok && strings.TrimSpace(value) != item.Value {
					return m.completeSelection()
				}
			}
		}
		if m.selector.kind != selectionSlash && m.selector.kind != selectionPath {
			return m, nil
		}
	}
	switch msg.String() {
	case "enter":
		return m.sendCurrent()
	case "esc":
		if m.canCancelActive() {
			m.cancelActive()
			return m, nil
		}
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
	case "ctrl+pgup":
		m.viewport.GotoTop()
		m.tailing = false
		return m, nil
	case "ctrl+pgdown":
		m.viewport.GotoBottom()
		m.tailing = true
		return m, nil
	case "up":
		if strings.TrimSpace(m.composer.Value()) == "" {
			m.viewport.ScrollUp(historyScrollLines)
			m.tailing = false
			return m, nil
		}
	case "down":
		if strings.TrimSpace(m.composer.Value()) == "" {
			m.viewport.ScrollDown(historyScrollLines)
			m.tailing = m.viewport.AtBottom()
			return m, nil
		}
	}
	var cmd tea.Cmd
	m.composer, cmd = m.composer.Update(msg)
	m.refreshInputSurface()
	return m, cmd
}

func (m *model) refreshSelectionSurface() {
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m model) sendCurrent() (tea.Model, tea.Cmd) {
	text := strings.TrimSpace(m.composer.Value())
	if text == "" && len(m.attachments) == 0 {
		return m, nil
	}
	if _, clearing := m.clearingThreads[m.threadID]; clearing {
		m.deferredSendFor = m.threadID
		m.status = "waiting for clear"
		m.refreshViewport()
		return m, nil
	}
	if strings.HasPrefix(text, "/") {
		m.composer.Reset()
		m.selector.clear()
		m.refreshInputSurface()
		return m.handleSlash(text)
	}
	resolvedText, referencedImages, err := m.resolveAtReferences(text)
	if err != nil {
		m.addSystem("reference failed: " + err.Error())
		return m, nil
	}
	text = resolvedText
	displayText := text
	text = m.withWorkingDirectoryContext(text)
	m.composer.Reset()
	m.selector.clear()
	m.refreshInputSurface()
	taskID, err := threadindex.NewID("task-")
	if err != nil {
		m.addSystem("could not create task id: " + err.Error())
		return m, nil
	}
	content, err := m.takeMessageContent(text)
	if err != nil {
		m.addSystem("could not create message: " + err.Error())
		return m, nil
	}
	addr := protocol.MessageAddress{ThreadID: m.threadID, TaskID: taskID}
	message := protocol.ChatMessage{ID: "user-" + taskID, Role: protocol.RoleUser, Addr: addr, Content: content}
	if cwd := m.currentWorkingDirectory(); cwd != "" && cwd != m.workspaceRoot {
		tools.StampWorkingDirectory(&message, cwd)
	}
	m.lastTaskID = taskID
	m.markTurn(taskID, m.threadID, "queued")
	m.rows = append(m.rows, chatRow{Kind: rowUser, ID: message.ID, TaskID: taskID, Text: messageText(message)})
	m.addThinking(taskID)
	m.refreshViewportToBottom()
	return m, sendMessageCmd(m.ctx, m.channel, m.index, addr, message, displayText, referencedImages)
}

func (m *model) takeMessageContent(text string) ([]protocol.ContentPart, error) {
	content := make([]protocol.ContentPart, 0, 1+len(m.attachments))
	if text != "" {
		part, err := protocol.NewTextPart(text)
		if err != nil {
			return nil, err
		}
		content = append(content, part)
	}
	for _, attachment := range m.attachments {
		content = append(content, attachment.Part)
	}
	m.attachments = nil
	m.updateAttachmentPlaceholder()
	return content, nil
}

func (m model) completeSelection() (tea.Model, tea.Cmd) {
	item, ok := m.selector.selectedItem()
	if !ok {
		m.selector.clear()
		m.reflowSurfaces()
		m.refreshViewport()
		return m, nil
	}
	switch m.selector.kind {
	case selectionSlash:
		m.composer.SetValue(item.Value + " ")
		m.selector.clear()
		m.refreshInputSurface()
		return m, nil
	case selectionPath:
		m.composer.SetValue(item.Value)
		m.selector.clear()
		m.refreshInputSurface()
		return m, nil
	case selectionThread:
		return m.selectThread(item.Value)
	case selectionTool:
		m.selector.clear()
		m.showToolDetail(item.Value)
		return m, nil
	default:
		m.selector.clear()
		m.reflowSurfaces()
		m.refreshViewport()
		return m, nil
	}
}
