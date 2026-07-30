package tui

import (
	"context"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/scitrera/agent-harness-go/pkg/channel"
)

const (
	statusHeight          = 1
	defaultTerminalWidth  = 80
	defaultTerminalHeight = 24
)

func (m *model) resize(width, height int) {
	if width <= 0 || height <= 0 {
		return
	}
	m.width = width
	m.height = height
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m *model) reflowSurfaces() {
	if m.width <= 0 || m.height <= 0 {
		return
	}
	drawerHeight := m.drawerHeight()
	composerHeight := 0
	if m.composer.Prompt == "" && m.composer.Placeholder == "" {
		composerHeight = minComposerHeight
	} else {
		m.composer.SetWidth(m.width)
		composerHeight = m.composer.Height()
	}
	selectionSpace := m.height - composerHeight - statusHeight - drawerHeight - 1
	m.selector.limitHeight(selectionSpace)
	viewHeight := m.height - composerHeight - statusHeight - drawerHeight - m.selector.height()
	if viewHeight < 1 {
		viewHeight = 1
	}
	m.viewport.SetWidth(m.width)
	m.viewport.SetHeight(viewHeight)
	m.viewport.MouseWheelEnabled = true
}

func (m *model) drawerHeight() int {
	if m.drawer == drawerNone {
		return 0
	}
	return lipgloss.Height(m.renderDrawer())
}

func (m *model) drawerMaxHeight() int {
	if m.drawer == drawerNone {
		return 0
	}
	if m.height <= 10 {
		return 3
	}
	return 7
}

func (m *model) addSystem(text string) {
	m.rows = append(m.rows, chatRow{Kind: rowSystem, Text: text})
	m.status = firstLine(text)
	m.refreshViewport()
}

func (m *model) showDrawer(mode drawerMode, text string) {
	m.drawer = mode
	m.drawerContent = text
	m.status = firstLine(text)
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m *model) refreshViewport() {
	m.refreshViewportWithTail(m.tailing || m.viewport.AtBottom())
}

func (m *model) refreshViewportToBottom() {
	m.refreshViewportWithTail(true)
}

func (m *model) refreshViewportWithTail(tail bool) {
	if m.viewport.Height() <= 0 {
		if m.width <= 0 {
			m.width = defaultTerminalWidth
		}
		if m.height <= 0 {
			m.height = defaultTerminalHeight
		}
		m.reflowSurfaces()
	}
	offset := m.viewport.YOffset()
	m.viewport.SetContent(m.renderRows())
	if tail {
		m.viewport.GotoBottom()
		m.tailing = true
		return
	}
	m.viewport.SetYOffset(offset)
	m.tailing = false
}

func firstLine(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "ready"
	}
	line, _, _ := strings.Cut(text, "\n")
	return line
}

func waitEvent(ctx context.Context, events <-chan channel.Event) tea.Cmd {
	return func() tea.Msg {
		select {
		case event := <-events:
			return streamEventMsg{Event: event}
		case <-ctx.Done():
			return quitMsg{}
		}
	}
}
