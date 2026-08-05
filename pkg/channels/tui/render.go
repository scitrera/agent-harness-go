package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

var (
	styleUser      = lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Bold(true)
	styleAssistant = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
	styleThinking  = lipgloss.NewStyle().Foreground(lipgloss.Color("244")).Italic(true)
	styleSystem    = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	styleTool      = lipgloss.NewStyle().Foreground(lipgloss.Color("151"))
	styleStatus    = lipgloss.NewStyle().Foreground(lipgloss.Color("238")).Background(lipgloss.Color("252"))
	styleDrawer    = lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Background(lipgloss.Color("236")).Padding(0, 1)
	styleSuggest   = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))
	styleSuggestOn = lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Background(lipgloss.Color("238"))
)

func (m model) View() tea.View {
	view := tea.NewView(m.render())
	view.AltScreen = true
	// Leave mouse reporting disabled so the terminal owns drag selection and copy.
	view.MouseMode = tea.MouseModeNone
	if cursor := m.composer.Cursor(); cursor != nil {
		adjusted := *cursor
		adjusted.Y += m.viewport.Height()
		if drawer := m.renderDrawer(); drawer != "" {
			adjusted.Y += lipgloss.Height(drawer)
		}
		if selection := m.renderSelection(); selection != "" {
			adjusted.Y += lipgloss.Height(selection)
		}
		view.Cursor = &adjusted
	}
	return view
}

func (m model) render() string {
	parts := []string{m.viewport.View()}
	if drawer := m.renderDrawer(); drawer != "" {
		parts = append(parts, drawer)
	}
	if selection := m.renderSelection(); selection != "" {
		parts = append(parts, selection)
	}
	parts = append(parts, m.composer.View())
	parts = append(parts, m.renderStatus())
	return strings.Join(parts, "\n")
}

func (m model) renderSelection() string {
	if !m.selector.active() {
		return ""
	}
	start, items := m.selector.visibleItems()
	lines := make([]string, 0, m.selector.height())
	if m.selector.filtering || m.selector.query != "" {
		filter := "  filter: " + m.selector.query
		if m.width > 0 {
			filter = styleSuggest.Width(m.width).Render(fitCells(filter, m.width))
		} else {
			filter = styleSuggest.Render(filter)
		}
		lines = append(lines, filter)
	}
	if len(items) == 0 {
		empty := "  no matches"
		if m.width > 0 {
			empty = styleSuggest.Width(m.width).Render(empty)
		} else {
			empty = styleSuggest.Render(empty)
		}
		return strings.Join(append(lines, empty), "\n")
	}
	for offset, item := range items {
		index := start + offset
		marker := "  "
		style := styleSuggest
		if index == m.selector.selected {
			marker = "> "
			style = styleSuggestOn
		}
		line := marker + item.Label
		if item.Description != "" {
			line += "  " + item.Description
		}
		if m.width > 0 {
			line = fitCells(line, m.width)
			lines = append(lines, style.Width(m.width).Render(line))
			continue
		}
		lines = append(lines, style.Render(line))
	}
	return strings.Join(lines, "\n")
}

func (m *model) renderRows() string {
	if len(m.rows) == 0 {
		return styleSystem.Render("New thread. Type a message or /commands.")
	}
	if m.renderedRows == nil {
		m.renderedRows = map[string]renderedRowCache{}
	}
	var b strings.Builder
	for i, row := range m.rows {
		if i > 0 {
			b.WriteString("\n")
		}
		key := fmt.Sprintf("%d:%s", i, row.ID)
		cached, ok := m.renderedRows[key]
		if !ok || cached.Kind != row.Kind || cached.Text != row.Text || cached.Streaming != row.Streaming || cached.Width != m.width {
			cached = renderedRowCache{
				Kind:      row.Kind,
				Text:      row.Text,
				Streaming: row.Streaming,
				Width:     m.width,
				Rendered:  m.renderRow(row),
			}
			m.renderedRows[key] = cached
		}
		b.WriteString(cached.Rendered)
	}
	return b.String()
}

func (m model) renderRow(row chatRow) string {
	prefix := ""
	style := styleAssistant
	switch row.Kind {
	case rowUser:
		prefix = "you"
		style = styleUser
	case rowAssistant:
		prefix = "assistant"
		style = styleAssistant
	case rowThinking:
		prefix = "assistant"
		style = styleThinking
	case rowTool:
		prefix = "tool"
		style = styleTool
	case rowSystem:
		prefix = "system"
		style = styleSystem
	}
	text := strings.TrimSpace(row.Text)
	if text == "" && row.Streaming {
		text = "..."
	}
	if row.Kind == rowAssistant {
		text = renderAssistantMarkdown(text, m.markdownWidth(prefix))
	}
	prefixText := prefix + "> "
	prefixWidth := lipgloss.Width(prefixText)
	available := m.width - prefixWidth
	if m.width <= 0 {
		available = defaultMarkdownWidth
	}
	if available < 1 {
		available = 1
	}
	text = wrapCells(text, available)
	return style.Render(prefixText) + indentContinuation(text, prefixWidth)
}

func (m model) markdownWidth(prefix string) int {
	if m.width <= 0 {
		return defaultMarkdownWidth
	}
	available := m.width - lipgloss.Width(prefix+"> ")
	if available < 1 {
		return 1
	}
	return available
}

func wrapCells(text string, width int) string {
	if width <= 0 || text == "" {
		return text
	}
	wordWrapped := ansi.Wordwrap(text, width, "")
	return ansi.Hardwrap(wordWrapped, width, true)
}

func indentContinuation(text string, width int) string {
	if width <= 0 || !strings.Contains(text, "\n") {
		return text
	}
	indent := strings.Repeat(" ", width)
	return strings.ReplaceAll(text, "\n", "\n"+indent)
}

func (m model) renderStatus() string {
	segments := []string{
		"status " + m.statusText(),
	}
	if count := len(m.pendingApprovals); count > 0 {
		segments = append(segments, fmt.Sprintf("approvals %d", count))
	}
	if count := m.activeTools(); count > 0 {
		segments = append(segments, fmt.Sprintf("tools %d", count))
	}
	if count := len(m.attachments); count > 0 {
		segments = append(segments, fmt.Sprintf("images %d", count))
	}
	if dropped := m.droppedEvents(); dropped > 0 {
		segments = append(segments, fmt.Sprintf("dropped %d", dropped))
	}
	segments = append(segments, "model "+compactModelName(m.activeModel()))
	thread := m.activeThreadTitle()
	if thread == "" {
		thread = shortID(m.threadID)
	}
	if thread != "" {
		segments = append(segments, "thread "+fitCells(thread, 20))
	}
	if !m.viewport.AtBottom() {
		segments = append(segments, fmt.Sprintf("scroll %.0f%%", m.viewport.ScrollPercent()*100))
	}
	line := fitStatusSegments(segments, m.width)
	if m.width > 0 {
		return styleStatus.Width(m.width).Render(line)
	}
	return styleStatus.Render(line)
}

func fitStatusSegments(segments []string, width int) string {
	if width <= 0 {
		return strings.Join(segments, " | ")
	}
	line := ""
	for _, segment := range segments {
		if line == "" {
			line = fitCells(segment, width)
			continue
		}
		candidate := line + " | " + segment
		if lipgloss.Width(candidate) > width {
			continue
		}
		line = candidate
	}
	return line
}

func compactModelName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unknown"
	}
	if index := strings.LastIndex(name, "/"); index >= 0 && index+1 < len(name) {
		name = name[index+1:]
	}
	return fitCells(name, 24)
}

func (m model) renderDrawer() string {
	if m.drawer == drawerNone {
		return ""
	}
	if strings.TrimSpace(m.drawerContent) != "" {
		return m.fitDrawer(m.drawerContent)
	}
	switch m.drawer {
	case drawerHelp:
		return m.fitDrawer(helpText)
	case drawerApprovals:
		return m.fitDrawer(m.approvalSummary())
	case drawerPermissions:
		return m.fitDrawer(m.permissionSummary())
	case drawerTools:
		return m.fitDrawer(m.toolsSummary())
	case drawerThreads:
		return m.fitDrawer(m.threadSummary())
	case drawerTasks:
		return m.fitDrawer("task state loading")
	case drawerTeam:
		return m.fitDrawer("team graph loading")
	case drawerAgents:
		return m.fitDrawer("agent catalog loading")
	case drawerRequirements, drawerExtensibility:
		return m.fitDrawer(m.extensionSummary(""))
	default:
		return ""
	}
}

func (m model) fitDrawer(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	limit := m.drawerMaxHeight()
	if limit > 0 && len(lines) > limit {
		lines = append(lines[:limit-1], fmt.Sprintf("... %d more", len(lines)-limit+1))
	}
	if m.width > 0 {
		contentWidth := m.width - 2
		if contentWidth < 1 {
			contentWidth = 1
		}
		for i, line := range lines {
			lines[i] = fitCells(line, contentWidth)
		}
		return styleDrawer.Width(m.width).Render(strings.Join(lines, "\n"))
	}
	return styleDrawer.Render(strings.Join(lines, "\n"))
}

func (m model) statusText() string {
	if strings.TrimSpace(m.status) == "" {
		return "ready"
	}
	return firstLine(m.status)
}

func (m model) activeModel() string {
	if m.modelStatus == nil {
		return "unknown"
	}
	name := m.modelStatus.ActiveModelName(m.threadID)
	if name == "" {
		return "unknown"
	}
	return name
}

func (m model) activeTools() int {
	count := 0
	for _, entry := range m.tools {
		switch entry.Event.Status {
		case "queued", "started":
			count++
		}
	}
	return count
}

func fitCells(s string, width int) string {
	if width <= 0 || lipgloss.Width(s) <= width {
		return s
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	var b strings.Builder
	for _, r := range s {
		candidate := b.String() + string(r) + "..."
		if lipgloss.Width(candidate) > width {
			break
		}
		b.WriteRune(r)
	}
	return b.String() + "..."
}

func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}
