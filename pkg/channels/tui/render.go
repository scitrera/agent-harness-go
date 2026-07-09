package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

var (
	styleUser      = lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Bold(true)
	styleAssistant = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
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
	view.MouseMode = tea.MouseModeCellMotion
	if cursor := m.composer.Cursor(); cursor != nil {
		adjusted := *cursor
		adjusted.Position.Y += m.viewport.Height()
		if drawer := m.renderDrawer(); drawer != "" {
			adjusted.Position.Y += lipgloss.Height(drawer)
		}
		if selection := m.renderSelection(); selection != "" {
			adjusted.Position.Y += lipgloss.Height(selection)
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
	lines := make([]string, len(items))
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
			lines[offset] = style.Width(m.width).Render(line)
			continue
		}
		lines[offset] = style.Render(line)
	}
	return strings.Join(lines, "\n")
}

func (m model) renderRows() string {
	if len(m.rows) == 0 {
		return styleSystem.Render("New thread. Type a message or /commands.")
	}
	var b strings.Builder
	for i, row := range m.rows {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(m.renderRow(row))
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
	return style.Render(prefix+"> ") + text
}

func (m model) markdownWidth(prefix string) int {
	if m.width <= 0 {
		return defaultMarkdownWidth
	}
	available := m.width - lipgloss.Width(prefix+"> ")
	if available < minMarkdownWidth {
		return minMarkdownWidth
	}
	return available
}

func (m model) renderStatus() string {
	segments := []string{
		"status " + m.statusText(),
		"model " + m.activeModel(),
		"ctx unknown",
		fmt.Sprintf("scroll %.0f%%", m.viewport.ScrollPercent()*100),
		"thread " + shortID(m.threadID),
		fmt.Sprintf("approvals %d", len(m.pendingApprovals)),
		fmt.Sprintf("tools %d", m.activeTools()),
		fmt.Sprintf("dropped %d", m.channel.DroppedEvents()),
		"title " + m.activeThreadTitle(),
	}
	line := strings.Join(segments, " | ")
	if m.width > 0 {
		line = fitCells(line, m.width)
		return styleStatus.Width(m.width).Render(line)
	}
	return styleStatus.Render(line)
}

func (m model) renderDrawer() string {
	if m.drawer == drawerNone {
		return ""
	}
	if strings.TrimSpace(m.drawerContent) != "" {
		return m.fitDrawer(m.drawerContent)
	}
	switch m.drawer {
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
	limit := m.drawerHeight()
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
