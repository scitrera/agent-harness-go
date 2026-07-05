package tui

import (
	"strings"

	"charm.land/lipgloss/v2"
)

func (m *model) refreshInputSurface() {
	if m.selector.kind == selectionThread && strings.TrimSpace(m.composer.Value()) == "" {
		m.reflowSurfaces()
		m.refreshViewport()
		return
	}
	if m.selector.kind == selectionThread {
		m.selector.clear()
	}
	m.refreshSlashSuggestions()
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m model) desiredComposerHeight() int {
	rows := m.composerVisualRows()
	if rows < minComposerHeight {
		return minComposerHeight
	}
	if rows > maxComposerHeight {
		return maxComposerHeight
	}
	return rows
}

func (m model) composerVisualRows() int {
	value := m.composer.Value()
	if value == "" {
		return minComposerHeight
	}
	contentWidth := m.composerContentWidth()
	rows := 0
	for _, line := range strings.Split(value, "\n") {
		lineWidth := lipgloss.Width(line)
		if lineWidth == 0 {
			rows++
			continue
		}
		rows += (lineWidth + contentWidth - 1) / contentWidth
	}
	return rows
}

func (m model) composerContentWidth() int {
	width := m.width
	if width <= 0 {
		width = defaultTerminalWidth
	}
	width -= lipgloss.Width(m.composer.Prompt)
	if width < 1 {
		return 1
	}
	return width
}
