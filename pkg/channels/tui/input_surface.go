package tui

import (
	"strings"
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
	if m.refreshPathSuggestions() {
		m.reflowSurfaces()
		m.refreshViewport()
		return
	}
	m.refreshSlashSuggestions()
	m.reflowSurfaces()
	m.refreshViewport()
}
