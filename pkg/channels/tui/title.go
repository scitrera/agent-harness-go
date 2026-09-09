// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"strings"
	"unicode"

	tea "charm.land/bubbletea/v2"
)

const terminalTitlePrefix = "Project Sahara"

func (m model) terminalTitle() string {
	title := m.activeThreadTitle()
	if strings.TrimSpace(title) == "" {
		title = shortID(m.threadID)
	}
	if strings.TrimSpace(title) == "" {
		return terminalTitlePrefix
	}
	return terminalTitlePrefix + " - " + title
}

func (m model) activeThreadTitle() string {
	for _, session := range m.threads {
		if session.ID == m.threadID {
			return session.Title
		}
	}
	return ""
}

func setTerminalTitleCmd(title string) tea.Cmd {
	return tea.Printf("\x1b]0;%s\x07", sanitizeTerminalTitle(title))
}

func sanitizeTerminalTitle(title string) string {
	var b strings.Builder
	for _, r := range title {
		if r == '\x1b' || r == '\x07' || unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}
