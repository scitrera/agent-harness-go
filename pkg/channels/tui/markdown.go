// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"strings"

	"charm.land/glamour/v2"
)

const defaultMarkdownWidth = 80

func renderAssistantMarkdown(text string, width int) string {
	if strings.TrimSpace(text) == "" || !looksLikeMarkdown(text) {
		return text
	}
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStylePath("dark"),
		glamour.WithWordWrap(normalizeMarkdownWidth(width)),
	)
	if err != nil {
		return text
	}
	rendered, err := renderer.Render(text)
	if err != nil {
		return text
	}
	return strings.Trim(rendered, "\r\n")
}

func looksLikeMarkdown(text string) bool {
	if strings.Contains(text, "**") || strings.Contains(text, "__") || strings.Contains(text, "`") || strings.Contains(text, "](") {
		return true
	}
	if strings.Count(text, "*") >= 2 || strings.Count(text, "_") >= 2 {
		return true
	}
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if isMarkdownLine(trimmed) {
			return true
		}
	}
	return false
}

func isMarkdownLine(line string) bool {
	if strings.HasPrefix(line, "# ") || strings.HasPrefix(line, "## ") || strings.HasPrefix(line, "### ") || strings.HasPrefix(line, "> ") {
		return true
	}
	if strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") || strings.HasPrefix(line, "+ ") {
		return true
	}
	if strings.Contains(line, "| ---") || strings.Contains(line, "--- |") {
		return true
	}
	return isOrderedListLine(line)
}

func isOrderedListLine(line string) bool {
	for i, r := range line {
		if r >= '0' && r <= '9' {
			continue
		}
		return i > 0 && strings.HasPrefix(line[i:], ". ")
	}
	return false
}

func normalizeMarkdownWidth(width int) int {
	if width <= 0 {
		return defaultMarkdownWidth
	}
	return width
}
