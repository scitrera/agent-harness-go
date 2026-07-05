package tui

import "strings"

var slashCommandSuggestions = []selectionItem{
	{Value: "/clear", Label: "/clear", Description: "Clear current thread"},
	{Value: "/cancel", Label: "/cancel", Description: "Cancel active task"},
	{Value: "/status", Label: "/status", Description: "Show status"},
	{Value: "/threads", Label: "/threads", Description: "List threads"},
	{Value: "/tools", Label: "/tools", Description: "Show active tools"},
	{Value: "/tasks", Label: "/tasks", Description: "Show task state"},
	{Value: "/task", Label: "/task", Description: "Show task state"},
	{Value: "/team", Label: "/team", Description: "Show team graph"},
	{Value: "/agents", Label: "/agents", Description: "Show agents"},
	{Value: "/agent", Label: "/agent", Description: "Show agents"},
	{Value: "/approvals", Label: "/approvals", Description: "Show approvals"},
	{Value: "/approve", Label: "/approve", Description: "Approve request"},
	{Value: "/deny", Label: "/deny", Description: "Deny request"},
	{Value: "/permissions", Label: "/permissions", Description: "Manage permissions"},
	{Value: "/permission", Label: "/permission", Description: "Manage permissions"},
	{Value: "/requirements", Label: "/requirements", Description: "Show requirements"},
	{Value: "/mcp", Label: "/mcp", Description: "Show MCP state"},
	{Value: "/hooks", Label: "/hooks", Description: "Show hooks"},
	{Value: "/worldstate", Label: "/worldstate", Description: "Show world state"},
	{Value: "/compact", Label: "/compact", Description: "Compact context"},
	{Value: "/quit", Label: "/quit", Description: "Quit TUI"},
	{Value: "/exit", Label: "/exit", Description: "Quit TUI"},
}

func (m *model) refreshSlashSuggestions() {
	selectedValue := ""
	if m.selector.kind == selectionSlash {
		if selected, ok := m.selector.selectedItem(); ok {
			selectedValue = selected.Value
		}
	}
	items := matchingSlashSuggestions(m.composer.Value())
	if len(items) == 0 {
		if m.selector.kind == selectionSlash {
			m.selector.clear()
		}
		return
	}
	m.selector = newSelection(selectionSlash, items, selectedValue, maxSelectionRows)
}

func matchingSlashSuggestions(input string) []selectionItem {
	if input == "" || !strings.HasPrefix(input, "/") || strings.ContainsAny(input, " \t\n\r") {
		return nil
	}
	matches := make([]selectionItem, 0, maxSelectionRows)
	for _, suggestion := range slashCommandSuggestions {
		if strings.HasPrefix(suggestion.Value, input) {
			matches = append(matches, suggestion)
			if len(matches) == maxSelectionRows {
				return matches
			}
		}
	}
	return matches
}
