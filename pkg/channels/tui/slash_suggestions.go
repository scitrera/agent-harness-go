package tui

import (
	"fmt"
	"sort"
	"strings"
)

var localSlashCommandSuggestions = []selectionItem{
	{Value: "/help", Label: "/help", Description: "List commands"},
	{Value: "/commands", Label: "/commands", Description: "List commands"},
	{Value: "/model", Label: "/model", Description: "List or switch models"},
	{Value: "/models", Label: "/models", Description: "List models"},
	{Value: "/schedules", Label: "/schedules", Description: "Inspect scheduled turns"},
	{Value: "/runs", Label: "/runs", Description: "Inspect scheduled runs"},
	{Value: "/thread", Label: "/thread", Description: "Manage threads"},
	{Value: "/threads", Label: "/threads", Description: "Select a thread"},
	{Value: "/status", Label: "/status", Description: "Show status"},
	{Value: "/pwd", Label: "/pwd", Description: "Show working directory"},
	{Value: "/cd", Label: "/cd", Description: "Change working directory"},
	{Value: "/cancel", Label: "/cancel", Description: "Cancel active task"},
	{Value: "/clear", Label: "/clear", Description: "Clear current thread"},
	{Value: "/tools", Label: "/tools", Description: "Inspect tool activity"},
	{Value: "/attach", Label: "/attach", Description: "Attach an image from the working directory"},
	{Value: "/attachments", Label: "/attachments", Description: "Show queued images"},
	{Value: "/tasks", Label: "/tasks", Description: "Manage task state"},
	{Value: "/task", Label: "/task", Description: "Manage task state"},
	{Value: "/team", Label: "/team", Description: "Manage team graph"},
	{Value: "/agents", Label: "/agents", Description: "Inspect agents"},
	{Value: "/agent", Label: "/agent", Description: "Inspect agents"},
	{Value: "/approvals", Label: "/approvals", Description: "Review approvals"},
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

var slashSubcommands = map[string][]selectionItem{
	"model": {
		{Value: "list", Label: "list", Description: "List available models"},
		{Value: "switch", Label: "switch", Description: "Switch model"},
	},
	"models": {
		{Value: "list", Label: "list", Description: "List available models"},
	},
	"runs": {
		{Value: "--help", Label: "--help", Description: "Show filters and cursor usage"},
		{Value: "--status", Label: "--status", Description: "Filter task status"},
		{Value: "--limit", Label: "--limit", Description: "Set page size"},
		{Value: "--cursor", Label: "--cursor", Description: "Continue with opaque cursor"},
	},
	"thread": {
		{Value: "list", Label: "list", Description: "Select a thread"},
		{Value: "current", Label: "current", Description: "Show current thread"},
		{Value: "new", Label: "new", Description: "Create a thread"},
		{Value: "switch", Label: "switch", Description: "Switch thread"},
		{Value: "rename", Label: "rename", Description: "Rename thread"},
		{Value: "clear", Label: "clear", Description: "Clear a thread"},
		{Value: "delete", Label: "delete", Description: "Delete a thread"},
	},
	"tools": {
		{Value: "active", Label: "active", Description: "Show active tools"},
		{Value: "history", Label: "history", Description: "Show tool history"},
		{Value: "show", Label: "show", Description: "Inspect one tool call"},
	},
	"tasks": {
		{Value: "plans", Label: "plans", Description: "List plans"},
		{Value: "list", Label: "list", Description: "List tasks"},
		{Value: "plan", Label: "plan", Description: "Manage plans"},
		{Value: "create", Label: "create", Description: "Create a task"},
		{Value: "claim", Label: "claim", Description: "Claim a task"},
		{Value: "next", Label: "next", Description: "Claim next task"},
		{Value: "approve", Label: "approve", Description: "Approve a task"},
		{Value: "reject", Label: "reject", Description: "Reject a task"},
		{Value: "complete", Label: "complete", Description: "Complete a task"},
		{Value: "cancel", Label: "cancel", Description: "Cancel a task"},
		{Value: "reopen", Label: "reopen", Description: "Reopen a task"},
	},
	"team": {
		{Value: "agents", Label: "agents", Description: "List agents"},
		{Value: "register", Label: "register", Description: "Register agent"},
		{Value: "children", Label: "children", Description: "List descendants"},
		{Value: "add-child", Label: "add-child", Description: "Add child agent"},
		{Value: "status", Label: "status", Description: "Update agent status"},
	},
	"agents": {
		{Value: "list", Label: "list", Description: "List agents"},
		{Value: "show", Label: "show", Description: "Inspect an agent"},
	},
	"permissions": {
		{Value: "pending", Label: "pending", Description: "Review pending requests"},
		{Value: "approve", Label: "approve", Description: "Approve a request"},
		{Value: "deny", Label: "deny", Description: "Deny a request"},
		{Value: "scopes", Label: "scopes", Description: "Explain approval scopes"},
	},
	"requirements": {
		{Value: "summary", Label: "summary", Description: "Show summary"},
		{Value: "hooks", Label: "hooks", Description: "Show hooks"},
		{Value: "policy", Label: "policy", Description: "Show policy"},
		{Value: "models", Label: "models", Description: "Show models"},
		{Value: "mcp", Label: "mcp", Description: "Show MCP configuration"},
		{Value: "catalogs", Label: "catalogs", Description: "Show catalogs"},
		{Value: "features", Label: "features", Description: "Show feature gates"},
	},
	"mcp": {
		{Value: "resources", Label: "resources", Description: "List resources"},
		{Value: "templates", Label: "templates", Description: "List templates"},
		{Value: "read", Label: "read", Description: "Read a resource"},
	},
	"compact": {
		{Value: "status", Label: "status", Description: "Show compaction status"},
	},
	"attach": {
		{Value: "clear", Label: "clear", Description: "Clear queued images"},
	},
}

func (m *model) refreshSlashSuggestions() {
	selectedValue := ""
	if m.selector.kind == selectionSlash {
		if selected, ok := m.selector.selectedItem(); ok {
			selectedValue = selected.Value
		}
	}
	items := m.matchingSlashSuggestions(m.composer.Value())
	if len(items) == 0 {
		if m.selector.kind == selectionSlash {
			m.selector.clear()
		}
		return
	}
	m.selector = newSelection(selectionSlash, items, selectedValue, maxSelectionRows)
}

func (m model) matchingSlashSuggestions(input string) []selectionItem {
	if input == "" || !strings.HasPrefix(input, "/") || strings.ContainsAny(input, "\n\r") {
		return nil
	}
	if !strings.ContainsAny(input, " \t") {
		return matchingSelectionItems(m.rootSlashSuggestions(), input, true)
	}
	return m.matchingSlashArguments(input)
}

func (m model) rootSlashSuggestions() []selectionItem {
	items := append([]selectionItem(nil), localSlashCommandSuggestions...)
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		seen[item.Value] = struct{}{}
	}
	if m.commandSource == nil {
		return items
	}
	for _, command := range m.commandSource.AvailableCommands() {
		value := "/" + strings.TrimSpace(command.Name)
		if value == "/" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		label := value
		if hint := strings.TrimSpace(command.ArgumentHint); hint != "" {
			label += " " + hint
		}
		items = append(items, selectionItem{Value: value, Label: label, Description: command.Description})
		seen[value] = struct{}{}
	}
	return items
}

func (m model) matchingSlashArguments(input string) []selectionItem {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return nil
	}
	command := strings.ToLower(strings.TrimPrefix(fields[0], "/"))
	if command == "threads" {
		command = "thread"
	}
	if command == "task" {
		command = "tasks"
	}
	if command == "agent" {
		command = "agents"
	}
	if command == "permission" {
		command = "permissions"
	}
	position, prefix := slashArgumentPosition(input, fields)

	var candidates []selectionItem
	if position == 1 {
		candidates = slashSubcommands[command]
	}
	if position >= 2 {
		subcommand := ""
		if len(fields) > 1 {
			subcommand = strings.ToLower(fields[1])
		}
		switch {
		case command == "thread" && containsString([]string{"switch", "delete", "clear", "rename"}, subcommand):
			for _, session := range m.threads {
				description := session.Title
				if session.ID == m.threadID {
					description = strings.TrimSpace("current " + description)
				}
				candidates = append(candidates, selectionItem{Value: session.ID, Label: shortID(session.ID), Description: description})
			}
		case command == "tools" && subcommand == "show":
			for id, entry := range m.tools {
				candidates = append(candidates, selectionItem{Value: id, Label: shortID(id), Description: entry.Event.ToolName + " " + string(entry.Event.Status)})
			}
		case command == "permissions" && containsString([]string{"approve", "deny"}, subcommand):
			candidates = m.approvalArgumentSuggestions()
		}
	}
	if (command == "approve" || command == "deny") && position == 1 {
		candidates = m.approvalArgumentSuggestions()
	}
	if (command == "approve" && position == 2) ||
		(command == "permissions" && len(fields) > 1 && strings.EqualFold(fields[1], "approve") && position == 3) {
		candidates = []selectionItem{
			{Value: "once", Label: "once", Description: "Current call only"},
			{Value: "session", Label: "session", Description: "Current process and workspace"},
			{Value: "always", Label: "always", Description: "Persist when supported"},
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Label < candidates[j].Label })
	matches := matchingSelectionItems(candidates, prefix, false)
	for i := range matches {
		matches[i].Value = replaceSlashArgument(input, matches[i].Value)
	}
	return matches
}

func (m model) approvalArgumentSuggestions() []selectionItem {
	items := make([]selectionItem, 0, len(m.pendingApprovals))
	for id, req := range m.pendingApprovals {
		items = append(items, selectionItem{
			Value:       id,
			Label:       shortID(id),
			Description: strings.TrimSpace(fmt.Sprintf("%s %s", req.Tool, req.Reason)),
		})
	}
	return items
}

func slashArgumentPosition(input string, fields []string) (position int, prefix string) {
	if strings.HasSuffix(input, " ") || strings.HasSuffix(input, "\t") {
		return len(fields), ""
	}
	return len(fields) - 1, fields[len(fields)-1]
}

func replaceSlashArgument(input, replacement string) string {
	trimmed := strings.TrimRight(input, " \t")
	if len(trimmed) != len(input) {
		return trimmed + " " + replacement
	}
	index := strings.LastIndexAny(input, " \t")
	if index < 0 {
		return replacement
	}
	return input[:index+1] + replacement
}

func matchingSelectionItems(items []selectionItem, prefix string, valuesIncludeSlash bool) []selectionItem {
	prefix = strings.ToLower(prefix)
	matches := make([]selectionItem, 0, len(items))
	for _, item := range items {
		value := item.Value
		if !valuesIncludeSlash {
			value = strings.TrimPrefix(value, "/")
		}
		if strings.HasPrefix(strings.ToLower(value), prefix) {
			matches = append(matches, item)
		}
	}
	return matches
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
