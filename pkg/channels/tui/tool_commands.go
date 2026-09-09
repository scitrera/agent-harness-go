// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"fmt"
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func (m model) handleTools(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) >= 2 && fields[1] == "show" {
		if len(fields) < 3 {
			m.addSystem("usage: /tools show <call-id>")
			return m, nil
		}
		id, ok := m.matchTool(fields[2])
		if !ok {
			m.addSystem("tool call not found: " + fields[2])
			return m, nil
		}
		m.showToolDetail(id)
		return m, nil
	}
	activeOnly := len(fields) >= 2 && fields[1] == "active"
	m.openToolSelector(activeOnly)
	return m, nil
}

func (m *model) openToolSelector(activeOnly bool) {
	items := m.toolSelectionItems(activeOnly)
	if len(items) == 0 {
		text := "tool activity\nnone yet"
		if activeOnly {
			text = "active tools\nnone"
		}
		m.toolDetailID = ""
		m.toolActiveOnly = activeOnly
		m.showDrawer(drawerTools, text)
		return
	}
	m.drawer = drawerTools
	m.drawerContent = "tool activity\n↑/↓ select | / filter | Enter inspect | Esc close"
	m.toolActiveOnly = activeOnly
	m.toolDetailID = ""
	m.selector = newSelection(selectionTool, items, "", maxSelectionRows)
	m.status = "select tool"
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m *model) refreshToolSelector() {
	if m.selector.kind != selectionTool {
		return
	}
	selectedValue := ""
	if selected, ok := m.selector.selectedItem(); ok {
		selectedValue = selected.Value
	}
	items := m.toolSelectionItems(m.toolActiveOnly)
	if len(items) == 0 {
		m.selector.clear()
		m.drawer = drawerNone
		m.drawerContent = ""
		m.status = m.activeTurnStatus()
		m.reflowSurfaces()
		return
	}
	m.selector.replaceItems(items, selectedValue)
	m.reflowSurfaces()
}

func (m model) toolSelectionItems(activeOnly bool) []selectionItem {
	ids := make([]string, 0, len(m.tools))
	for id, entry := range m.tools {
		if activeOnly && entry.Event.Status != tools.ToolEventQueued && entry.Event.Status != tools.ToolEventStarted {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := m.tools[ids[i]]
		right := m.tools[ids[j]]
		if left.Seen == right.Seen {
			return ids[i] < ids[j]
		}
		return left.Seen > right.Seen
	})
	items := make([]selectionItem, 0, len(ids))
	for _, id := range ids {
		event := m.tools[id].Event
		items = append(items, selectionItem{
			Value:       id,
			Label:       shortID(id),
			Description: event.ToolName + " " + string(event.Status),
		})
	}
	return items
}

func (m model) matchTool(prefix string) (string, bool) {
	for id := range m.tools {
		if id == prefix || strings.HasPrefix(id, prefix) {
			return id, true
		}
	}
	return "", false
}

func (m *model) showToolDetail(id string) {
	entry, ok := m.tools[id]
	if !ok {
		m.addSystem("tool call not found: " + id)
		return
	}
	m.showDrawer(drawerTools, toolEventDetail(entry.Event))
	m.toolActiveOnly = false
	m.toolDetailID = id
	m.status = "tool " + entry.Event.ToolName
}

func (m *model) refreshOpenToolDrawer(changedID string) {
	if m.drawer != drawerTools || m.selector.kind == selectionTool {
		return
	}
	if m.toolDetailID != "" {
		if m.toolDetailID != changedID {
			return
		}
		entry, ok := m.tools[m.toolDetailID]
		if !ok {
			return
		}
		m.drawerContent = toolEventDetail(entry.Event)
	} else {
		m.drawerContent = m.toolsSummary()
	}
	m.reflowSurfaces()
}

func toolEventDetail(event tools.ToolEvent) string {
	lines := []string{
		"tool " + event.ToolName,
		"call: " + event.CallID,
		"status: " + string(event.Status),
	}
	if event.ArgumentsHash != "" {
		lines = append(lines, fmt.Sprintf("arguments: %d bytes sha256=%s", event.ArgumentsBytes, shortHash(event.ArgumentsHash)))
	}
	if event.DurationMS > 0 {
		lines = append(lines, fmt.Sprintf("duration: %dms", event.DurationMS))
	}
	if event.PolicyDecision != "" || event.ApprovalDecision != "" {
		lines = append(lines, "policy: "+emptyAs(event.PolicyDecision, "-")+" approval="+emptyAs(event.ApprovalDecision, "-"))
	}
	if event.ErrorCode != "" {
		lines = append(lines, "error: "+event.ErrorCode+" "+event.ErrorMessage)
	}
	if event.Result.PID > 0 || event.Result.ExitCode != 0 {
		lines = append(lines, fmt.Sprintf("process: pid=%d exit=%d", event.Result.PID, event.Result.ExitCode))
	}
	if event.Result.OutputBytes > 0 || event.Result.OutputTruncated {
		lines = append(lines, fmt.Sprintf("output: %d bytes truncated=%t", event.Result.OutputBytes, event.Result.OutputTruncated))
	}
	for _, change := range event.Result.FileChanges {
		lines = append(lines, "file: "+change.Kind+" "+change.Path)
	}
	return strings.Join(lines, "\n")
}

func shortHash(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}
