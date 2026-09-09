// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"
)

func (m *model) openApprovalSelector() {
	items := m.approvalSelectionItems()
	if len(items) == 0 {
		m.showDrawer(drawerApprovals, "approval inbox\nno pending approvals")
		return
	}
	m.drawer = drawerApprovals
	m.drawerContent = "approval inbox\n↑/↓ select | / filter | a/s/A approve | d deny"
	m.selector = newSelection(selectionApproval, items, "", maxSelectionRows)
	m.status = "select approval"
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m model) approvalSelectionItems() []selectionItem {
	ids := make([]string, 0, len(m.pendingApprovals))
	for id := range m.pendingApprovals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]selectionItem, 0, len(ids))
	for _, id := range ids {
		req := m.pendingApprovals[id]
		description := strings.TrimSpace(req.Tool + " " + req.Reason)
		items = append(items, selectionItem{
			Value:       id,
			Label:       shortID(id),
			Description: description,
		})
	}
	return items
}

func (m *model) refreshApprovalSelector() {
	if m.selector.kind != selectionApproval {
		return
	}
	selectedValue := ""
	if selected, ok := m.selector.selectedItem(); ok {
		selectedValue = selected.Value
	}
	items := m.approvalSelectionItems()
	if len(items) == 0 {
		m.selector.clear()
		m.drawer = drawerNone
		m.drawerContent = ""
		m.status = "ready"
		m.reflowSurfaces()
		return
	}
	m.selector.replaceItems(items, selectedValue)
	m.reflowSurfaces()
}

func (m model) resolveSelectedApproval(granted bool, scope string) (tea.Model, tea.Cmd) {
	item, ok := m.selector.selectedItem()
	if !ok {
		return m, nil
	}
	command := "/deny"
	if granted {
		command = "/approve"
	}
	m.resolveApproval([]string{command, item.Value, scope}, granted)
	m.refreshApprovalSelector()
	return m, nil
}
