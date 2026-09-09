// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"fmt"
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"
)

func (m model) handlePermissions(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 2 || fields[1] == "list" || fields[1] == "ls" || fields[1] == "pending" {
		m.showDrawer(drawerPermissions, m.permissionSummary())
		return m, nil
	}
	switch fields[1] {
	case "approve", "grant", "allow":
		if len(fields) < 3 {
			m.addSystem("usage: /permissions approve <request-id> [once|session|always]")
			return m, nil
		}
		args := append([]string{"/approve"}, fields[2:]...)
		m.resolveApproval(args, true)
		return m, nil
	case "deny", "reject":
		if len(fields) < 3 {
			m.addSystem("usage: /permissions deny <request-id>")
			return m, nil
		}
		args := append([]string{"/deny"}, fields[2:]...)
		m.resolveApproval(args, false)
		return m, nil
	case "scopes":
		m.showDrawer(drawerPermissions, permissionScopeSummary())
		return m, nil
	}
	m.addSystem("unknown /permissions command")
	return m, nil
}

func (m model) permissionSummary() string {
	lines := []string{"tool permissions"}
	lines = append(lines, "approve: /permissions approve <request-id> [once|session|always]")
	lines = append(lines, "deny: /permissions deny <request-id>")
	lines = append(lines, "scopes: once runs only this call; session grants this process; always asks the runner to persist")
	if len(m.pendingApprovals) == 0 {
		return strings.Join(append(lines, "pending: none"), "\n")
	}
	ids := make([]string, 0, len(m.pendingApprovals))
	for id := range m.pendingApprovals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines = append(lines, "pending")
	for _, id := range ids {
		req := m.pendingApprovals[id]
		line := fmt.Sprintf("%s tool=%s task=%s status=%s", req.RequestID, req.Tool, shortID(req.TaskID), req.Status)
		if strings.TrimSpace(req.Reason) != "" {
			line += " reason=" + req.Reason
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func permissionScopeSummary() string {
	return strings.Join([]string{
		"permission scopes",
		"once: approve only the current tool call",
		"session: approve this tool for the current process and workspace",
		"always: request a durable workspace grant when the runner has a grant store",
	}, "\n")
}
