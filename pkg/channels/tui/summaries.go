package tui

import (
	"fmt"
	"sort"
	"strings"
)

func (m model) statusSummary() string {
	lines := []string{
		"model: " + m.activeModel(),
	}
	if m.workspaceID != "" {
		lines = append(lines, "workspace: "+m.workspaceID)
	}
	lines = append(lines,
		"thread: "+m.threadID,
		"title: "+m.activeThreadTitle(),
		fmt.Sprintf("threads: %d", len(m.threads)),
		fmt.Sprintf("pending approvals: %d", len(m.pendingApprovals)),
		fmt.Sprintf("tracked tools: %d", len(m.tools)),
		fmt.Sprintf("scroll: %.0f%%", m.viewport.ScrollPercent()*100),
		fmt.Sprintf("dropped events: %d", m.droppedEvents()),
	)
	return strings.Join(lines, "\n")
}

func (m model) droppedEvents() int64 {
	if m.channel == nil {
		return 0
	}
	return m.channel.DroppedEvents()
}

func (m model) approvalSummary() string {
	if len(m.pendingApprovals) == 0 {
		return "no pending approvals"
	}
	ids := make([]string, 0, len(m.pendingApprovals))
	for id := range m.pendingApprovals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := []string{"pending approvals"}
	for _, id := range ids {
		req := m.pendingApprovals[id]
		lines = append(lines, fmt.Sprintf("%s tool=%s task=%s status=%s", req.RequestID, req.Tool, shortID(req.TaskID), req.Status))
	}
	return strings.Join(lines, "\n")
}

func (m model) toolsSummary() string {
	if len(m.tools) == 0 {
		return "no tool activity yet"
	}
	ids := make([]string, 0, len(m.tools))
	for id := range m.tools {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := []string{"tool activity"}
	for _, id := range ids {
		lines = append(lines, shortID(id)+" "+renderToolEvent(m.tools[id].Event))
	}
	return strings.Join(lines, "\n")
}

func (m model) currentThreadSummary() string {
	lines := []string{
		"current thread",
	}
	if m.workspaceID != "" {
		lines = append(lines, "workspace: "+m.workspaceID)
	}
	lines = append(lines,
		"id: "+m.threadID,
		"title: "+m.activeThreadTitle(),
		fmt.Sprintf("messages shown: %d", len(m.rows)),
	)
	return strings.Join(lines, "\n")
}

func (m model) threadSummary() string {
	if len(m.threads) == 0 {
		return "no threads"
	}
	lines := []string{"threads"}
	for _, session := range m.threads {
		marker := " "
		if session.ID == m.threadID {
			marker = "*"
		}
		lines = append(lines, fmt.Sprintf("%s %s %s", marker, shortID(session.ID), session.Title))
	}
	return strings.Join(lines, "\n")
}
