// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/team"
)

func (m model) handleTeam(fields []string) (tea.Model, tea.Cmd) {
	if m.teamStore == nil {
		m.showDrawer(drawerTeam, "team graph is not wired")
		return m, nil
	}
	if len(fields) < 2 || fields[1] == "agents" || fields[1] == "list" || fields[1] == "ls" {
		return m, teamSummaryCmd(m.ctx, m.teamStore)
	}
	switch fields[1] {
	case "register":
		return m.registerTeamAgent(fields)
	case "children", "descendants":
		return m.teamDescendants(fields)
	case "add-child", "child":
		return m.addTeamChild(fields)
	case "status":
		return m.updateTeamStatus(fields)
	}
	m.addSystem("unknown /team command")
	return m, nil
}

func (m model) registerTeamAgent(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 4 {
		m.addSystem("usage: /team register <agent-id> <type> [name]")
		return m, nil
	}
	node := team.AgentNode{ID: team.AgentID(fields[2]), Type: subagent.AgentType(fields[3])}
	if len(fields) > 4 {
		node.Name = subagent.AgentName(strings.Join(fields[4:], " "))
	}
	return m, teamMutateCmd(m.ctx, "agent registered", func(ctx context.Context) (string, error) {
		if err := m.teamStore.RegisterAgent(ctx, node); err != nil {
			return "", err
		}
		return teamSummary(ctx, m.teamStore)
	})
}

func (m model) teamDescendants(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 {
		m.addSystem("usage: /team children <agent-id>")
		return m, nil
	}
	parentID := team.AgentID(fields[2])
	return m, teamMutateCmd(m.ctx, "team descendants loaded", func(ctx context.Context) (string, error) {
		agents, err := m.teamStore.DescendantsBFS(ctx, parentID)
		if err != nil {
			return "", err
		}
		if len(agents) == 0 {
			return "descendants for " + string(parentID) + ": none", nil
		}
		lines := []string{"descendants for " + string(parentID)}
		for _, agent := range agents {
			lines = append(lines, renderAgentNode(agent))
		}
		return strings.Join(lines, "\n"), nil
	})
}

func (m model) addTeamChild(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 5 {
		m.addSystem("usage: /team add-child <parent-id> <child-id> <type> [name]")
		return m, nil
	}
	child := team.AgentNode{ID: team.AgentID(fields[3]), Type: subagent.AgentType(fields[4])}
	if len(fields) > 5 {
		child.Name = subagent.AgentName(strings.Join(fields[5:], " "))
	}
	req := team.AddChildRequest{ParentID: team.AgentID(fields[2]), Child: child}
	return m, teamMutateCmd(m.ctx, "child agent added", func(ctx context.Context) (string, error) {
		if err := m.teamStore.AddChild(ctx, req); err != nil {
			return "", err
		}
		return teamSummary(ctx, m.teamStore)
	})
}

func (m model) updateTeamStatus(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 4 {
		m.addSystem("usage: /team status <agent-id> <running|completed|cancelled>")
		return m, nil
	}
	status := team.AgentStatus(fields[3])
	agentID := team.AgentID(fields[2])
	return m, teamMutateCmd(m.ctx, "agent status updated", func(ctx context.Context) (string, error) {
		if _, err := m.teamStore.UpdateAgentStatus(ctx, agentID, status); err != nil {
			return "", err
		}
		return teamSummary(ctx, m.teamStore)
	})
}

func teamSummaryCmd(ctx context.Context, store TeamStore) tea.Cmd {
	return teamMutateCmd(ctx, "team loaded", func(ctx context.Context) (string, error) {
		return teamSummary(ctx, store)
	})
}

func teamMutateCmd(ctx context.Context, status string, run func(context.Context) (string, error)) tea.Cmd {
	return func() tea.Msg {
		text, err := run(ctx)
		return drawerLoadedMsg{Drawer: drawerTeam, Text: text, Status: status, Err: err}
	}
}

func teamSummary(ctx context.Context, store TeamStore) (string, error) {
	agents, err := store.ListAgents(ctx)
	if err != nil {
		return "", err
	}
	if len(agents) == 0 {
		return "team agents: none", nil
	}
	lines := []string{"team agents"}
	for _, agent := range agents {
		lines = append(lines, renderAgentNode(agent))
	}
	return strings.Join(lines, "\n"), nil
}

func renderAgentNode(agent team.AgentNode) string {
	name := string(agent.Name)
	if name == "" {
		name = "-"
	}
	return fmt.Sprintf("%s type=%s status=%s name=%s", agent.ID, agent.Type, agent.Status, name)
}
