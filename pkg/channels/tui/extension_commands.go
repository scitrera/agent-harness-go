package tui

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

func (m model) handleAgents(fields []string) (tea.Model, tea.Cmd) {
	if m.agentCatalog == nil {
		m.showDrawer(drawerAgents, "agent catalog is not wired")
		return m, nil
	}
	if len(fields) < 2 || fields[1] == "list" || fields[1] == "ls" {
		return m, agentsSummaryCmd(m.ctx, m.agentCatalog)
	}
	if fields[1] == "show" {
		if len(fields) < 3 {
			m.addSystem("usage: /agents show <type>")
			return m, nil
		}
		return m, agentDetailCmd(m.ctx, m.agentCatalog, subagent.AgentType(fields[2]))
	}
	m.addSystem("unknown /agents command")
	return m, nil
}

func (m model) handleExtension(fields []string) (tea.Model, tea.Cmd) {
	name := strings.TrimPrefix(fields[0], "/")
	summary := m.extensionSummary(name)
	m.showDrawer(drawerForExtension(name), summary)
	return m, nil
}

func agentsSummaryCmd(ctx context.Context, catalog AgentCatalog) tea.Cmd {
	return func() tea.Msg {
		defs, err := catalog.List(ctx)
		if err != nil {
			return drawerLoadedMsg{Drawer: drawerAgents, Err: err}
		}
		return drawerLoadedMsg{Drawer: drawerAgents, Text: agentDefinitionsSummary(defs), Status: "agents loaded"}
	}
}

func agentDetailCmd(ctx context.Context, catalog AgentCatalog, typ subagent.AgentType) tea.Cmd {
	return func() tea.Msg {
		def, err := catalog.Get(ctx, typ)
		if err != nil {
			return drawerLoadedMsg{Drawer: drawerAgents, Err: err}
		}
		return drawerLoadedMsg{Drawer: drawerAgents, Text: agentDefinitionDetail(def), Status: "agent loaded"}
	}
}

func agentDefinitionsSummary(defs []subagent.Definition) string {
	if len(defs) == 0 {
		return "agents: none"
	}
	lines := []string{"agents"}
	for _, def := range defs {
		mode := string(def.PermissionMode)
		if mode == "" {
			mode = "inherit"
		}
		lines = append(lines, fmt.Sprintf("%s name=%s mode=%s tools=%d background=%t", def.Type, def.Name, mode, len(def.AllowedTools), def.Background))
	}
	return strings.Join(lines, "\n")
}

func agentDefinitionDetail(def subagent.Definition) string {
	mode := string(def.PermissionMode)
	if mode == "" {
		mode = "inherit"
	}
	lines := []string{
		"agent " + string(def.Type),
		"name: " + string(def.Name),
		"description: " + def.Description,
		"permission mode: " + mode,
		"model: " + emptyAs(def.Model, "inherit"),
		fmt.Sprintf("max turns: %d", def.MaxTurns),
		"allow tools: " + listOrNone(def.AllowedTools),
		"deny tools: " + listOrNone(def.DeniedTools),
		"skills: " + listOrNone(def.Skills),
		"mcp servers: " + listOrNone(def.MCPServers),
	}
	if def.ExecPolicyHint != "" {
		lines = append(lines, "exec policy: "+def.ExecPolicyHint)
	}
	return strings.Join(lines, "\n")
}

func (m model) extensionSummary(name string) string {
	lines := []string{"extension surfaces"}
	lines = append(lines, "agents: /agents [list|show <type>]")
	lines = append(lines, "tasks: /tasks plans | /tasks list <plan-id> | /tasks create <plan-id> <task-id> <title>")
	lines = append(lines, "team: /team agents | /team register <agent-id> <type> | /team add-child <parent-id> <child-id> <type>")
	lines = append(lines, "permissions: /permissions pending | /permissions approve <request-id> [scope]")
	lines = append(lines, "tools: /tools")
	switch name {
	case "requirements":
		lines = append(lines, "requirements: composed policy/hook/mcp metadata is not yet exposed as a live TUI store")
	case "mcp":
		lines = append(lines, "mcp: tool lifecycle events appear in /tools; server inventory is pending a registry surface")
	case "hooks":
		lines = append(lines, "hooks: pre/post tool decisions appear through tool lifecycle and approval rows")
	case "worldstate":
		lines = append(lines, "worldstate: compacted context state is still runner-owned; use slash command pass-through when configured")
	case "compact":
		lines = append(lines, "compact: use configured runner command for compaction; TUI keeps the chat surface stable while it runs")
	}
	return strings.Join(lines, "\n")
}

func drawerForExtension(name string) drawerMode {
	if name == "requirements" || name == "mcp" || name == "hooks" || name == "worldstate" || name == "compact" {
		return drawerRequirements
	}
	return drawerExtensibility
}

func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ",")
}

func emptyAs(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
