package tui

import "github.com/scitrera/agent-harness-go/pkg/tools"

type rowKind string

const (
	rowSystem    rowKind = "system"
	rowUser      rowKind = "user"
	rowAssistant rowKind = "assistant"
	rowTool      rowKind = "tool"
)

type chatRow struct {
	Kind      rowKind
	ID        string
	TaskID    string
	Text      string
	Streaming bool
}

type approvalRequest struct {
	RequestID string
	TaskID    string
	Tool      string
	Status    string
	Reason    string
}

type toolEntry struct {
	Event tools.ToolEvent
	Seen  int64
}

type drawerMode string

const (
	drawerNone          drawerMode = ""
	drawerApprovals     drawerMode = "approvals"
	drawerPermissions   drawerMode = "permissions"
	drawerTools         drawerMode = "tools"
	drawerThreads       drawerMode = "threads"
	drawerTasks         drawerMode = "tasks"
	drawerTeam          drawerMode = "team"
	drawerAgents        drawerMode = "agents"
	drawerRequirements  drawerMode = "requirements"
	drawerExtensibility drawerMode = "extensibility"
)
