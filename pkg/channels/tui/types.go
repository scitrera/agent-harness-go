package tui

import (
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type rowKind string

const (
	rowSystem    rowKind = "system"
	rowUser      rowKind = "user"
	rowShell     rowKind = "shell"
	rowAssistant rowKind = "assistant"
	rowReasoning rowKind = "reasoning"
	rowThinking  rowKind = "thinking"
	rowTool      rowKind = "tool"
)

type chatRow struct {
	Kind      rowKind
	ID        string
	TaskID    string
	Text      string
	Streaming bool
	// ToolCall distinguishes actual tool activity from other rows rendered with
	// the tool style (notably approvals). Only consecutive ToolCall rows are
	// compacted in the transcript view.
	ToolCall bool
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

type subagentActivity struct {
	CallID    string
	TaskID    string
	ThreadID  string
	Name      string
	Phase     string
	Output    string
	Latest    string
	StartedMS int64
}

type turnActivity struct {
	WorkspaceID string
	ThreadID    string
	Phase       string
}

type renderedRowCache struct {
	Kind      rowKind
	Text      string
	Streaming bool
	Width     int
	Rendered  string
}

type pendingAttachment struct {
	Name string
	Mime string
	Size int64
	Part protocol.ContentPart
}

type outboundKind uint8

const (
	outboundConversation outboundKind = iota
	outboundCommand
	outboundMetaCommand
	outboundShell
)

type queuedMessage struct {
	ID               uint64
	Kind             outboundKind
	WorkspaceID      string
	ThreadID         string
	Input            string
	DisplayText      string
	Message          protocol.ChatMessage
	ReferencedImages []referencedImage
	Attachments      []pendingAttachment
	Presented        bool
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
	drawerConfirmation  drawerMode = "confirmation"
	drawerHelp          drawerMode = "help"
)

type confirmationKind string

const (
	confirmationNone         confirmationKind = ""
	confirmationClearThread  confirmationKind = "clear_thread"
	confirmationDeleteThread confirmationKind = "delete_thread"
)

type pendingConfirmation struct {
	Kind        confirmationKind
	WorkspaceID string
	ThreadID    string
}

func (c pendingConfirmation) active() bool {
	return c.Kind != confirmationNone && c.ThreadID != ""
}
