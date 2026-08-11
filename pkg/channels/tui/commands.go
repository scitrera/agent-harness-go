package tui

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func (m model) handleSlash(input string) (tea.Model, tea.Cmd) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return m, nil
	}
	switch fields[0] {
	case "/help":
		m.openHelp()
		return m, nil
	case "/commands":
		m.openCommandBrowser()
		return m, nil
	case "/quit", "/exit":
		return m, tea.Quit
	case "/cancel":
		m.cancelActive()
		return m, nil
	case "/status":
		m.addSystem(m.statusSummary())
		return m, nil
	case "/model", "/models", "/schedules", "/runs":
		return m.enqueueMetaCommand(input)
	case "/pwd", "/cd":
		updated, cmd, err := m.handleWorkingDirectory(fields)
		if err != nil {
			m.addSystem(fields[0] + ": " + err.Error())
			return m, nil
		}
		return updated, cmd
	case "/clear":
		return m.clearThread(append([]string{"/thread", "clear"}, fields[1:]...))
	case "/thread", "/threads":
		return m.handleThread(fields)
	case "/approvals":
		m.openApprovalSelector()
		return m, nil
	case "/permissions", "/permission":
		return m.handlePermissions(fields)
	case "/approve":
		m.resolveApproval(fields, true)
		return m, nil
	case "/deny":
		m.resolveApproval(fields, false)
		return m, nil
	case "/tools":
		return m.handleTools(fields)
	case "/attach", "/attachments":
		return m.handleAttachments(fields)
	case "/tasks", "/task":
		return m.handleTasks(fields)
	case "/team":
		return m.handleTeam(fields)
	case "/agents", "/agent":
		return m.handleAgents(fields)
	case "/requirements", "/mcp", "/hooks", "/worldstate", "/compact":
		return m.handleExtension(fields)
	}
	return m.enqueueCommand(input)
}

// enqueueMetaCommand sends a runner-owned command without presenting it as a
// conversational user turn. Remote TUIs still need to round-trip through the
// worker that owns model state, but commands such as /model should not rename
// the thread or display a misleading model-generation placeholder.
func (m model) enqueueMetaCommand(text string) (tea.Model, tea.Cmd) {
	taskID, err := threadindex.NewID("task-")
	if err != nil {
		m.addSystem("could not create task id: " + err.Error())
		return m, nil
	}
	if strings.HasPrefix(text, "/models") {
		text = "/model" + strings.TrimPrefix(text, "/models")
	}
	part, err := protocol.NewTextPart(text)
	if err != nil {
		m.addSystem("could not create command message: " + err.Error())
		return m, nil
	}
	addr := protocol.MessageAddress{WorkspaceID: m.workspaceID, ThreadID: m.threadID, TaskID: taskID}
	message := protocol.ChatMessage{ID: "user-" + taskID, Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{part}}
	if err := m.scopeMessage(&addr, &message); err != nil {
		m.addSystem("could not scope command: " + err.Error())
		return m, nil
	}
	m.lastTaskID = taskID
	m.markTurn(taskID, m.threadID, "queued")
	m.refreshViewport()
	return m, sendMetaCommandCmd(m.ctx, m.channel, m.workspaceID, addr, message)
}

func (m model) enqueueCommand(text string) (tea.Model, tea.Cmd) {
	taskID, err := threadindex.NewID("task-")
	if err != nil {
		m.addSystem("could not create task id: " + err.Error())
		return m, nil
	}
	content, err := m.takeMessageContent(text)
	if err != nil {
		m.addSystem("could not create command message: " + err.Error())
		return m, nil
	}
	addr := protocol.MessageAddress{WorkspaceID: m.workspaceID, ThreadID: m.threadID, TaskID: taskID}
	message := protocol.ChatMessage{ID: "user-" + taskID, Role: protocol.RoleUser, Addr: addr, Content: content}
	if err := m.scopeMessage(&addr, &message); err != nil {
		m.addSystem("could not scope command: " + err.Error())
		return m, nil
	}
	m.lastTaskID = taskID
	m.markTurn(taskID, m.threadID, "queued")
	m.rows = append(m.rows, chatRow{Kind: rowUser, ID: message.ID, TaskID: taskID, Text: messageText(message)})
	m.addThinking(taskID)
	m.refreshViewportToBottom()
	return m, sendMessageCmd(m.ctx, m.channel, m.index, m.initialWorkspaceID, m.workspaceID, addr, message, text, nil)
}

func (m *model) cancelActive() {
	if !m.canCancelActive() {
		m.addSystem("no active task to cancel")
		return
	}
	if m.canceller.Cancel(m.lastTaskID) {
		delete(m.turns, m.lastTaskID)
		m.removeThinking(m.lastTaskID)
		m.addSystem("cancelled " + m.lastTaskID)
		return
	}
	m.addSystem("task not cancellable: " + m.lastTaskID)
}

func (m model) canCancelActive() bool {
	if m.lastTaskID == "" || m.canceller == nil {
		return false
	}
	activity, ok := m.turns[m.lastTaskID]
	return ok && m.activityOnCurrentThread(activity)
}

func (m *model) resolveApproval(fields []string, granted bool) {
	if m.approvals == nil {
		m.addSystem("approval broker is not wired")
		return
	}
	if len(fields) < 2 {
		m.addSystem("usage: " + fields[0] + " <request-id> [once|session|always]")
		return
	}
	req, ok := m.pendingApprovals[fields[1]]
	if !ok {
		m.addSystem("approval request not pending: " + fields[1])
		return
	}
	scope := "once"
	if len(fields) > 2 {
		scope = fields[2]
	}
	decision := approval.Decision{Granted: granted, Scope: scope}
	if !m.approvals.Resolve(req.TaskID, req.RequestID, decision) {
		m.addSystem("approval request was already resolved: " + req.RequestID)
		return
	}
	delete(m.pendingApprovals, req.RequestID)
	m.markTurn(req.TaskID, m.threadID, "thinking")
	if granted {
		m.addSystem("approved " + req.RequestID + " (" + scope + ")")
		return
	}
	m.addSystem("denied " + req.RequestID)
}

func (m model) matchThread(prefix string) (string, bool) {
	for _, session := range m.threads {
		if session.ID == prefix || strings.HasPrefix(session.ID, prefix) {
			return session.ID, true
		}
	}
	return "", false
}

func (m model) nextThreadAfterDelete(id string) string {
	for _, session := range m.threads {
		if session.ID != id {
			return session.ID
		}
	}
	return ""
}

func sendMessageCmd(
	ctx context.Context,
	ch ChannelSurface,
	index threadindex.Store,
	initialWorkspaceID string,
	storageWorkspaceID string,
	addr protocol.MessageAddress,
	message protocol.ChatMessage,
	text string,
	referencedImages []referencedImage,
) tea.Cmd {
	return func() tea.Msg {
		var err error
		message, err = appendReferencedImages(ctx, message, referencedImages)
		if err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("load image reference: %w", err)}
		}
		if err := touchWorkspaceThread(index, initialWorkspaceID, storageWorkspaceID, addr.ThreadID, text); err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("touch thread: %w", err)}
		}
		if err := ch.Enqueue(ctx, channel.Inbound{Addr: addr, Message: message}); err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("enqueue: %w", err)}
		}
		return sendResultMsg{WorkspaceID: storageWorkspaceID, Session: threadindex.Session{ID: addr.ThreadID}, TaskID: addr.TaskID}
	}
}

func sendMetaCommandCmd(ctx context.Context, ch ChannelSurface, storageWorkspaceID string, addr protocol.MessageAddress, message protocol.ChatMessage) tea.Cmd {
	return func() tea.Msg {
		if err := ch.Enqueue(ctx, channel.Inbound{Addr: addr, Message: message}); err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("enqueue: %w", err)}
		}
		return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID}
	}
}
