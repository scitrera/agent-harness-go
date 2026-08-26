package tui

import (
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/approval"
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
		return m, m.cancelActive()
	case "/status":
		m.addSystem(m.statusSummary())
		return m, nil
	case "/shell-response":
		return m.handleShellResponse(fields), nil
	case "/model", "/models", "/schedules", "/runs", "/refinements", "/ledger":
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

func (m model) handleShellResponse(fields []string) tea.Model {
	if len(fields) == 1 || strings.EqualFold(fields[1], "status") {
		m.addSystem(m.shellResponseSummary())
		return m
	}
	if m.shellPreferences == nil {
		m.addSystem("shell response preferences are not writable; use --tui-shell-trigger-agent for the global default")
		return m
	}
	scope := "thread"
	valueIndex := 1
	if strings.EqualFold(fields[1], "user") {
		scope = "user"
		valueIndex = 2
	}
	if len(fields) <= valueIndex {
		m.addSystem("usage: /shell-response [status|on|off|default|user on|off|default]")
		return m
	}
	value, ok := shellPreferenceValue(fields[valueIndex])
	if !ok || len(fields) != valueIndex+1 {
		m.addSystem("usage: /shell-response [status|on|off|default|user on|off|default]")
		return m
	}
	var err error
	if scope == "user" {
		err = m.shellPreferences.SetUser(m.userID, value)
	} else {
		err = m.shellPreferences.SetThread(m.userID, m.workspaceID, m.threadID, value)
	}
	if err != nil {
		m.addSystem("shell response preference failed: " + err.Error())
		return m
	}
	m.addSystem(m.shellResponseSummary())
	return m
}

func shellPreferenceValue(value string) (*bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on", "true", "yes":
		return boolPointer(true), true
	case "off", "false", "no":
		return boolPointer(false), true
	case "default", "inherit":
		return nil, true
	default:
		return nil, false
	}
}

func (m model) shellResponseSummary() string {
	resolution := m.resolveShellPreference(m.workspaceID, m.threadID)
	return "idle shell response: " + onOff(resolution.Effective) +
		" (source=" + resolution.Source +
		", global=" + onOff(resolution.Global) +
		", user=" + optionalOnOff(resolution.User) +
		", thread=" + optionalOnOff(resolution.Thread) + ")"
}

func onOff(value bool) string {
	if value {
		return "on"
	}
	return "off"
}

func optionalOnOff(value *bool) string {
	if value == nil {
		return "default"
	}
	return onOff(*value)
}

// enqueueMetaCommand sends a runner-owned command without presenting it as a
// conversational user turn. Remote TUIs still need to round-trip through the
// worker that owns model state, but commands such as /model should not rename
// the thread or display a misleading model-generation placeholder.
func (m model) enqueueMetaCommand(text string) (tea.Model, tea.Cmd) {
	pending, err := m.prepareQueuedMessage(outboundMetaCommand, text, nil)
	if err != nil {
		m.addSystem("could not prepare command: " + err.Error())
		return m, nil
	}
	return m.submitPreparedMessage(pending)
}

func (m model) enqueueCommand(text string) (tea.Model, tea.Cmd) {
	pending, err := m.prepareQueuedMessage(outboundCommand, text, m.attachments)
	if err != nil {
		m.addSystem("could not prepare command: " + err.Error())
		return m, nil
	}
	m.attachments = nil
	m.updateAttachmentPlaceholder()
	return m.submitPreparedMessage(pending)
}

func (m *model) cancelActive() tea.Cmd {
	taskID := m.activeTaskID()
	if taskID == "" || m.canceller == nil {
		m.addSystem("no active task to cancel")
		return nil
	}
	if m.tryCancelTask(taskID) {
		return m.dispatchNextPending()
	}
	if m.cancelPendingID != taskID {
		m.cancelPendingID = taskID
		m.status = "cancellation requested"
		m.refreshViewport()
	}
	return nil
}

func (m model) canCancelActive() bool {
	return m.canceller != nil && m.activeTaskID() != ""
}

func (m model) activeTaskID() string {
	if activity, ok := m.turns[m.lastTaskID]; ok && m.activityOnCurrentThread(activity) {
		return m.lastTaskID
	}
	for taskID, activity := range m.turns {
		if m.activityOnCurrentThread(activity) {
			return taskID
		}
	}
	return ""
}

func (m *model) tryCancelTask(taskID string) bool {
	if taskID == "" || m.canceller == nil || !m.canceller.Cancel(taskID) {
		return false
	}
	delete(m.turns, taskID)
	m.removeThinking(taskID)
	if m.cancelPendingID == taskID {
		m.cancelPendingID = ""
	}
	m.addSystem("cancelled " + taskID)
	return true
}

func (m *model) retryPendingCancel(taskID string) tea.Cmd {
	if taskID == "" || m.cancelPendingID != taskID {
		return nil
	}
	if _, active := m.turns[taskID]; !active {
		m.cancelPendingID = ""
		return nil
	}
	if !m.tryCancelTask(taskID) {
		return nil
	}
	return m.dispatchNextPending()
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
