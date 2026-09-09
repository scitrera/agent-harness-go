// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func (m *model) applyEvent(event channel.Event) {
	if event.Type == channel.EventMessageFinal && event.Message != nil {
		m.observeMessageModel(event.Addr, *event.Message)
	}
	eventWorkspaceID := event.Addr.WorkspaceID
	if eventWorkspaceID == "" {
		eventWorkspaceID = m.workspaceID
	}
	if !m.workspaceMatchesCurrent(eventWorkspaceID) {
		m.applyForeignWorkspaceEvent(event, eventWorkspaceID)
		return
	}
	if event.Addr.ThreadID != "" && event.Addr.ThreadID != m.threadID {
		m.applyBackgroundEvent(event)
		return
	}
	switch event.Type {
	case channel.EventMessageStarted:
		if event.Message != nil && shellcontext.IsCommitAck(*event.Message) {
			m.markTurnAt(event.Addr.TaskID, eventWorkspaceID, event.Addr.ThreadID, "saving context")
		} else {
			m.markTurnAt(event.Addr.TaskID, eventWorkspaceID, event.Addr.ThreadID, "thinking")
		}
	case channel.EventTokenDelta:
		kind := m.appendAssistantDelta(event.MessageID, event.Index, event.Addr.TaskID, event.Delta)
		if kind == rowReasoning {
			m.markTurnAt(event.Addr.TaskID, eventWorkspaceID, event.Addr.ThreadID, "reasoning")
		} else {
			m.consumeReasoning(event.Addr.TaskID)
			m.markTurnAt(event.Addr.TaskID, eventWorkspaceID, event.Addr.ThreadID, "responding")
		}
	case channel.EventPartAppended:
		m.applyPartAppended(event)
	case channel.EventPartUpdated:
		m.applyPartUpdated(event)
	case channel.EventMessageFinal:
		if event.Message != nil {
			m.applyFinalMessage(*event.Message)
		}
		m.finishTurn(event.Addr.TaskID)
	case channel.EventToolLifecycle:
		m.applyToolLifecycle(event)
	case channel.EventMemoryRecall:
		m.applyMemoryRecall(event)
	case channel.EventError:
		delete(m.turns, event.Addr.TaskID)
		m.status = "turn error"
		m.addSystem("turn error")
	}
	m.syncThinking(event.Addr.TaskID)
	m.refreshViewport()
}

func (m *model) applyMemoryRecall(event channel.Event) {
	var status channel.MemoryRecallStatus
	if json.Unmarshal(event.Payload, &status) != nil {
		return
	}
	provider := strings.TrimSpace(status.Provider)
	if provider == "" {
		provider = "memory"
	}
	var text string
	switch status.ResultCode {
	case "error":
		text = fmt.Sprintf("%s recall unavailable", provider)
	case "empty":
		text = fmt.Sprintf("%s recall: no items", provider)
	default:
		text = fmt.Sprintf("%s recall: %d item(s) in %dms", provider, status.ItemCount, status.LatencyMS)
	}
	m.addSystem(text)
}

func (m *model) applyForeignWorkspaceEvent(event channel.Event, workspaceID string) {
	switch event.Type {
	case channel.EventMessageStarted:
		m.markTurnAt(event.Addr.TaskID, workspaceID, event.Addr.ThreadID, "thinking")
	case channel.EventTokenDelta:
		m.markTurnAt(event.Addr.TaskID, workspaceID, event.Addr.ThreadID, "responding")
	case channel.EventMessageFinal, channel.EventError:
		m.finishTurn(event.Addr.TaskID)
	}
}

func (m *model) applyBackgroundEvent(event channel.Event) {
	// A command may finish after the user switches threads. Preserve lifecycle
	// bookkeeping for a real turn on that background thread without confusing
	// child-thread subagent events (whose task activity belongs to the parent).
	if activity, ok := m.turns[event.Addr.TaskID]; ok && activity.ThreadID == event.Addr.ThreadID {
		switch event.Type {
		case channel.EventMessageStarted:
			phase := "thinking"
			if event.Message != nil && shellcontext.IsCommitAck(*event.Message) {
				phase = "saving context"
			}
			m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, phase)
		case channel.EventTokenDelta:
			m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "responding")
		case channel.EventMessageFinal, channel.EventError:
			m.finishTurn(event.Addr.TaskID)
		}
	}
	var childTool *tools.ToolEvent
	if event.Type == channel.EventToolLifecycle {
		if entry, ok := m.recordToolEvent(event); ok {
			childTool = &entry.Event
		}
	}
	m.applySubagentChildEvent(event, childTool)
	m.refreshViewport()
}

func (m *model) applyPartAppended(event channel.Event) {
	if event.Part == nil {
		return
	}
	part := *event.Part
	if reasoning, ok := reasoningPartText(part); ok {
		m.upsertReasoningPart(event.MessageID, event.Index, event.Addr.TaskID, reasoning.Text, true)
		m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "reasoning")
		return
	}
	// Reasoning describes the decision leading to the next visible action. Once
	// that action arrives it is no longer the current transcript item unless the
	// user explicitly opted into retaining traces.
	m.consumeReasoning(event.Addr.TaskID)
	if approval, ok := part.AsApprovalRequest(); ok {
		m.recordApproval(event.Addr.TaskID, approval.ID, approval.Tool, string(approval.Status), approval.Reason)
		m.refreshApprovalSelector()
		m.upsertToolishRow(approval.ID, "approval "+approval.Tool+": "+string(approval.Status), false)
		if approval.Status == "" || approval.Status == "pending" {
			m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "approval needed")
		}
		return
	}
	if text, ok := part.AsText(); ok {
		m.upsertAssistantPart(event.MessageID, event.Index, event.Addr.TaskID, text.Text, true)
		m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "responding")
		return
	}
	if call, ok := part.AsToolCall(); ok {
		m.upsertToolPartRow(call.ID, toolCallText(call), call.Name)
		return
	}
	if result, ok := part.AsToolResult(); ok {
		m.upsertToolPartRow(result.CallID, toolResultText(result), result.Name)
		return
	}
	if sub, ok := part.AsSubagent(); ok {
		m.applySubagentPart(event.Addr.TaskID, sub)
	}
}

func (m *model) applyPartUpdated(event channel.Event) {
	id := patchString(event.Patch, "id")
	status := patchString(event.Patch, "status")
	if id == "" || status == "" {
		return
	}
	req := m.pendingApprovals[id]
	if req.RequestID == "" {
		req = approvalRequest{RequestID: id, TaskID: event.Addr.TaskID}
	}
	req.Status = status
	if tool := patchString(event.Patch, "tool"); tool != "" {
		req.Tool = tool
	}
	if reason := patchString(event.Patch, "reason"); reason != "" {
		req.Reason = reason
	}
	if status == "pending" {
		m.pendingApprovals[id] = req
		m.refreshApprovalSelector()
		m.upsertToolishRow(id, "approval "+req.Tool+": "+status, false)
		return
	}
	delete(m.pendingApprovals, id)
	m.refreshApprovalSelector()
	m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "thinking")
	m.upsertToolishRow(id, "approval "+req.Tool+": "+status, false)
}

func patchString(patch map[string]json.RawMessage, key string) string {
	raw := patch[key]
	if len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return value
}

func (m *model) applyToolLifecycle(event channel.Event) {
	entry, ok := m.recordToolEvent(event)
	if !ok {
		return
	}
	switch entry.Event.Status {
	case tools.ToolEventQueued, tools.ToolEventStarted:
		m.consumeReasoning(event.Addr.TaskID)
		phase := "tool " + entry.Event.ToolName
		if entry.Event.ToolName == tools.SubagentToolName {
			phase = "subagent working"
		}
		m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, phase)
	case tools.ToolEventFinished:
		m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "thinking")
	case tools.ToolEventAborted:
		m.markTurnAt(event.Addr.TaskID, event.Addr.WorkspaceID, event.Addr.ThreadID, "tool failed")
	}
	line := renderToolEvent(entry.Event)
	if entry.Event.ToolName == tools.SubagentToolName {
		line = m.applySubagentLifecycle(event.Addr.TaskID, entry.Event)
	}
	m.upsertToolishRow(entry.Event.CallID, line, true)
}

func (m *model) recordToolEvent(event channel.Event) (toolEntry, bool) {
	if len(event.Payload) == 0 {
		return toolEntry{}, false
	}
	var toolEvent tools.ToolEvent
	if err := json.Unmarshal(event.Payload, &toolEvent); err != nil {
		return toolEntry{}, false
	}
	entry := toolEntry{Event: toolEvent, Seen: time.Now().UnixMilli()}
	m.tools[toolEvent.CallID] = entry
	m.refreshToolSelector()
	m.refreshOpenToolDrawer(toolEvent.CallID)
	return entry, true
}

func (m *model) recordApproval(taskID, requestID, tool, status, reason string) {
	if status == "" || status == "pending" {
		m.pendingApprovals[requestID] = approvalRequest{RequestID: requestID, TaskID: taskID, Tool: tool, Status: "pending", Reason: reason}
		return
	}
	delete(m.pendingApprovals, requestID)
}

func (m *model) upsertAssistantPart(id string, index int, taskID, text string, streaming bool) {
	rowID := assistantPartRowID(id, index)
	if text == "" && !streaming {
		return
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowAssistant && m.rows[i].ID == rowID {
			if text != "" || !streaming {
				m.rows[i].Text = text
			}
			m.rows[i].Streaming = streaming
			if m.rows[i].TaskID == "" {
				m.rows[i].TaskID = taskID
			}
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, ID: rowID, TaskID: taskID, Text: text, Streaming: streaming})
}

func (m *model) upsertReasoningPart(id string, index int, taskID, text string, streaming bool) {
	rowID := assistantPartRowID(id, index)
	if text == "" && !streaming {
		return
	}
	for i := range m.rows {
		if m.rows[i].ID != rowID || (m.rows[i].Kind != rowReasoning && m.rows[i].Kind != rowAssistant) {
			continue
		}
		m.rows[i].Kind = rowReasoning
		if text != "" || !streaming {
			m.rows[i].Text = text
		}
		m.rows[i].Streaming = streaming
		m.rows[i].TaskID = taskID
		return
	}
	m.rows = append(m.rows, chatRow{Kind: rowReasoning, ID: rowID, TaskID: taskID, Text: text, Streaming: streaming})
}

func (m *model) appendAssistantDelta(id string, index int, taskID, delta string) rowKind {
	rowID := assistantPartRowID(id, index)
	for i := range m.rows {
		if (m.rows[i].Kind == rowAssistant || m.rows[i].Kind == rowReasoning) && m.rows[i].ID == rowID {
			if delta == "" {
				return m.rows[i].Kind
			}
			m.rows[i].Text += delta
			m.rows[i].Streaming = true
			if m.rows[i].TaskID == "" {
				m.rows[i].TaskID = taskID
			}
			return m.rows[i].Kind
		}
	}
	if delta == "" {
		return rowAssistant
	}
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, ID: rowID, TaskID: taskID, Text: delta, Streaming: true})
	return rowAssistant
}

func (m *model) consumeReasoning(taskID string) {
	if m.retainReasoning {
		return
	}
	m.rows = removeReasoningRows(m.rows, taskID)
}

func (m *model) upsertToolishRow(id, text string, toolCall bool) {
	if id == "" {
		id = text
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowTool && m.rows[i].ID == id {
			m.rows[i].Text = text
			m.rows[i].ToolCall = m.rows[i].ToolCall || toolCall
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowTool, ID: id, Text: text, ToolCall: toolCall})
}

func (m *model) ensureToolishRow(id, text string, toolCall bool) {
	if id == "" {
		id = text
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowTool && m.rows[i].ID == id {
			m.rows[i].ToolCall = m.rows[i].ToolCall || toolCall
			if strings.TrimSpace(m.rows[i].Text) == "" {
				m.rows[i].Text = text
			}
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowTool, ID: id, Text: text, ToolCall: toolCall})
}

func (m *model) applyFinalMessage(message protocol.ChatMessage) {
	for i, part := range message.Content {
		if reasoning, ok := reasoningPartText(part); ok {
			m.upsertReasoningPart(message.ID, i, message.Addr.TaskID, reasoning.Text, false)
			continue
		}
		m.consumeReasoning(message.Addr.TaskID)
		if text, ok := part.AsText(); ok {
			m.upsertAssistantPart(message.ID, i, message.Addr.TaskID, text.Text, false)
			continue
		}
		if call, ok := part.AsToolCall(); ok {
			m.ensureToolishRow(call.ID, toolCallText(call), true)
			continue
		}
		if result, ok := part.AsToolResult(); ok {
			m.ensureToolishRow(result.CallID, toolResultText(result), true)
			continue
		}
		if approval, ok := part.AsApprovalRequest(); ok {
			m.ensureToolishRow(approval.ID, approvalText(approval.Tool, string(approval.Status)), false)
			continue
		}
		if sub, ok := part.AsSubagent(); ok {
			m.applySubagentPart(message.Addr.TaskID, sub)
		}
	}
	m.upsertTerminalStatus(message)
}

func renderToolEvent(event tools.ToolEvent) string {
	parts := []string{fmt.Sprintf("tool %s: %s", event.ToolName, event.Status)}
	if event.DurationMS > 0 {
		parts = append(parts, fmt.Sprintf("%dms", event.DurationMS))
	}
	if event.PolicyDecision != "" {
		parts = append(parts, "policy="+event.PolicyDecision)
	}
	if event.ApprovalDecision != "" {
		parts = append(parts, "approval="+event.ApprovalDecision)
	}
	if event.ErrorCode != "" {
		errorText := "error=" + event.ErrorCode
		if event.ErrorMessage != "" {
			errorText += " (" + event.ErrorMessage + ")"
		}
		parts = append(parts, errorText)
	}
	if event.Result.ExitCode != 0 {
		parts = append(parts, fmt.Sprintf("exit=%d", event.Result.ExitCode))
	}
	if len(event.Result.FileChanges) > 0 {
		parts = append(parts, fmt.Sprintf("files=%d", len(event.Result.FileChanges)))
	}
	return strings.Join(parts, " ")
}
