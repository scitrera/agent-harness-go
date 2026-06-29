package tui

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func rowsFromHistory(messages []protocol.ChatMessage) []chatRow {
	rows := make([]chatRow, 0, len(messages))
	for _, message := range messages {
		rows = append(rows, rowFromMessage(message))
	}
	return rows
}

func rowFromMessage(message protocol.ChatMessage) chatRow {
	kind := rowAssistant
	switch message.Role {
	case protocol.RoleUser:
		kind = rowUser
	case protocol.RoleTool, protocol.RoleToolResult:
		kind = rowTool
	case protocol.RoleSystem:
		kind = rowSystem
	}
	return chatRow{Kind: kind, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message)}
}

func (m *model) applyEvent(event channel.Event) {
	if event.Addr.ThreadID != "" && event.Addr.ThreadID != m.threadID {
		m.applyBackgroundEvent(event)
		return
	}
	switch event.Type {
	case channel.EventMessageStarted:
		if event.Message != nil {
			m.upsertAssistant(event.Message.ID, "", true)
		}
	case channel.EventTokenDelta:
		m.appendAssistantDelta(event.MessageID, event.Delta)
	case channel.EventPartAppended:
		m.applyPartAppended(event)
	case channel.EventPartUpdated:
		m.applyPartUpdated(event)
	case channel.EventMessageFinal:
		if event.Message != nil {
			m.upsertAssistant(event.Message.ID, messageText(*event.Message), false)
		}
	case channel.EventToolLifecycle:
		m.applyToolLifecycle(event)
	case channel.EventError:
		m.addSystem("turn error")
	}
	m.refreshViewport()
}

func (m *model) applyBackgroundEvent(event channel.Event) {
	if event.Type == channel.EventToolLifecycle {
		m.recordToolEvent(event)
	}
}

func (m *model) applyPartAppended(event channel.Event) {
	if event.Part == nil {
		return
	}
	part := *event.Part
	if approval, ok := part.AsApprovalRequest(); ok {
		m.recordApproval(event.Addr.TaskID, approval.ID, approval.Tool, string(approval.Status), approval.Reason)
		m.upsertToolishRow(approval.ID, "approval "+approval.Tool+": "+string(approval.Status))
		return
	}
	if text, ok := part.AsText(); ok {
		if text.Text != "" {
			m.appendAssistantDelta(event.MessageID, text.Text)
		}
		return
	}
	if call, ok := part.AsToolCall(); ok {
		m.upsertToolishRow(call.ID, "tool call "+call.Name)
		return
	}
	if result, ok := part.AsToolResult(); ok {
		label := "tool result " + result.Name
		if result.IsError {
			label += " failed"
		}
		m.upsertToolishRow(result.CallID, label)
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
		m.upsertToolishRow(id, "approval "+req.Tool+": "+status)
		return
	}
	delete(m.pendingApprovals, id)
	m.upsertToolishRow(id, "approval "+req.Tool+": "+status)
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
	line := renderToolEvent(entry.Event)
	m.upsertToolishRow(entry.Event.CallID, line)
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
	return entry, true
}

func (m *model) recordApproval(taskID, requestID, tool, status, reason string) {
	if status == "" || status == "pending" {
		m.pendingApprovals[requestID] = approvalRequest{RequestID: requestID, TaskID: taskID, Tool: tool, Status: "pending", Reason: reason}
		return
	}
	delete(m.pendingApprovals, requestID)
}

func (m *model) upsertAssistant(id, text string, streaming bool) {
	if id == "" {
		id = "assistant"
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowAssistant && m.rows[i].ID == id {
			if text != "" || !streaming {
				m.rows[i].Text = text
			}
			m.rows[i].Streaming = streaming
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, ID: id, Text: text, Streaming: streaming})
}

func (m *model) appendAssistantDelta(id, delta string) {
	if delta == "" {
		return
	}
	if id == "" {
		id = "assistant"
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowAssistant && m.rows[i].ID == id {
			m.rows[i].Text += delta
			m.rows[i].Streaming = true
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, ID: id, Text: delta, Streaming: true})
}

func (m *model) upsertToolishRow(id, text string) {
	if id == "" {
		id = text
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowTool && m.rows[i].ID == id {
			m.rows[i].Text = text
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowTool, ID: id, Text: text})
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
		parts = append(parts, "error="+event.ErrorCode)
	}
	if len(event.Result.FileChanges) > 0 {
		parts = append(parts, fmt.Sprintf("files=%d", len(event.Result.FileChanges)))
	}
	return strings.Join(parts, " ")
}

func messageText(message protocol.ChatMessage) string {
	var out strings.Builder
	for _, part := range message.Content {
		if text, ok := part.AsText(); ok {
			out.WriteString(text.Text)
			continue
		}
		if approval, ok := part.AsApprovalRequest(); ok {
			out.WriteString("\napproval ")
			out.WriteString(approval.Tool)
			out.WriteString(": ")
			out.WriteString(string(approval.Status))
		}
	}
	return strings.TrimSpace(out.String())
}
