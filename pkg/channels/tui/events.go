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

func (m *model) applyEvent(event channel.Event) {
	if event.Addr.ThreadID != "" && event.Addr.ThreadID != m.threadID {
		m.applyBackgroundEvent(event)
		return
	}
	switch event.Type {
	case channel.EventMessageStarted:
		return
	case channel.EventTokenDelta:
		m.appendAssistantDelta(event.MessageID, event.Index, event.Delta)
	case channel.EventPartAppended:
		m.applyPartAppended(event)
	case channel.EventPartUpdated:
		m.applyPartUpdated(event)
	case channel.EventMessageFinal:
		if event.Message != nil {
			m.applyFinalMessage(*event.Message)
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
		m.upsertAssistantPart(event.MessageID, event.Index, text.Text, true)
		return
	}
	if call, ok := part.AsToolCall(); ok {
		m.upsertToolPartRow(call.ID, toolCallText(call), call.Name)
		return
	}
	if result, ok := part.AsToolResult(); ok {
		m.upsertToolPartRow(result.CallID, toolResultText(result), result.Name)
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

func (m *model) upsertAssistantPart(id string, index int, text string, streaming bool) {
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
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, ID: rowID, TaskID: "", Text: text, Streaming: streaming})
}

func (m *model) appendAssistantDelta(id string, index int, delta string) {
	if delta == "" {
		return
	}
	rowID := assistantPartRowID(id, index)
	for i := range m.rows {
		if m.rows[i].Kind == rowAssistant && m.rows[i].ID == rowID {
			m.rows[i].Text += delta
			m.rows[i].Streaming = true
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowAssistant, ID: rowID, Text: delta, Streaming: true})
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

func (m *model) ensureToolishRow(id, text string) {
	if id == "" {
		id = text
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowTool && m.rows[i].ID == id {
			if strings.TrimSpace(m.rows[i].Text) == "" {
				m.rows[i].Text = text
			}
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowTool, ID: id, Text: text})
}

func (m *model) applyFinalMessage(message protocol.ChatMessage) {
	for i, part := range message.Content {
		if text, ok := part.AsText(); ok {
			m.upsertAssistantPart(message.ID, i, text.Text, false)
			continue
		}
		if call, ok := part.AsToolCall(); ok {
			m.ensureToolishRow(call.ID, toolCallText(call))
			continue
		}
		if result, ok := part.AsToolResult(); ok {
			m.ensureToolishRow(result.CallID, toolResultText(result))
			continue
		}
		if approval, ok := part.AsApprovalRequest(); ok {
			m.ensureToolishRow(approval.ID, approvalText(approval.Tool, string(approval.Status)))
		}
	}
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
