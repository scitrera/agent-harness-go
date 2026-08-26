package tui

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
)

func rowsFromHistory(messages []protocol.ChatMessage) []chatRow {
	return rowsFromHistoryWithReasoning(messages, false)
}

func rowsFromHistoryWithReasoning(messages []protocol.ChatMessage, retainReasoning bool) []chatRow {
	rows := make([]chatRow, 0, len(messages))
	for _, message := range messages {
		for _, row := range rawRowsForMessage(message) {
			if !retainReasoning && rowConsumesReasoning(row) {
				rows = removeReasoningRows(rows, row.TaskID)
			}
			rows = appendOrReplaceToolRow(rows, row)
		}
	}
	return rows
}

func rowsForMessage(message protocol.ChatMessage) []chatRow {
	rows := make([]chatRow, 0, len(message.Content)+1)
	for _, row := range rawRowsForMessage(message) {
		if rowConsumesReasoning(row) {
			rows = removeReasoningRows(rows, row.TaskID)
		}
		rows = appendOrReplaceToolRow(rows, row)
	}
	return rows
}

func rawRowsForMessage(message protocol.ChatMessage) []chatRow {
	switch message.Role {
	case protocol.RoleUser:
		if record, ok := shellcontext.FromMessage(message); ok {
			return []chatRow{{Kind: rowShell, ID: message.ID, TaskID: message.Addr.TaskID, Text: shellcontext.DisplayText(record)}}
		}
		return []chatRow{{Kind: rowUser, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message)}}
	case protocol.RoleTool, protocol.RoleToolResult:
		return toolRowsForMessage(message)
	case protocol.RoleSystem:
		return []chatRow{{Kind: rowSystem, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message)}}
	default:
		return rawAssistantRowsForMessage(message)
	}
}

func assistantPartRowID(id string, index int) string {
	if id == "" {
		id = "assistant"
	}
	return fmt.Sprintf("%s:%d", id, index)
}

func messageText(message protocol.ChatMessage) string {
	var out strings.Builder
	for _, part := range message.Content {
		if text, ok := part.AsText(); ok {
			out.WriteString(stripWorkingDirectoryContext(text.Text))
			continue
		}
		if image, ok := part.AsImage(); ok {
			name := strings.TrimSpace(image.AltText)
			if name == "" {
				name = "image"
			}
			out.WriteString("\n[image: ")
			out.WriteString(name)
			out.WriteString("]")
			continue
		}
		if file, ok := part.AsFile(); ok {
			name := strings.TrimSpace(file.FileName)
			if name == "" {
				name = "file"
			}
			out.WriteString("\n[file: ")
			out.WriteString(name)
			out.WriteString("]")
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

func rawAssistantRowsForMessage(message protocol.ChatMessage) []chatRow {
	rows := make([]chatRow, 0, len(message.Content)+1)
	for i, part := range message.Content {
		if reasoning, ok := reasoningPartText(part); ok {
			if strings.TrimSpace(reasoning.Text) != "" {
				rows = append(rows, chatRow{Kind: rowReasoning, ID: assistantPartRowID(message.ID, i), TaskID: message.Addr.TaskID, Text: reasoning.Text})
			}
			continue
		}
		if text, ok := part.AsText(); ok {
			if strings.TrimSpace(text.Text) != "" {
				rows = append(rows, chatRow{Kind: rowAssistant, ID: assistantPartRowID(message.ID, i), TaskID: message.Addr.TaskID, Text: text.Text})
			}
			continue
		}
		if row, ok := toolRowFromPart(message, part); ok {
			rows = appendOrReplaceToolRow(rows, row)
		}
	}
	if len(rows) == 0 {
		if text := messageText(message); text != "" {
			rows = append(rows, chatRow{Kind: rowAssistant, ID: message.ID, TaskID: message.Addr.TaskID, Text: text})
		}
	}
	if status := terminalStatusText(message); status != "" {
		rows = append(rows, chatRow{Kind: rowSystem, ID: terminalStatusRowID(message), TaskID: message.Addr.TaskID, Text: status})
	}
	return rows
}

func reasoningPartText(part protocol.ContentPart) (protocol.ReasoningPart, bool) {
	if part.Type() != protocol.ContentReasoning {
		return protocol.ReasoningPart{}, false
	}
	var reasoning protocol.ReasoningPart
	if err := part.Decode(&reasoning); err != nil {
		return protocol.ReasoningPart{}, false
	}
	return reasoning, true
}

func rowConsumesReasoning(row chatRow) bool {
	return row.Kind == rowAssistant || row.Kind == rowTool
}

func removeReasoningRows(rows []chatRow, taskID string) []chatRow {
	filtered := rows[:0]
	for _, row := range rows {
		if row.Kind == rowReasoning && row.TaskID == taskID {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func terminalStatusRowID(message protocol.ChatMessage) string {
	id := message.ID
	if id == "" {
		id = "assistant"
	}
	return id + ":terminal"
}

func terminalStatusText(message protocol.ChatMessage) string {
	if raw := message.Meta["error"]; len(raw) > 0 {
		var reason string
		if err := json.Unmarshal(raw, &reason); err != nil {
			reason = strings.Trim(strings.TrimSpace(string(raw)), `"`)
		}
		if reason == "" {
			reason = "unknown error"
		}
		return "Turn failed: " + reason
	}
	if raw := message.Meta["cancelled"]; len(raw) > 0 {
		var cancelled bool
		if err := json.Unmarshal(raw, &cancelled); err == nil && cancelled {
			return "Turn cancelled."
		}
	}
	return ""
}

func (m *model) upsertTerminalStatus(message protocol.ChatMessage) {
	text := terminalStatusText(message)
	if text == "" {
		return
	}
	id := terminalStatusRowID(message)
	for i := range m.rows {
		if m.rows[i].Kind == rowSystem && m.rows[i].ID == id {
			m.rows[i].Text = text
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowSystem, ID: id, TaskID: message.Addr.TaskID, Text: text})
}

func toolRowsForMessage(message protocol.ChatMessage) []chatRow {
	rows := make([]chatRow, 0, len(message.Content))
	for _, part := range message.Content {
		if row, ok := toolRowFromPart(message, part); ok {
			rows = appendOrReplaceToolRow(rows, row)
		}
	}
	if len(rows) > 0 {
		return rows
	}
	return []chatRow{{Kind: rowTool, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message), ToolCall: true}}
}

func appendOrReplaceToolRow(rows []chatRow, row chatRow) []chatRow {
	if row.Kind != rowTool || row.ID == "" {
		return append(rows, row)
	}
	for i := range rows {
		if rows[i].Kind == rowTool && rows[i].ID == row.ID {
			if strings.TrimSpace(row.Text) != "" {
				rows[i].Text = row.Text
			}
			rows[i].ToolCall = rows[i].ToolCall || row.ToolCall
			return rows
		}
	}
	return append(rows, row)
}

func (m *model) upsertToolPartRow(id, text, toolName string) {
	if id == "" {
		id = text
	}
	for i := range m.rows {
		if m.rows[i].Kind == rowTool && m.rows[i].ID == id {
			m.rows[i].ToolCall = true
			if isToolLifecycleText(m.rows[i].Text, toolName) {
				return
			}
			m.rows[i].Text = text
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowTool, ID: id, Text: text, ToolCall: true})
}

func isToolLifecycleText(text, toolName string) bool {
	if toolName == "" {
		return false
	}
	text = strings.TrimSpace(text)
	return strings.HasPrefix(text, "tool "+toolName+": ") ||
		(toolName == "spawn_subagent" && strings.HasPrefix(text, "Subagent "))
}

func toolRowFromPart(message protocol.ChatMessage, part protocol.ContentPart) (chatRow, bool) {
	if call, ok := part.AsToolCall(); ok {
		return chatRow{Kind: rowTool, ID: call.ID, TaskID: message.Addr.TaskID, Text: toolCallText(call), ToolCall: true}, true
	}
	if result, ok := part.AsToolResult(); ok {
		return chatRow{Kind: rowTool, ID: result.CallID, TaskID: message.Addr.TaskID, Text: toolResultText(result), ToolCall: true}, true
	}
	if approval, ok := part.AsApprovalRequest(); ok {
		return chatRow{Kind: rowTool, ID: approval.ID, TaskID: message.Addr.TaskID, Text: approvalText(approval.Tool, string(approval.Status))}, true
	}
	return chatRow{}, false
}

func toolCallText(call protocol.ToolCallPartBody) string {
	return "tool call " + call.Name
}

func toolResultText(result protocol.ToolResultPartBody) string {
	label := "tool result " + result.Name
	if result.IsError {
		label += " failed"
	}
	return label
}

func approvalText(tool, status string) string {
	return "approval " + tool + ": " + status
}
