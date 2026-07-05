package tui

import (
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func rowsFromHistory(messages []protocol.ChatMessage) []chatRow {
	rows := make([]chatRow, 0, len(messages))
	for _, message := range messages {
		for _, row := range rowsForMessage(message) {
			rows = appendOrReplaceToolRow(rows, row)
		}
	}
	return rows
}

func rowsForMessage(message protocol.ChatMessage) []chatRow {
	switch message.Role {
	case protocol.RoleUser:
		return []chatRow{{Kind: rowUser, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message)}}
	case protocol.RoleTool, protocol.RoleToolResult:
		return toolRowsForMessage(message)
	case protocol.RoleSystem:
		return []chatRow{{Kind: rowSystem, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message)}}
	default:
		return assistantRowsForMessage(message)
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

func assistantRowsForMessage(message protocol.ChatMessage) []chatRow {
	rows := make([]chatRow, 0, len(message.Content))
	for i, part := range message.Content {
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
	if len(rows) > 0 {
		return rows
	}
	text := messageText(message)
	if text == "" {
		return nil
	}
	return []chatRow{{Kind: rowAssistant, ID: message.ID, TaskID: message.Addr.TaskID, Text: text}}
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
	return []chatRow{{Kind: rowTool, ID: message.ID, TaskID: message.Addr.TaskID, Text: messageText(message)}}
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
			if isToolLifecycleText(m.rows[i].Text, toolName) {
				return
			}
			m.rows[i].Text = text
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowTool, ID: id, Text: text})
}

func isToolLifecycleText(text, toolName string) bool {
	if toolName == "" {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(text), "tool "+toolName+": ")
}

func toolRowFromPart(message protocol.ChatMessage, part protocol.ContentPart) (chatRow, bool) {
	if call, ok := part.AsToolCall(); ok {
		return chatRow{Kind: rowTool, ID: call.ID, TaskID: message.Addr.TaskID, Text: toolCallText(call)}, true
	}
	if result, ok := part.AsToolResult(); ok {
		return chatRow{Kind: rowTool, ID: result.CallID, TaskID: message.Addr.TaskID, Text: toolResultText(result)}, true
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
