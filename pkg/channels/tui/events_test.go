package tui

import (
	"encoding/json"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestModelApplyEvent_whenToolLifecycleArrives(t *testing.T) {
	// Given
	m := model{threadID: "t1", tools: map[string]toolEntry{}, pendingApprovals: map[string]approvalRequest{}, viewport: viewport.New()}
	event := tools.ToolEvent{Status: tools.ToolEventStarted, CallID: "call-1", ToolName: "shell"}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// When
	m.applyEvent(channel.Event{Type: channel.EventToolLifecycle, Addr: protocol.MessageAddress{ThreadID: "t1"}, Payload: payload})

	// Then
	if _, ok := m.tools["call-1"]; !ok {
		t.Fatalf("tool event not recorded: %+v", m.tools)
	}
	if len(m.rows) != 1 || m.rows[0].Kind != rowTool {
		t.Fatalf("tool row not rendered: %+v", m.rows)
	}
}

func TestRenderToolEventIncludesSafeFailureReason(t *testing.T) {
	got := renderToolEvent(tools.ToolEvent{
		Status:       tools.ToolEventFinished,
		ToolName:     "shell",
		ErrorCode:    "command_failed",
		ErrorMessage: "command executable not found",
		Result:       tools.ResultMetadata{ExitCode: 127},
	})
	if !strings.Contains(got, "error=command_failed (command executable not found)") {
		t.Fatalf("tool failure row = %q", got)
	}
	if !strings.Contains(got, "exit=127") {
		t.Fatalf("tool failure row omits exit code: %q", got)
	}
}

func TestFinalFailureMessageSurfacesReason(t *testing.T) {
	message := protocol.ChatMessage{
		ID:   "assistant-1",
		Role: protocol.RoleAssistant,
		Addr: protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
		Meta: map[string]json.RawMessage{"error": json.RawMessage(`"provider returned 404"`)},
	}

	m := model{threadID: "t1", turns: map[string]turnActivity{"task-1": {ThreadID: "t1"}}, viewport: viewport.New()}
	m.applyEvent(channel.Event{Type: channel.EventMessageFinal, Addr: message.Addr, Message: &message})

	if len(m.rows) != 1 || m.rows[0].Kind != rowSystem || m.rows[0].Text != "Turn failed: provider returned 404" {
		t.Fatalf("failure rows = %+v", m.rows)
	}
	if _, ok := m.turns["task-1"]; ok {
		t.Fatal("failed turn remained active")
	}
}

func TestRowsFromHistorySurfacesCancelledTurn(t *testing.T) {
	message := protocol.ChatMessage{
		ID:   "assistant-1",
		Role: protocol.RoleAssistant,
		Addr: protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
		Meta: map[string]json.RawMessage{"cancelled": json.RawMessage("true")},
	}

	rows := rowsFromHistory([]protocol.ChatMessage{message})
	if len(rows) != 1 || rows[0].Kind != rowSystem || rows[0].Text != "Turn cancelled." {
		t.Fatalf("cancelled history rows = %+v", rows)
	}
}

func TestModelApplyPartUpdated_whenApprovalResolves(t *testing.T) {
	// Given
	m := model{threadID: "t1", pendingApprovals: map[string]approvalRequest{"call-1": {RequestID: "call-1", TaskID: "task-1", Tool: "shell", Status: "pending"}}, viewport: viewport.New()}
	patch := map[string]json.RawMessage{
		"id":     json.RawMessage(`"call-1"`),
		"tool":   json.RawMessage(`"shell"`),
		"status": json.RawMessage(`"approved"`),
	}

	// When
	m.applyEvent(channel.Event{Type: channel.EventPartUpdated, Addr: protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"}, Patch: patch})

	// Then
	if len(m.pendingApprovals) != 0 {
		t.Fatalf("approval should be resolved: %+v", m.pendingApprovals)
	}
}

func TestModelApplyEvent_keepsAssistantResponseBelowTopLineBeforeResize(t *testing.T) {
	// Given
	m := model{
		channel:          NewChannel(),
		threadID:         "t1",
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		viewport:         viewport.New(),
		composer:         newComposer(),
		tailing:          true,
	}

	// When
	m.applyEvent(channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      protocol.MessageAddress{ThreadID: "t1"},
		MessageID: "assistant-1",
		Delta:     "hello from agent",
	})
	view := m.View()

	// Then
	if !strings.Contains(m.viewport.View(), "hello from agent") {
		t.Fatalf("assistant response should render in viewport before resize: %q", m.viewport.View())
	}
	if view.Cursor == nil {
		t.Fatal("composer cursor should still be exported")
	}
	if view.Cursor.Y == 0 {
		t.Fatalf("cursor on top row will make terminal output overwrite transcript: %+v", view.Cursor.Position)
	}
}

func TestModelApplyEvent_keepsAssistantTextRowsAroundToolRows(t *testing.T) {
	// Given
	m := model{
		channel:          NewChannel(),
		threadID:         "t1",
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		viewport:         viewport.New(),
		composer:         newComposer(),
		tailing:          true,
	}
	messageID := "assistant-turn"
	toolPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell", Args: protocol.RawToArgs(json.RawMessage(`{"cmd":"pwd"}`))})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	resultPart, err := protocol.NewToolResultPart("call-1", "shell", json.RawMessage(`{"stdout":"/tmp"}`), false)
	if err != nil {
		t.Fatalf("tool result part: %v", err)
	}
	textPart, err := protocol.NewTextPart("")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}

	// When
	m.applyEvent(channel.Event{Type: channel.EventMessageStarted, Addr: protocol.MessageAddress{ThreadID: "t1"}, Message: &protocol.ChatMessage{ID: messageID, Role: protocol.RoleAssistant}})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: messageID, Index: 0, Part: &textPart})
	m.applyEvent(channel.Event{Type: channel.EventTokenDelta, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: messageID, Index: 0, Delta: "I will check."})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: messageID, Index: 1, Part: &toolPart})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: messageID, Index: 2, Part: &resultPart})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: messageID, Index: 3, Part: &textPart})
	m.applyEvent(channel.Event{Type: channel.EventTokenDelta, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: messageID, Index: 3, Delta: "Done."})

	// Then
	if len(m.rows) != 3 {
		t.Fatalf("row count = %d, want 3: %+v", len(m.rows), m.rows)
	}
	if m.rows[0].Kind != rowAssistant || m.rows[0].Text != "I will check." {
		t.Fatalf("first row = %+v, want first assistant text", m.rows[0])
	}
	if m.rows[1].Kind != rowTool || !strings.Contains(m.rows[1].Text, "tool result shell") {
		t.Fatalf("second row = %+v, want tool result", m.rows[1])
	}
	if m.rows[2].Kind != rowAssistant || m.rows[2].Text != "Done." {
		t.Fatalf("third row = %+v, want trailing assistant text", m.rows[2])
	}
}

func TestRowsFromHistory_keepsAssistantTextAroundStableToolRows(t *testing.T) {
	// Given
	before, err := protocol.NewTextPart("I will check.")
	if err != nil {
		t.Fatalf("before text: %v", err)
	}
	call, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell", Args: protocol.RawToArgs(json.RawMessage(`{"cmd":"pwd"}`))})
	if err != nil {
		t.Fatalf("tool call: %v", err)
	}
	result, err := protocol.NewToolResultPart("call-1", "shell", json.RawMessage(`{"stdout":"/tmp"}`), false)
	if err != nil {
		t.Fatalf("tool result: %v", err)
	}
	after, err := protocol.NewTextPart("Done.")
	if err != nil {
		t.Fatalf("after text: %v", err)
	}
	messages := []protocol.ChatMessage{{
		ID:      "assistant-turn",
		Role:    protocol.RoleAssistant,
		Addr:    protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
		Content: []protocol.ContentPart{before, call, result, after},
	}}

	// When
	rows := rowsFromHistory(messages)

	// Then
	if len(rows) != 3 {
		t.Fatalf("row count = %d, want 3: %+v", len(rows), rows)
	}
	if rows[0].Kind != rowAssistant || rows[0].Text != "I will check." {
		t.Fatalf("first row = %+v, want assistant text", rows[0])
	}
	if rows[1].Kind != rowTool || !strings.Contains(rows[1].Text, "tool result shell") {
		t.Fatalf("second row = %+v, want tool result details", rows[1])
	}
	if rows[2].Kind != rowAssistant || rows[2].Text != "Done." {
		t.Fatalf("third row = %+v, want trailing assistant text", rows[2])
	}
}

func TestRowsFromHistory_deduplicatesPersistedToolRowsAcrossMessages(t *testing.T) {
	// Given
	before, err := protocol.NewTextPart("I will check.")
	if err != nil {
		t.Fatalf("before text: %v", err)
	}
	call, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell", Args: protocol.RawToArgs(json.RawMessage(`{"cmd":"pwd"}`))})
	if err != nil {
		t.Fatalf("tool call: %v", err)
	}
	result, err := protocol.NewToolResultPart("call-1", "shell", json.RawMessage(`{"stdout":"/tmp"}`), false)
	if err != nil {
		t.Fatalf("tool result: %v", err)
	}
	after, err := protocol.NewTextPart("Done.")
	if err != nil {
		t.Fatalf("after text: %v", err)
	}
	messages := []protocol.ChatMessage{
		{
			ID:      "assistant-call",
			Role:    protocol.RoleAssistant,
			Addr:    protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
			Content: []protocol.ContentPart{before, call},
		},
		{
			ID:      "tool-result",
			Role:    protocol.RoleToolResult,
			Addr:    protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
			Content: []protocol.ContentPart{result},
		},
		{
			ID:      "assistant-final",
			Role:    protocol.RoleAssistant,
			Addr:    protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
			Content: []protocol.ContentPart{after},
		},
	}

	// When
	rows := rowsFromHistory(messages)

	// Then
	if len(rows) != 3 {
		t.Fatalf("row count = %d, want 3: %+v", len(rows), rows)
	}
	if rows[0].Kind != rowAssistant || rows[0].Text != "I will check." {
		t.Fatalf("first row = %+v, want assistant text", rows[0])
	}
	if rows[1].Kind != rowTool || !strings.Contains(rows[1].Text, "tool result shell") {
		t.Fatalf("second row = %+v, want single updated tool row", rows[1])
	}
	if rows[2].Kind != rowAssistant || rows[2].Text != "Done." {
		t.Fatalf("third row = %+v, want trailing assistant text", rows[2])
	}
}

func TestApplyEventDoesNotRenderAnotherWorkspacesMatchingThreadID(t *testing.T) {
	m := model{
		workspaceID: "project-b", threadID: "shared",
		rows:  []chatRow{{Kind: rowSystem, Text: "project B"}},
		turns: map[string]turnActivity{}, tools: map[string]toolEntry{},
		pendingApprovals: map[string]approvalRequest{}, viewport: viewport.New(),
	}
	m.applyEvent(channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "shared", TaskID: "task-a"},
		MessageID: "assistant-a", Delta: "project A response",
	})
	if len(m.rows) != 1 || m.rows[0].Text != "project B" {
		t.Fatalf("foreign workspace event changed rows: %+v", m.rows)
	}
	if activity := m.turns["task-a"]; activity.WorkspaceID != "project-a" || activity.Phase != "responding" {
		t.Fatalf("foreign workspace activity = %+v", activity)
	}
}
