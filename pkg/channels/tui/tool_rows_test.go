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

func TestModelApplyPartAppended_preservesToolLifecycleDetails(t *testing.T) {
	// Given
	m := model{
		threadID:         "t1",
		tools:            map[string]toolEntry{},
		pendingApprovals: map[string]approvalRequest{},
		viewport:         viewport.New(),
	}
	event := tools.ToolEvent{Status: tools.ToolEventFinished, CallID: "call-1", ToolName: "shell", DurationMS: 42}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resultPart, err := protocol.NewToolResultPart("call-1", "shell", json.RawMessage(`{"stdout":"/tmp"}`), false)
	if err != nil {
		t.Fatalf("tool result part: %v", err)
	}

	// When
	m.applyEvent(channel.Event{Type: channel.EventToolLifecycle, Addr: protocol.MessageAddress{ThreadID: "t1"}, Payload: payload})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: protocol.MessageAddress{ThreadID: "t1"}, MessageID: "assistant-turn", Index: 1, Part: &resultPart})

	// Then
	if len(m.rows) != 1 {
		t.Fatalf("row count = %d, want 1: %+v", len(m.rows), m.rows)
	}
	if !strings.Contains(m.rows[0].Text, "tool shell: finished") || !strings.Contains(m.rows[0].Text, "42ms") {
		t.Fatalf("tool lifecycle details were not preserved: %+v", m.rows[0])
	}
	if strings.Contains(m.rows[0].Text, "tool result shell") {
		t.Fatalf("generic tool result replaced lifecycle details: %+v", m.rows[0])
	}
}
