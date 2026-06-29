package tui

import (
	"encoding/json"
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
