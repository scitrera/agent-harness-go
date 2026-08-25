package tui

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestReasoningIsLabeledWhileCurrentThenConsumedByResponse(t *testing.T) {
	m := reasoningTestModel(false)
	addr := protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"}
	reasoning, err := protocol.NewReasoningPart("", false)
	if err != nil {
		t.Fatalf("reasoning part: %v", err)
	}
	response, err := protocol.NewTextPart("")
	if err != nil {
		t.Fatalf("response part: %v", err)
	}

	m.applyEvent(channel.Event{Type: channel.EventMessageStarted, Addr: addr})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: addr, MessageID: "assistant-1", Index: 0, Part: &reasoning})
	m.applyEvent(channel.Event{Type: channel.EventTokenDelta, Addr: addr, MessageID: "assistant-1", Index: 0, Delta: "The user asked a simple question."})

	if len(m.rows) != 1 || m.rows[0].Kind != rowReasoning {
		t.Fatalf("current reasoning rows = %+v", m.rows)
	}
	if got := m.renderRow(m.rows[0]); !strings.Contains(got, "reasoning> ") {
		t.Fatalf("rendered reasoning row = %q, want reasoning prefix", got)
	}
	if got := m.turns["task-1"].Phase; got != "reasoning" {
		t.Fatalf("turn phase = %q, want reasoning", got)
	}

	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: addr, MessageID: "assistant-1", Index: 1, Part: &response})
	m.applyEvent(channel.Event{Type: channel.EventTokenDelta, Addr: addr, MessageID: "assistant-1", Index: 1, Delta: "Ready to work."})

	if len(m.rows) != 1 || m.rows[0].Kind != rowAssistant || m.rows[0].Text != "Ready to work." {
		t.Fatalf("rows after response = %+v", m.rows)
	}
}

func TestReasoningIsConsumedByToolCall(t *testing.T) {
	m := reasoningTestModel(false)
	addr := protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"}
	reasoning, err := protocol.NewReasoningPart("I should inspect the repository.", false)
	if err != nil {
		t.Fatalf("reasoning part: %v", err)
	}
	call, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "list_dir"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}

	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: addr, MessageID: "assistant-1", Index: 0, Part: &reasoning})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: addr, MessageID: "assistant-1", Index: 1, Part: &call})

	if len(m.rows) != 1 || m.rows[0].Kind != rowTool {
		t.Fatalf("rows after tool call = %+v", m.rows)
	}
}

func TestRetainReasoningKeepsTraceAfterResponse(t *testing.T) {
	m := reasoningTestModel(true)
	addr := protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"}
	reasoning, err := protocol.NewReasoningPart("The user asked a simple question.", false)
	if err != nil {
		t.Fatalf("reasoning part: %v", err)
	}
	response, err := protocol.NewTextPart("Ready to work.")
	if err != nil {
		t.Fatalf("response part: %v", err)
	}

	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: addr, MessageID: "assistant-1", Index: 0, Part: &reasoning})
	m.applyEvent(channel.Event{Type: channel.EventPartAppended, Addr: addr, MessageID: "assistant-1", Index: 1, Part: &response})

	if len(m.rows) != 2 || m.rows[0].Kind != rowReasoning || m.rows[1].Kind != rowAssistant {
		t.Fatalf("retained reasoning rows = %+v", m.rows)
	}
}

func TestRowsFromHistoryTreatReasoningAsEphemeralByDefault(t *testing.T) {
	reasoning, err := protocol.NewReasoningPart("The user asked a simple question.", false)
	if err != nil {
		t.Fatalf("reasoning part: %v", err)
	}
	response, err := protocol.NewTextPart("Ready to work.")
	if err != nil {
		t.Fatalf("response part: %v", err)
	}
	messages := []protocol.ChatMessage{{
		ID:      "assistant-1",
		Role:    protocol.RoleAssistant,
		Addr:    protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
		Content: []protocol.ContentPart{reasoning, response},
	}}

	rows := rowsFromHistory(messages)
	if len(rows) != 1 || rows[0].Kind != rowAssistant || rows[0].Text != "Ready to work." {
		t.Fatalf("default history rows = %+v", rows)
	}

	retained := rowsFromHistoryWithReasoning(messages, true)
	if len(retained) != 2 || retained[0].Kind != rowReasoning || retained[1].Kind != rowAssistant {
		t.Fatalf("retained history rows = %+v", retained)
	}
}

func TestRowsFromHistoryShowsUnconsumedTrailingReasoning(t *testing.T) {
	reasoning, err := protocol.NewReasoningPart("Still deciding what to do.", false)
	if err != nil {
		t.Fatalf("reasoning part: %v", err)
	}
	rows := rowsFromHistory([]protocol.ChatMessage{{
		ID:      "assistant-1",
		Role:    protocol.RoleAssistant,
		Addr:    protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
		Content: []protocol.ContentPart{reasoning},
	}})
	if len(rows) != 1 || rows[0].Kind != rowReasoning {
		t.Fatalf("trailing reasoning rows = %+v", rows)
	}
}

func reasoningTestModel(retain bool) model {
	return model{
		threadID:         "t1",
		retainReasoning:  retain,
		turns:            map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		viewport:         viewport.New(),
	}
}
