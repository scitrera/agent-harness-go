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

func TestSubagentActivityKeepsParentSpawnRowLive(t *testing.T) {
	m := model{
		threadID:         "parent",
		viewport:         viewport.New(),
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		subagents:        map[string]subagentActivity{},
		turns:            map[string]turnActivity{},
		tailing:          true,
	}
	toolEvent := tools.ToolEvent{Status: tools.ToolEventStarted, CallID: "spawn-1", ToolName: tools.SubagentToolName}
	payload, err := json.Marshal(toolEvent)
	if err != nil {
		t.Fatalf("marshal lifecycle: %v", err)
	}
	parentAddr := protocol.MessageAddress{ThreadID: "parent", TaskID: "task-1"}
	m.applyEvent(channel.Event{Type: channel.EventToolLifecycle, Addr: parentAddr, Payload: payload})

	if len(m.rows) != 1 || !strings.Contains(m.rows[0].Text, "Subagent working") {
		t.Fatalf("initial subagent row = %+v", m.rows)
	}
	if m.status != "subagent working" {
		t.Fatalf("turn status = %q", m.status)
	}

	childAddr := protocol.MessageAddress{ThreadID: "parent::sub::1", TaskID: "task-1"}
	m.applyEvent(channel.Event{Type: channel.EventMessageStarted, Addr: childAddr})
	m.applyEvent(channel.Event{Type: channel.EventTokenDelta, Addr: childAddr, Delta: "Reviewing the repository structure now"})
	if len(m.rows) != 1 || !strings.Contains(m.rows[0].Text, "Reviewing the repository structure now") {
		t.Fatalf("live subagent row = %+v", m.rows)
	}
	before := m.rows[0].Text
	if !m.advanceThinking() || m.rows[0].Text == before {
		t.Fatalf("subagent indicator did not animate: before=%q after=%q", before, m.rows[0].Text)
	}

	finalPart, err := protocol.NewTextPart("Found the important packages and tests")
	if err != nil {
		t.Fatalf("final part: %v", err)
	}
	final := protocol.ChatMessage{Role: protocol.RoleAssistant, Addr: childAddr, Content: []protocol.ContentPart{finalPart}}
	m.applyEvent(channel.Event{Type: channel.EventMessageFinal, Addr: childAddr, Message: &final})
	if !strings.Contains(m.rows[0].Text, "Subagent completed") || !strings.Contains(m.rows[0].Text, "Found the important packages") {
		t.Fatalf("completed subagent row = %+v", m.rows)
	}
}

func TestSubagentPartNamesAndCompletesLiveRow(t *testing.T) {
	m := model{
		threadID:         "parent",
		viewport:         viewport.New(),
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		subagents:        map[string]subagentActivity{"spawn-1": {CallID: "spawn-1", TaskID: "task-1", Phase: "working"}},
		turns:            map[string]turnActivity{},
	}
	part, err := protocol.NewSubagentPart(protocol.SubagentPart{
		ID:       "spawn-1",
		Name:     "reviewer",
		ThreadID: "parent::sub::1",
		Status:   protocol.SubagentCompleted,
		Summary:  "review complete",
	})
	if err != nil {
		t.Fatalf("subagent part: %v", err)
	}
	m.applyEvent(channel.Event{
		Type: channel.EventPartAppended,
		Addr: protocol.MessageAddress{ThreadID: "parent", TaskID: "task-1"},
		Part: &part,
	})
	if len(m.rows) != 1 || !strings.Contains(m.rows[0].Text, "Subagent reviewer completed") || !strings.Contains(m.rows[0].Text, "review complete") {
		t.Fatalf("named subagent row = %+v", m.rows)
	}
}
