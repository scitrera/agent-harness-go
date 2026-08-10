package provider

import (
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func textMsg(t *testing.T, role protocol.Role, text string) protocol.ChatMessage {
	t.Helper()
	p, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	return protocol.ChatMessage{Role: role, Content: []protocol.ContentPart{p}}
}

func toolCallMsg(t *testing.T, id, name string) protocol.ChatMessage {
	t.Helper()
	p, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: id, Name: name})
	if err != nil {
		t.Fatalf("tool call: %v", err)
	}
	return protocol.ChatMessage{Role: protocol.RoleAssistant, Content: []protocol.ContentPart{p}}
}

func toolResultMsg(t *testing.T, callID string) protocol.ChatMessage {
	t.Helper()
	p, err := protocol.NewToolResultPart(callID, "tool", []byte(`"ok"`), false)
	if err != nil {
		t.Fatalf("tool result: %v", err)
	}
	return protocol.ChatMessage{Role: protocol.RoleToolResult, Content: []protocol.ContentPart{p}}
}

func TestSanitizeTranscriptLeavesValidPairsUntouched(t *testing.T) {
	in := []protocol.ChatMessage{
		textMsg(t, protocol.RoleUser, "hi"),
		toolCallMsg(t, "c1", "read_file"),
		toolResultMsg(t, "c1"),
		textMsg(t, protocol.RoleAssistant, "done"),
	}
	out := sanitizeTranscript(in)
	if len(out) != len(in) {
		t.Fatalf("valid transcript should pass through, got %d of %d", len(out), len(in))
	}
}

func TestSanitizeTranscriptDropsOrphanToolResult(t *testing.T) {
	// tool result with no preceding tool_call (e.g. assistant turn trimmed).
	in := []protocol.ChatMessage{
		textMsg(t, protocol.RoleUser, "hi"),
		toolResultMsg(t, "ghost"),
		textMsg(t, protocol.RoleAssistant, "answer"),
	}
	out := sanitizeTranscript(in)
	if len(out) != 2 {
		t.Fatalf("expected orphan result dropped, got %#v", out)
	}
	for _, m := range out {
		for _, p := range m.Content {
			if p.Type() == protocol.ContentToolResult {
				t.Fatal("orphan tool_result survived")
			}
		}
	}
}

func TestSanitizeTranscriptStripsUnansweredToolCall(t *testing.T) {
	// assistant tool_call with no following result (e.g. result turn trimmed).
	in := []protocol.ChatMessage{
		textMsg(t, protocol.RoleUser, "hi"),
		toolCallMsg(t, "c9", "list_dir"),
		textMsg(t, protocol.RoleAssistant, "never got the result"),
	}
	out := sanitizeTranscript(in)
	for _, m := range out {
		for _, p := range m.Content {
			if p.Type() == protocol.ContentToolCall {
				t.Fatal("unanswered tool_call survived")
			}
		}
	}
	// The tool_call message had only the call part -> dropped entirely.
	if len(out) != 2 {
		t.Fatalf("expected emptied tool_call message dropped, got %#v", out)
	}
}

func TestSanitizeTranscriptDropsContentlessTerminalMarker(t *testing.T) {
	in := []protocol.ChatMessage{
		textMsg(t, protocol.RoleUser, "hi"),
		{ID: "turn-terminal", Role: protocol.RoleAssistant},
		textMsg(t, protocol.RoleUser, "continue"),
	}
	out := sanitizeTranscript(in)
	if len(out) != 2 || out[0].ID == "turn-terminal" || out[1].ID == "turn-terminal" {
		t.Fatalf("contentless terminal marker reached provider transcript: %#v", out)
	}
}

// A tool-result message that ALSO carries an extra subagent reference part must
// lower to exactly the same single OpenAI "tool" message as a plain tool-result
// message: the extra part is ignored (lowerMessage has no case for it) and never
// leaks into the tool content. This guards the co-located subagent-part change.
func TestLowerMessageIgnoresExtraSubagentPartOnToolResult(t *testing.T) {
	tr, err := protocol.NewToolResultPart("c1", "tool", []byte(`"ok"`), false)
	if err != nil {
		t.Fatalf("tool result: %v", err)
	}
	sp, err := protocol.NewSubagentPart(protocol.SubagentPart{
		ID:       "c1",
		Name:     "child",
		ThreadID: "t::sub::1",
		Status:   protocol.SubagentCompleted,
		Summary:  "child summary",
	})
	if err != nil {
		t.Fatalf("subagent part: %v", err)
	}

	plain := lowerMessage(protocol.ChatMessage{Role: protocol.RoleToolResult, Content: []protocol.ContentPart{tr}})
	withExtra := lowerMessage(protocol.ChatMessage{Role: protocol.RoleToolResult, Content: []protocol.ContentPart{tr, sp}})

	if len(plain) != 1 || len(withExtra) != 1 {
		t.Fatalf("expected exactly one tool message each, got plain=%d withExtra=%d", len(plain), len(withExtra))
	}
	if withExtra[0].Role != "tool" {
		t.Fatalf("role = %q, want tool", withExtra[0].Role)
	}
	// openAIMessage has a slice field (not ==-comparable); compare via JSON.
	plainJSON, _ := json.Marshal(plain[0])
	extraJSON, _ := json.Marshal(withExtra[0])
	if string(plainJSON) != string(extraJSON) {
		t.Fatalf("extra subagent part changed the lowered tool message: plain=%s withExtra=%s", plainJSON, extraJSON)
	}
	// The subagent summary must not leak into the tool content.
	if s, ok := withExtra[0].Content.(string); ok && s != `"ok"` {
		t.Fatalf("tool content = %q, want \"ok\" (no subagent leakage)", s)
	}
}
