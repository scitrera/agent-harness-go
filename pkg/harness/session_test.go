package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func userMsg(t *testing.T, id, text string) protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
}

func newSeededSession(t *testing.T, seed ...protocol.ChatMessage) *Session {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryStore()
	if len(seed) > 0 {
		if err := store.SaveHistory(ctx, "thread-1", seed); err != nil {
			t.Fatalf("seed history: %v", err)
		}
	}
	session, err := NewSession(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, store, tools.NewRegistry(), tools.MemoryAuthority{})
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	return session
}

func Test_Session_DropTrailingUserDuplicate_matches_by_id(t *testing.T) {
	session := newSeededSession(t, userMsg(t, "m1", "committed copy text"))
	// Incoming carries the same id but a richer body (e.g. attachments stripped on
	// the committed copy) — drop the committed copy so the incoming stays canonical.
	incoming := userMsg(t, "m1", "incoming body")
	if !session.DropTrailingUserDuplicate(incoming) {
		t.Fatalf("expected drop on id match")
	}
	if got := len(session.History()); got != 0 {
		t.Fatalf("expected history emptied, got %d", got)
	}
}

func Test_Session_DropTrailingUserDuplicate_matches_by_text_when_ids_differ(t *testing.T) {
	session := newSeededSession(t, userMsg(t, "host-committed-id", "hello there"))
	incoming := userMsg(t, "task-delivered-id", "hello there")
	if !session.DropTrailingUserDuplicate(incoming) {
		t.Fatalf("expected drop on content match")
	}
	if got := len(session.History()); got != 0 {
		t.Fatalf("expected history emptied, got %d", got)
	}
}

func Test_Session_DropTrailingUserDuplicate_keeps_distinct_message(t *testing.T) {
	session := newSeededSession(t, userMsg(t, "m1", "an earlier question"))
	incoming := userMsg(t, "m2", "a different question")
	if session.DropTrailingUserDuplicate(incoming) {
		t.Fatalf("must not drop a distinct trailing message")
	}
	if got := len(session.History()); got != 1 {
		t.Fatalf("expected history preserved, got %d", got)
	}
}

func Test_Session_DropTrailingUserDuplicate_ignores_non_user_tail(t *testing.T) {
	part, err := protocol.NewTextPart("an assistant reply")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	asst := protocol.ChatMessage{ID: "a1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}
	session := newSeededSession(t, userMsg(t, "m1", "hi"), asst)
	// The incoming repeats an earlier user message verbatim, but it is no longer
	// the tail (an assistant reply followed) — a legitimate repeat, not a dup.
	if session.DropTrailingUserDuplicate(userMsg(t, "m1", "hi")) {
		t.Fatalf("must not drop when the tail is not a user message")
	}
	if got := len(session.History()); got != 2 {
		t.Fatalf("expected history preserved, got %d", got)
	}
}

func Test_Session_InvokeTool_persists_tool_result_when_local_tool_succeeds(t *testing.T) {
	// Given
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "SOUL.md"), []byte("name: Scitrera\n"), 0o644); err != nil {
		t.Fatalf("write soul fixture: %v", err)
	}
	store := NewMemoryStore()
	ws, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	reg := tools.NewRegistry()
	if err := tools.RegisterLocal(reg, tools.LocalConfig{Workspace: ws}); err != nil {
		t.Fatalf("register tools: %v", err)
	}
	session, err := NewSession(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, store, reg, tools.MemoryAuthority{})
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	// When
	_, err = session.InvokeTool(ctx, protocol.ToolInvokeEnvelope{
		CallID: "tool-1",
		Name:   "edit_file",
		Args:   protocol.RawToArgs(json.RawMessage(`{"path":"SOUL.md","old_text":"Scitrera","new_text":"Winick"}`)),
	})

	// Then
	if err != nil {
		t.Fatalf("invoke tool: %v", err)
	}
	loaded, err := store.LoadHistory(ctx, "thread-1")
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Role != protocol.RoleToolResult {
		t.Fatalf("expected one tool result message, got %#v", loaded)
	}
	updated, err := os.ReadFile(filepath.Join(root, "SOUL.md"))
	if err != nil {
		t.Fatalf("read soul: %v", err)
	}
	if string(updated) != "name: Winick\n" {
		t.Fatalf("unexpected soul content: %q", string(updated))
	}
}
