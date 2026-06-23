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
