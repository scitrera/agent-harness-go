package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
)

// mkPart returns a helper that unwraps a (ContentPart, error) constructor result,
// failing the test on error (the spec constructors never actually error).
func mkPart(t *testing.T) func(protocol.ContentPart, error) protocol.ContentPart {
	return func(p protocol.ContentPart, err error) protocol.ContentPart {
		t.Helper()
		if err != nil {
			t.Fatalf("build part: %v", err)
		}
		return p
	}
}

// seedHistory writes a 3-message thread (user, assistant w/ tool_call + usage
// meta, tool_result) to <stateDir>/history/<thread>.json and returns stateDir.
func seedHistory(t *testing.T, thread string) string {
	t.Helper()
	mk := mkPart(t)
	stateDir := t.TempDir()
	hist := filepath.Join(stateDir, "history")
	if err := os.MkdirAll(hist, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	usageMeta, _ := json.Marshal(map[string]any{
		"model": "gpt-4o-mini", "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "calls": 1,
	})
	toolCall := mk(protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "c1", Name: "read_file",
		Args: map[string]json.RawMessage{"path": json.RawMessage(`"SOUL.md"`)},
	}))
	msgs := []protocol.ChatMessage{
		{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{mk(protocol.NewTextPart("hello"))}},
		{
			ID: "a1", Role: protocol.RoleAssistant,
			Content: []protocol.ContentPart{mk(protocol.NewTextPart("on it")), toolCall},
			Meta:    map[string]json.RawMessage{compaction.MetaUsage: usageMeta},
		},
		{
			ID: "t1", Role: protocol.RoleToolResult,
			Content: []protocol.ContentPart{mk(protocol.NewToolResultPart("c1", "read_file", json.RawMessage(`{"ok":true}`), false))},
		},
	}
	data, _ := json.Marshal(msgs)
	if err := os.WriteFile(filepath.Join(hist, sanitizeThread(thread)+".json"), data, 0o644); err != nil {
		t.Fatalf("write history: %v", err)
	}
	return stateDir
}

func TestRunExportOpenAI(t *testing.T) {
	stateDir := seedHistory(t, "thread1")
	var buf bytes.Buffer
	if err := runExport(stateDir, "thread1", "openai", &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 thread line, got %d", len(lines))
	}
	var rec openAIExport
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("decode line: %v", err)
	}
	if rec.Thread != "thread1" {
		t.Fatalf("thread = %q", rec.Thread)
	}
	if len(rec.Messages) != 3 {
		t.Fatalf("messages = %d, want 3 (user, assistant, tool)", len(rec.Messages))
	}
	if rec.Messages[0].Role != "user" || rec.Messages[0].Content != "hello" {
		t.Fatalf("user msg = %+v", rec.Messages[0])
	}
	asst := rec.Messages[1]
	if asst.Role != "assistant" || len(asst.ToolCalls) != 1 || asst.ToolCalls[0].ID != "c1" || asst.ToolCalls[0].Function.Name != "read_file" {
		t.Fatalf("assistant msg = %+v", asst)
	}
	if !strings.Contains(asst.ToolCalls[0].Function.Arguments, "SOUL.md") {
		t.Fatalf("tool args = %q", asst.ToolCalls[0].Function.Arguments)
	}
	tool := rec.Messages[2]
	if tool.Role != "tool" || tool.ToolCallID != "c1" || !strings.Contains(tool.Content, "ok") {
		t.Fatalf("tool msg = %+v", tool)
	}
	if rec.Usage.TotalTokens != 15 || rec.Usage.Model != "gpt-4o-mini" || rec.Usage.Calls != 1 {
		t.Fatalf("usage = %+v", rec.Usage)
	}
}

func TestRunExportTraceLossless(t *testing.T) {
	stateDir := seedHistory(t, "thread1")
	var buf bytes.Buffer
	if err := runExport(stateDir, "thread1", "trace", &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	var rec traceExport
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rec.Messages) != 3 {
		t.Fatalf("trace messages = %d, want 3", len(rec.Messages))
	}
	// Lossless: the raw tool_call part survives round-trip.
	if _, ok := rec.Messages[1].Content[1].AsToolCall(); !ok {
		t.Fatalf("assistant tool_call part not preserved in trace export")
	}
	if rec.Usage.TotalTokens != 15 {
		t.Fatalf("usage total = %d, want 15", rec.Usage.TotalTokens)
	}
}

func TestRunExportErrors(t *testing.T) {
	stateDir := seedHistory(t, "thread1")
	var buf bytes.Buffer
	if err := runExport(stateDir, "thread1", "bogus", &buf); err == nil {
		t.Fatal("expected error for unknown format")
	}
	if err := runExport(stateDir, "missing", "openai", &buf); err == nil {
		t.Fatal("expected error for missing thread")
	}
}

func TestRunExportAll(t *testing.T) {
	stateDir := seedHistory(t, "thread1")
	var buf bytes.Buffer
	if err := runExport(stateDir, "all", "openai", &buf); err != nil {
		t.Fatalf("export all: %v", err)
	}
	if n := len(strings.Split(strings.TrimSpace(buf.String()), "\n")); n != 1 {
		t.Fatalf("export all lines = %d, want 1", n)
	}
}

func TestRunWorkspaceExportIsScopedAndLabelsRecords(t *testing.T) {
	stateDir := t.TempDir()
	files := store.NewFileStore("", stateDir)
	part := mkPart(t)(protocol.NewTextPart("hello"))
	for _, workspaceID := range []string{"project-a", "project-b"} {
		message := protocol.ChatMessage{
			ID:      "message-" + workspaceID,
			Role:    protocol.RoleUser,
			Addr:    protocol.MessageAddress{WorkspaceID: workspaceID, ThreadID: "shared"},
			Content: []protocol.ContentPart{part},
		}
		if err := files.SaveWorkspaceHistory(t.Context(), workspaceID, "shared", []protocol.ChatMessage{message}); err != nil {
			t.Fatalf("save %s: %v", workspaceID, err)
		}
	}

	var buf bytes.Buffer
	if err := runWorkspaceExport(stateDir, "project-a", "all", "trace", &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	var record traceExport
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if record.Workspace != "project-a" || record.Thread != "shared" || len(record.Messages) != 1 || record.Messages[0].ID != "message-project-a" {
		t.Fatalf("record = %+v", record)
	}
}
