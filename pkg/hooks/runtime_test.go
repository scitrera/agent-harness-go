package hooks

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func Test_Runtime_ApproveTool_runs_pre_tool_hooks_and_allows_on_success(t *testing.T) {
	// Given
	recordFile := tempRecordFile(t)
	hook := helperHook("record")
	hook.Event = EventPreToolUse
	hook.Env["HOOK_RECORD_FILE"] = recordFile
	runtime := NewRuntime(Config{DefaultTimeout: 5 * time.Second}, []CommandHook{hook})

	// When
	decision := runtime.ApproveTool(context.Background(), ToolCall{CallID: "call-1", Name: "read_file"})

	// Then
	if !decision.Allow {
		t.Fatalf("expected hook to allow tool, got %#v", decision)
	}
	got, err := os.ReadFile(recordFile)
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	if string(got) != "hook-record\n" {
		t.Fatalf("unexpected hook record: %q", string(got))
	}
}

func Test_Runtime_ApproveTool_denies_when_pre_tool_hook_blocks(t *testing.T) {
	// Given
	hook := helperHook("block")
	hook.Event = EventPreToolUse
	runtime := NewRuntime(Config{DefaultTimeout: 5 * time.Second}, []CommandHook{hook})

	// When
	decision := runtime.ApproveTool(context.Background(), ToolCall{CallID: "call-1", Name: "write_file"})

	// Then
	if decision.Allow || decision.Reason != "blocked by hook" {
		t.Fatalf("expected blocking decision, got %#v", decision)
	}
}

func Test_Runtime_ToolFinished_dispatches_post_tool_hooks(t *testing.T) {
	// Given
	recordFile := tempRecordFile(t)
	hook := helperHook("record")
	hook.Event = EventPostToolUse
	hook.Env["HOOK_RECORD_FILE"] = recordFile
	runtime := NewRuntime(Config{DefaultTimeout: 5 * time.Second}, []CommandHook{hook})

	// When
	runtime.ToolStarted(context.Background(), ToolCall{CallID: "call-1", Name: "calc"})
	runtime.ToolFinished(context.Background(), ToolCall{CallID: "call-1", Name: "calc"}, false, nil)

	// Then
	got, err := os.ReadFile(recordFile)
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	if string(got) != "hook-record\n" {
		t.Fatalf("unexpected hook record: %q", string(got))
	}
}

func Test_Runtime_EmitToolEvent_dispatches_todo3_lifecycle_payload(t *testing.T) {
	// Given
	recordFile := tempRecordFile(t)
	hook := helperHook("record")
	hook.Event = EventToolLifecycle
	hook.Env["HOOK_RECORD_FILE"] = recordFile
	runtime := NewRuntime(Config{DefaultTimeout: 5 * time.Second}, []CommandHook{hook})
	event := tools.NewToolEvent(tools.ToolEventStarted, tools.Request{
		CallID:    "call-1",
		Name:      "calc",
		Arguments: []byte(`{"secret":"redacted"}`),
		Addr:      protocol.MessageAddress{ThreadID: "thread-1"},
	})

	// When
	if err := runtime.EmitToolEvent(context.Background(), event); err != nil {
		t.Fatalf("emit tool event: %v", err)
	}

	// Then
	got, err := os.ReadFile(recordFile)
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	if string(got) != "hook-record\n" {
		t.Fatalf("unexpected hook record: %q", string(got))
	}
}
