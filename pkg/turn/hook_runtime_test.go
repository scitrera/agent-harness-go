package turn

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestHookRuntimeHelperProcess(t *testing.T) {
	mode := os.Getenv("TURN_HOOK_HELPER_MODE")
	if mode == "" {
		return
	}
	switch mode {
	case "block":
		fmt.Fprint(os.Stderr, "blocked by turn hook")
		os.Exit(2)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q", mode)
		os.Exit(1)
	}
}

func Test_Runner_Run_hook_runtime_blocks_tool_before_execution(t *testing.T) {
	// Given
	invoked := 0
	reg := tools.NewRegistry()
	if err := reg.Register("write_file", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		invoked++
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "write_file"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("blocked")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	prov := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runtime := hooks.NewRuntime(hooks.Config{DefaultTimeout: 5 * time.Second}, []hooks.CommandHook{{
		Name:    "block-write",
		Event:   hooks.EventPreToolUse,
		Command: []string{os.Args[0], "-test.run=TestHookRuntimeHelperProcess"},
		Env:     map[string]string{"TURN_HOOK_HELPER_MODE": "block"},
	}})
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  prov,
		Registry:  reg,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Approvers: []hooks.ToolApprover{runtime},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	assistant, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "write"))

	// Then
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if assistant.ID != "assistant-final" {
		t.Fatalf("expected final assistant, got %#v", assistant)
	}
	if invoked != 0 {
		t.Fatalf("blocked hook must prevent tool execution, invoked=%d", invoked)
	}
}
