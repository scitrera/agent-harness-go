package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/mcp"
)

type fakeMCPCaller struct {
	server string
	tool   string
	args   json.RawMessage
}

func (f *fakeMCPCaller) CallTool(_ context.Context, server string, tool string, args json.RawMessage) (mcp.CallToolResult, error) {
	f.server = server
	f.tool = tool
	f.args = append(json.RawMessage(nil), args...)
	return mcp.CallToolResult{Content: []mcp.Content{{Type: "text", Text: "ok"}}}, nil
}

func Test_RegisterMCPTool_invokes_server_tool_with_original_arguments(t *testing.T) {
	// Given
	ctx := context.Background()
	caller := &fakeMCPCaller{}
	reg := NewRegistry()
	if err := RegisterMCPTool(reg, MCPToolConfig{RegistryName: "mcp.calc.add", ServerName: "calc", ToolName: "add", Caller: caller}); err != nil {
		t.Fatalf("register mcp tool: %v", err)
	}

	// When
	result, err := reg.Invoke(ctx, Request{CallID: "call-mcp", Name: "mcp.calc.add", Arguments: json.RawMessage(`{"a":2,"b":5}`)})

	// Then
	if err != nil {
		t.Fatalf("invoke mcp tool: %v", err)
	}
	if caller.server != "calc" || caller.tool != "add" || string(caller.args) != `{"a":2,"b":5}` {
		t.Fatalf("unexpected mcp call: %#v", caller)
	}
	if result.CallID != "call-mcp" || result.Name != "mcp.calc.add" || !json.Valid(result.Payload) {
		t.Fatalf("unexpected result: %#v", result)
	}
}
