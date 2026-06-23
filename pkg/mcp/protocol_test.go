package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func Test_Manager_ListTools_and_CallTool_use_mcp_stdio_jsonrpc(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager := NewManager(time.Now)
	executable := currentTestExecutable(t)
	if err := manager.Register(ServerConfig{
		Name:    "calc",
		Command: executable,
		Args:    []string{"-test.run=Test_FakeMCPServer"},
		Env:     []string{"AGENT_HARNESS_FAKE_MCP=1"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	listed, err := manager.ListTools(ctx, "calc")
	result, callErr := manager.CallTool(ctx, "calc", "add", json.RawMessage(`{"a":2,"b":5}`))
	stopErr := manager.Stop(ctx, "calc")

	// Then
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if callErr != nil {
		t.Fatalf("call tool: %v", callErr)
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}
	if len(listed) != 1 || listed[0].Name != "add" {
		t.Fatalf("unexpected tools: %#v", listed)
	}
	if result.IsError || len(result.Content) != 1 || result.Content[0].Text != "7" {
		t.Fatalf("unexpected call result: %#v", result)
	}
}

func Test_Manager_ListTools_keeps_registered_server_cold_until_called(t *testing.T) {
	// Given
	manager := NewManager(time.Now)
	if err := manager.Register(ServerConfig{Name: "cold", Command: currentTestExecutable(t), Args: []string{"-test.run=Test_FakeMCPServer"}}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	statuses := manager.List()

	// Then
	if len(statuses) != 1 || statuses[0].Running {
		t.Fatalf("expected cold metadata before protocol call, got %#v", statuses)
	}
}

func currentTestExecutable(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	return executable
}

func Test_FakeMCPServer(t *testing.T) {
	if os.Getenv("AGENT_HARNESS_FAKE_MCP") != "1" {
		t.Skip("helper process only")
	}
	runFakeMCPServer()
	os.Exit(0)
}

func runFakeMCPServer() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      int64           `json:"id,omitempty"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params,omitempty"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		if req.ID == 0 {
			continue
		}
		respondFakeMCP(req.ID, req.Method)
	}
}

func respondFakeMCP(id int64, method string) {
	var result interface{}
	switch method {
	case "initialize":
		result = map[string]interface{}{"protocolVersion": "2024-11-05", "capabilities": map[string]interface{}{}}
	case "tools/list":
		result = map[string]interface{}{"tools": []map[string]interface{}{{"name": "add", "description": "add two numbers", "inputSchema": map[string]interface{}{"type": "object"}}}}
	case "tools/call":
		result = map[string]interface{}{"content": []map[string]interface{}{{"type": "text", "text": "7"}}}
	default:
		result = map[string]interface{}{}
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": result})
}
