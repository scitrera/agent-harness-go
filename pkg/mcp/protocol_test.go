// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

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

func Test_Manager_CallTool_drops_empty_optional_string_args_over_the_wire(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager := NewManager(time.Now)
	if err := manager.Register(ServerConfig{
		Name:    "echo",
		Command: currentTestExecutable(t),
		Args:    []string{"-test.run=Test_FakeMCPServer"},
		Env:     []string{"AGENT_HARNESS_FAKE_MCP=1", "AGENT_HARNESS_FAKE_MCP_ECHO=1"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	result, callErr := manager.CallTool(ctx, "echo", "echo", json.RawMessage(`{"id":"","note":"","tag":"keep"}`))
	stopErr := manager.Stop(ctx, "echo")

	// Then
	if callErr != nil {
		t.Fatalf("call tool: %v", callErr)
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}
	if len(result.Content) != 1 {
		t.Fatalf("unexpected content: %#v", result.Content)
	}
	var sent map[string]json.RawMessage
	if err := json.Unmarshal([]byte(result.Content[0].Text), &sent); err != nil {
		t.Fatalf("decode echoed arguments: %v", err)
	}
	if _, ok := sent["note"]; ok {
		t.Fatalf("expected empty optional string to be dropped, got %#v", sent)
	}
	if string(sent["id"]) != `""` {
		t.Fatalf("expected required empty string field to be preserved, got %#v", sent)
	}
	if string(sent["tag"]) != `"keep"` {
		t.Fatalf("expected non-empty string to be preserved, got %#v", sent)
	}
}

func Test_Manager_CallTool_restarts_dead_session_and_retries_once(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager := NewManager(time.Now)
	if err := manager.Register(ServerConfig{
		Name:    "calc",
		Command: currentTestExecutable(t),
		Args:    []string{"-test.run=Test_FakeMCPServer"},
		Env:     []string{"AGENT_HARNESS_FAKE_MCP=1"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := manager.ListTools(ctx, "calc"); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	// When: sever the stdin pipe out from under the manager to simulate a
	// dead session without killing the process, so the manager still
	// believes it's running but the next write will fail.
	manager.mu.Lock()
	state := manager.servers["calc"]
	oldPID := state.cmd.Process.Pid
	if err := state.stdin.Close(); err != nil {
		manager.mu.Unlock()
		t.Fatalf("close stdin: %v", err)
	}
	manager.mu.Unlock()

	result, callErr := manager.CallTool(ctx, "calc", "add", json.RawMessage(`{"a":2,"b":5}`))

	manager.mu.Lock()
	newPID := 0
	if newState := manager.servers["calc"]; newState.cmd != nil && newState.cmd.Process != nil {
		newPID = newState.cmd.Process.Pid
	}
	manager.mu.Unlock()

	stopErr := manager.Stop(ctx, "calc")

	// Then
	if callErr != nil {
		t.Fatalf("call tool after dead session: %v", callErr)
	}
	if result.IsError || len(result.Content) != 1 || result.Content[0].Text != "7" {
		t.Fatalf("unexpected call result: %#v", result)
	}
	if newPID == 0 || newPID == oldPID {
		t.Fatalf("expected server to be restarted with a new pid, old=%d new=%d", oldPID, newPID)
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
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
		respondFakeMCP(req.ID, req.Method, req.Params)
	}
}

func respondFakeMCP(id int64, method string, params json.RawMessage) {
	if os.Getenv("AGENT_HARNESS_FAKE_MCP_UNSUPPORTED_RESOURCES") == "1" && isResourceMethod(method) {
		writeFakeMCPError(id, codeMethodNotFound, "method not found")
		return
	}

	var result interface{}
	switch method {
	case "initialize":
		result = map[string]interface{}{"protocolVersion": "2024-11-05", "capabilities": map[string]interface{}{}}
	case "tools/list":
		result = fakeToolsList()
	case "tools/call":
		result = fakeToolsCall(params)
	case "resources/list":
		result = fakeResourcesList(params)
	case "resources/read":
		result = fakeResourceRead(params)
	case "resources/templates/list":
		result = fakeResourceTemplatesList(params)
	default:
		result = map[string]interface{}{}
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": result})
}

// fakeToolsList returns the "add" tool by default, or an "echo" tool (which
// declares a required and two optional string properties) when
// AGENT_HARNESS_FAKE_MCP_ECHO=1, so tests can verify argument normalization
// end-to-end.
func fakeToolsList() interface{} {
	if os.Getenv("AGENT_HARNESS_FAKE_MCP_ECHO") == "1" {
		return map[string]interface{}{"tools": []map[string]interface{}{{
			"name":        "echo",
			"description": "echo received arguments",
			"inputSchema": map[string]interface{}{
				"type":     "object",
				"required": []string{"id"},
				"properties": map[string]interface{}{
					"id":   map[string]interface{}{"type": "string"},
					"note": map[string]interface{}{"type": "string"},
					"tag":  map[string]interface{}{"type": "string"},
				},
			},
		}}}
	}
	return map[string]interface{}{"tools": []map[string]interface{}{{"name": "add", "description": "add two numbers", "inputSchema": map[string]interface{}{"type": "object"}}}}
}

// fakeToolsCall echoes back the received arguments as text when
// AGENT_HARNESS_FAKE_MCP_ECHO=1, otherwise always answers "7" for "add".
func fakeToolsCall(params json.RawMessage) interface{} {
	if os.Getenv("AGENT_HARNESS_FAKE_MCP_ECHO") == "1" {
		var decoded struct {
			Arguments json.RawMessage `json:"arguments"`
		}
		_ = json.Unmarshal(params, &decoded)
		return map[string]interface{}{"content": []map[string]interface{}{{"type": "text", "text": string(decoded.Arguments)}}}
	}
	return map[string]interface{}{"content": []map[string]interface{}{{"type": "text", "text": "7"}}}
}

func isResourceMethod(method string) bool {
	switch method {
	case "resources/list", "resources/read", "resources/templates/list":
		return true
	default:
		return false
	}
}

func writeFakeMCPError(id int64, code int, message string) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	})
}
