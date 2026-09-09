// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// fakeMCPManager doubles as an MCPCaller and mcpLister: it serves a per-server
// tool list and echoes CallTool into recorded fields.
type fakeMCPManager struct {
	tools   map[string][]mcp.Tool
	listErr map[string]error

	server string
	tool   string
	args   json.RawMessage
}

func (f *fakeMCPManager) ListTools(_ context.Context, server string) ([]mcp.Tool, error) {
	if err := f.listErr[server]; err != nil {
		return nil, err
	}
	return f.tools[server], nil
}

func (f *fakeMCPManager) CallTool(_ context.Context, server string, tool string, args json.RawMessage) (mcp.CallToolResult, error) {
	f.server = server
	f.tool = tool
	f.args = append(json.RawMessage(nil), args...)
	return mcp.CallToolResult{Content: []mcp.Content{{Type: "text", Text: "ok"}}}, nil
}

func Test_MCPProvider_Tools_uses_registry_name_scheme_and_schemas(t *testing.T) {
	// Given
	ctx := context.Background()
	mgr := &fakeMCPManager{tools: map[string][]mcp.Tool{
		"calc": {
			{Name: "add", Description: "adds", InputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"number"}}}`)},
			{Name: "sub", Description: "subtracts"},
		},
	}}
	p := NewMCPProvider(MCPProviderConfig{Caller: mgr, Servers: []string{"calc"}})

	// When
	descs, err := p.Tools(ctx, addr(), user())

	// Then
	if err != nil {
		t.Fatalf("tools: %v", err)
	}
	if len(descs) != 2 {
		t.Fatalf("expected 2 descriptors, got %d: %#v", len(descs), descs)
	}
	byName := map[string]Descriptor{}
	for _, d := range descs {
		byName[d.Name] = d
	}
	add, ok := byName["mcp.calc.add"]
	if !ok {
		t.Fatalf("missing mcp.calc.add: %#v", descs)
	}
	if add.Description != "adds" || string(add.Parameters) != `{"type":"object","properties":{"a":{"type":"number"}}}` {
		t.Fatalf("unexpected add descriptor: %#v", add)
	}
	if add.Trust != TrustDefault {
		t.Fatalf("expected TrustDefault, got %v", add.Trust)
	}
	if _, ok := byName["mcp.calc.sub"]; !ok {
		t.Fatalf("missing mcp.calc.sub: %#v", descs)
	}
}

func Test_MCPProvider_Invoke_routes_to_server_tool(t *testing.T) {
	// Given
	ctx := context.Background()
	mgr := &fakeMCPManager{tools: map[string][]mcp.Tool{
		"calc": {{Name: "add"}},
	}}
	p := NewMCPProvider(MCPProviderConfig{Caller: mgr, Servers: []string{"calc"}})
	if _, err := p.Tools(ctx, addr(), user()); err != nil {
		t.Fatalf("tools: %v", err)
	}

	// When
	result, err := p.Invoke(ctx, Request{CallID: "call-1", Name: "mcp.calc.add", Arguments: json.RawMessage(`{"a":2,"b":5}`)})

	// Then
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if mgr.server != "calc" || mgr.tool != "add" || string(mgr.args) != `{"a":2,"b":5}` {
		t.Fatalf("unexpected call: %#v", mgr)
	}
	if result.CallID != "call-1" || result.Name != "mcp.calc.add" || !json.Valid(result.Payload) {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func Test_MCPProvider_Invoke_resolves_without_prior_Tools_via_parser(t *testing.T) {
	// Given no Tools() call primed the route map.
	ctx := context.Background()
	mgr := &fakeMCPManager{}
	p := NewMCPProvider(MCPProviderConfig{Caller: mgr, Servers: []string{"calc"}})

	// When
	if _, err := p.Invoke(ctx, Request{CallID: "c", Name: "mcp.calc.add", Arguments: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Then
	if mgr.server != "calc" || mgr.tool != "add" {
		t.Fatalf("parser fallback failed: %#v", mgr)
	}
}

func Test_MCPProvider_Invoke_unknown_name_errors(t *testing.T) {
	// Given
	ctx := context.Background()
	p := NewMCPProvider(MCPProviderConfig{Caller: &fakeMCPManager{}, Servers: []string{"calc"}})

	// When
	_, err := p.Invoke(ctx, Request{CallID: "c", Name: "not-an-mcp-name", Arguments: json.RawMessage(`{}`)})

	// Then
	if !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("expected ErrUnknownTool, got %v", err)
	}
}

func Test_MCPProvider_Tools_skips_server_with_list_error(t *testing.T) {
	// Given calc lists fine, broken returns an error.
	ctx := context.Background()
	mgr := &fakeMCPManager{
		tools:   map[string][]mcp.Tool{"calc": {{Name: "add"}}},
		listErr: map[string]error{"broken": errors.New("boom")},
	}
	p := NewMCPProvider(MCPProviderConfig{Caller: mgr, Servers: []string{"broken", "calc"}})

	// When
	descs, err := p.Tools(ctx, addr(), user())

	// Then
	if err != nil {
		t.Fatalf("tools should not fail when one server errors: %v", err)
	}
	if len(descs) != 1 || descs[0].Name != "mcp.calc.add" {
		t.Fatalf("expected only calc's tool, got %#v", descs)
	}
}

func addr() protocol.MessageAddress { return protocol.MessageAddress{} }
func user() protocol.ChatMessage    { return protocol.ChatMessage{} }
