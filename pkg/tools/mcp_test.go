// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

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

type fakeMCPResourceClient struct {
	server       string
	listReq      mcp.ListResourcesRequest
	readReq      mcp.ReadResourceRequest
	templatesReq mcp.ListResourceTemplatesRequest
}

func (f *fakeMCPResourceClient) ListResources(_ context.Context, server string, req mcp.ListResourcesRequest) (mcp.ListResourcesResult, error) {
	f.server = server
	f.listReq = req
	return mcp.ListResourcesResult{
		Resources:  []mcp.ResourceDescriptor{{URI: "file:///tmp/plan.md", Name: "plan", MimeType: "text/markdown"}},
		NextCursor: "next-resources",
	}, nil
}

func (f *fakeMCPResourceClient) ReadResource(_ context.Context, server string, req mcp.ReadResourceRequest) (mcp.ReadResourceResult, error) {
	f.server = server
	f.readReq = req
	return mcp.ReadResourceResult{
		Contents: []mcp.ResourceContent{{URI: req.URI, MimeType: "text/markdown", Text: "# Plan\n"}},
	}, nil
}

func (f *fakeMCPResourceClient) ListResourceTemplates(_ context.Context, server string, req mcp.ListResourceTemplatesRequest) (mcp.ListResourceTemplatesResult, error) {
	f.server = server
	f.templatesReq = req
	return mcp.ListResourceTemplatesResult{
		ResourceTemplates: []mcp.ResourceTemplateDescriptor{{URITemplate: "file:///{path}", Name: "workspace file"}},
		NextCursor:        "next-templates",
	}, nil
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

func Test_RegisterMCPResourceTools_invoke_resource_client_with_typed_arguments(t *testing.T) {
	// Given
	ctx := context.Background()
	client := &fakeMCPResourceClient{}
	reg := NewRegistry()
	cfg := MCPResourceConfig{RegistryName: "mcp.docs.resources", ServerName: "docs", Client: client}
	if err := RegisterMCPListResourcesTool(reg, cfg); err != nil {
		t.Fatalf("register list resources: %v", err)
	}
	if err := RegisterMCPReadResourceTool(reg, MCPResourceConfig{RegistryName: "mcp.docs.read", ServerName: "docs", Client: client}); err != nil {
		t.Fatalf("register read resource: %v", err)
	}
	if err := RegisterMCPListResourceTemplatesTool(reg, MCPResourceConfig{RegistryName: "mcp.docs.templates", ServerName: "docs", Client: client}); err != nil {
		t.Fatalf("register list resource templates: %v", err)
	}

	// When
	resources, listErr := reg.Invoke(ctx, Request{CallID: "list-resources", Name: "mcp.docs.resources", Arguments: json.RawMessage(`{"cursor":"page-1"}`)})
	read, readErr := reg.Invoke(ctx, Request{CallID: "read-resource", Name: "mcp.docs.read", Arguments: json.RawMessage(`{"uri":"file:///tmp/plan.md"}`)})
	templates, templateErr := reg.Invoke(ctx, Request{CallID: "list-templates", Name: "mcp.docs.templates", Arguments: json.RawMessage(`{"cursor":"template-page-1"}`)})

	// Then
	if listErr != nil {
		t.Fatalf("list resources: %v", listErr)
	}
	if readErr != nil {
		t.Fatalf("read resource: %v", readErr)
	}
	if templateErr != nil {
		t.Fatalf("list templates: %v", templateErr)
	}
	if client.server != "docs" || client.listReq.Cursor != "page-1" || client.readReq.URI != "file:///tmp/plan.md" || client.templatesReq.Cursor != "template-page-1" {
		t.Fatalf("unexpected client calls: %#v", client)
	}
	if !json.Valid(resources.Payload) || !json.Valid(read.Payload) || !json.Valid(templates.Payload) {
		t.Fatalf("expected JSON payloads: %#v %#v %#v", resources, read, templates)
	}
}
