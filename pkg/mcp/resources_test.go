package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func Test_Manager_ListResources_ReadResource_and_ListResourceTemplates_use_mcp_stdio_jsonrpc(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager := NewManager(time.Now)
	executable := currentTestExecutable(t)
	if err := manager.Register(ServerConfig{
		Name:    "docs",
		Command: executable,
		Args:    []string{"-test.run=Test_FakeMCPServer"},
		Env:     []string{"AGENT_HARNESS_FAKE_MCP=1"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	templates, templateErr := manager.ListResourceTemplates(ctx, "docs", ListResourceTemplatesRequest{Cursor: "template-page-1"})
	resources, listErr := manager.ListResources(ctx, "docs", ListResourcesRequest{Cursor: "resource-page-1"})
	read, readErr := manager.ReadResource(ctx, "docs", ReadResourceRequest{URI: "file:///tmp/plan.md"})
	stopErr := manager.Stop(ctx, "docs")

	// Then
	if templateErr != nil {
		t.Fatalf("list templates: %v", templateErr)
	}
	if listErr != nil {
		t.Fatalf("list resources: %v", listErr)
	}
	if readErr != nil {
		t.Fatalf("read resource: %v", readErr)
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}
	if templates.NextCursor != "template-page-2" || len(templates.ResourceTemplates) != 1 {
		t.Fatalf("unexpected templates: %#v", templates)
	}
	if templates.ResourceTemplates[0].URITemplate != "file:///{path}" || templates.ResourceTemplates[0].Name != "workspace file" {
		t.Fatalf("unexpected template descriptor: %#v", templates.ResourceTemplates[0])
	}
	if resources.NextCursor != "resource-page-2" || len(resources.Resources) != 1 {
		t.Fatalf("unexpected resources: %#v", resources)
	}
	if resources.Resources[0].URI != "file:///tmp/plan.md" || resources.Resources[0].MimeType != "text/markdown" {
		t.Fatalf("unexpected resource descriptor: %#v", resources.Resources[0])
	}
	if len(read.Contents) != 1 || read.Contents[0].Text != "# Plan\n" || read.Contents[0].URI != "file:///tmp/plan.md" {
		t.Fatalf("unexpected resource read: %#v", read)
	}
	if len(templates.Raw) == 0 || len(resources.Raw) == 0 || len(read.Raw) == 0 {
		t.Fatalf("expected raw protocol payloads to be preserved")
	}
}

func Test_Manager_ResourceMethods_return_typed_unsupported_when_server_has_no_resources(t *testing.T) {
	// Given
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manager := NewManager(time.Now)
	executable := currentTestExecutable(t)
	if err := manager.Register(ServerConfig{
		Name:    "tools-only",
		Command: executable,
		Args:    []string{"-test.run=Test_FakeMCPServer"},
		Env:     []string{"AGENT_HARNESS_FAKE_MCP=1", "AGENT_HARNESS_FAKE_MCP_UNSUPPORTED_RESOURCES=1"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	_, listErr := manager.ListResources(ctx, "tools-only", ListResourcesRequest{})
	_, templateErr := manager.ListResourceTemplates(ctx, "tools-only", ListResourceTemplatesRequest{})
	_, readErr := manager.ReadResource(ctx, "tools-only", ReadResourceRequest{URI: "file:///tmp/plan.md"})
	stopErr := manager.Stop(ctx, "tools-only")

	// Then
	requireUnsupportedRPCError(t, listErr)
	requireUnsupportedRPCError(t, templateErr)
	requireUnsupportedRPCError(t, readErr)
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}
}

func fakeResourcesList(params json.RawMessage) map[string]interface{} {
	var req ListResourcesRequest
	_ = json.Unmarshal(params, &req)
	if req.Cursor == "empty" {
		return map[string]interface{}{"resources": []map[string]interface{}{}}
	}
	return map[string]interface{}{
		"resources": []map[string]interface{}{{
			"uri":         "file:///tmp/plan.md",
			"name":        "plan",
			"description": "current plan",
			"mimeType":    "text/markdown",
		}},
		"nextCursor": "resource-page-2",
	}
}

func fakeResourceRead(params json.RawMessage) map[string]interface{} {
	var req ReadResourceRequest
	_ = json.Unmarshal(params, &req)
	return map[string]interface{}{
		"contents": []map[string]interface{}{{
			"uri":      req.URI,
			"mimeType": "text/markdown",
			"text":     "# Plan\n",
		}},
	}
}

func fakeResourceTemplatesList(params json.RawMessage) map[string]interface{} {
	var req ListResourceTemplatesRequest
	_ = json.Unmarshal(params, &req)
	if req.Cursor == "empty" {
		return map[string]interface{}{"resourceTemplates": []map[string]interface{}{}}
	}
	return map[string]interface{}{
		"resourceTemplates": []map[string]interface{}{{
			"uriTemplate": "file:///{path}",
			"name":        "workspace file",
			"description": "read a file by path",
			"mimeType":    "text/plain",
		}},
		"nextCursor": "template-page-2",
	}
}

func requireUnsupportedRPCError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected unsupported RPC error")
	}
	if !IsUnsupported(err) {
		t.Fatalf("expected unsupported marker for %v", err)
	}
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != codeMethodNotFound {
		t.Fatalf("expected typed method-not-found RPC error, got %#v", err)
	}
}
