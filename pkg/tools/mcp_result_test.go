// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC
package tools

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"strings"
	"testing"
)

type imageMCPCaller struct{ result mcp.CallToolResult }

func (c imageMCPCaller) CallTool(context.Context, string, string, json.RawMessage) (mcp.CallToolResult, error) {
	return c.result, nil
}
func TestMCPImagesReachModelThroughBothAdapters(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("synthetic image payload"))
	original := mcp.CallToolResult{Content: []mcp.Content{{Type: "text", Text: "Captured the preview"}, {Type: "image", MimeType: "image/png", Data: encoded}}}
	for _, dynamic := range []bool{false, true} {
		caller := imageMCPCaller{result: original}
		req := Request{CallID: "image-call", Name: "mcp.browser.screenshot", Arguments: json.RawMessage(`{}`)}
		var result Result
		var err error
		if dynamic {
			result, err = NewMCPProvider(MCPProviderConfig{Caller: caller, Servers: []string{"browser"}}).Invoke(context.Background(), req)
		} else {
			registry := NewRegistry()
			err = RegisterMCPTool(registry, MCPToolConfig{RegistryName: req.Name, ServerName: "browser", ToolName: "screenshot", Caller: caller})
			if err != nil {
				t.Fatal(err)
			}
			result, err = registry.Invoke(context.Background(), req)
		}
		if err != nil {
			t.Fatal(err)
		}
		if result.IsError || len(result.InjectUserParts) != 2 {
			t.Fatalf("dynamic=%v: image did not reach user content: %#v", dynamic, result)
		}
		image, ok := result.InjectUserParts[1].AsImage()
		if !ok || image.DataURI != "data:image/png;base64,"+encoded {
			t.Fatal("model image is missing or altered")
		}
		if strings.Contains(string(result.Payload), encoded) || !strings.Contains(string(result.Payload), "Captured the preview") {
			t.Fatal("text evidence lost or base64 duplicated into tool JSON")
		}
		if original.Content[1].Data != encoded {
			t.Fatal("conversion mutated the caller's result")
		}
	}
}
func TestMCPResultPreservesErrorsAndBoundsImages(t *testing.T) {
	req := Request{CallID: "call", Name: "mcp.browser.screenshot"}
	result, err := resultFromMCP(req, mcp.CallToolResult{IsError: true, Content: []mcp.Content{{Type: "text", Text: "Browser failed"}}})
	if err != nil || !result.IsError {
		t.Fatal("upstream error flag was dropped")
	}
	for _, part := range []mcp.Content{{Type: "image", MimeType: "image/png", Data: "bad base64!"}, {Type: "image", MimeType: "text/plain", Data: "YWJj"}, {Type: "image", MimeType: "image/png", Data: strings.Repeat("A", base64.StdEncoding.EncodedLen(defaultInjectImageMaxBytes)+1)}} {
		result, err = resultFromMCP(req, mcp.CallToolResult{Content: []mcp.Content{part}})
		if err != nil || !result.IsError || len(result.InjectUserParts) != 0 {
			t.Fatal("invalid image was not bounded")
		}
		if len(result.Payload) > 500 {
			t.Fatal("invalid image leaked into tool JSON")
		}
	}
}
