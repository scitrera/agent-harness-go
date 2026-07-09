package provider

import (
	"bytes"
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestToolSpecMarshalsToFunctionShape(t *testing.T) {
	body, err := json.Marshal(ToolSpec{Name: "read_file", Description: "read a file", Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["type"] != "function" {
		t.Fatalf("type = %v", m["type"])
	}
	fn, _ := m["function"].(map[string]any)
	if fn["name"] != "read_file" || fn["description"] != "read a file" {
		t.Fatalf("function = %v", fn)
	}
	if _, ok := fn["parameters"].(map[string]any); !ok {
		t.Fatalf("parameters not an object: %v", fn["parameters"])
	}
}

func TestToolSpecDefaultsParameters(t *testing.T) {
	body, _ := json.Marshal(ToolSpec{Name: "x"})
	if !bytes.Contains(body, []byte(`"parameters":{"type":"object"}`)) {
		t.Fatalf("expected default object schema: %s", body)
	}
}

func TestOpenAIRequestIncludesTools(t *testing.T) {
	body, err := json.Marshal(toOpenAIRequest(ChatRequest{
		Model: "m",
		Tools: []ToolSpec{{Name: "read_file", Description: "d"}},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(body, []byte(`"tools"`)) || !bytes.Contains(body, []byte(`"read_file"`)) {
		t.Fatalf("openai request missing tools: %s", body)
	}
}

func TestOpenAIEncodingDropsMetaCacheHint(t *testing.T) {
	sys := spec.NewChatMessage("system-prompt", spec.RoleSystem)
	sys.Content = []spec.ContentPart{spec.NewTextPart("base instructions")}
	sys.Meta = map[string]json.RawMessage{"scitrera": json.RawMessage(`{"cache":{"stable_prefix_chars":17}}`)}

	body, err := json.Marshal(toOpenAIRequest(ChatRequest{Messages: []protocol.ChatMessage{sys}}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(body, []byte("scitrera")) || bytes.Contains(body, []byte("stable_prefix_chars")) {
		t.Fatalf("openai request leaked meta cache hint: %s", body)
	}
}

func TestPromptCachingStampsNativeBreakpoint(t *testing.T) {
	sys := spec.NewChatMessage("sys", spec.RoleSystem)
	sys.Content = []spec.ContentPart{spec.NewTextPart("base instructions")} // 17 chars
	usr := spec.NewChatMessage("u", spec.RoleUser)
	usr.Content = []spec.ContentPart{spec.NewTextPart("hello")}
	req := ChatRequest{Messages: []protocol.ChatMessage{sys, usr}}

	// native + PromptCaching=true → breakpoint emitted at end of system prefix.
	on := &SidecarClient{format: FormatNative, promptCaching: true}
	body, err := on.encodeRequest(req)
	if err != nil {
		t.Fatalf("encode (caching on): %v", err)
	}
	if !bytes.Contains(body, []byte(`"stable_prefix_chars":17`)) {
		t.Fatalf("expected cache breakpoint at end of system prefix: %s", body)
	}
	// The caller's message must not be mutated in place.
	if _, ok := sys.Meta["scitrera"]; ok {
		t.Fatalf("stamping leaked into caller's message meta: %v", sys.Meta)
	}

	// native + PromptCaching=false → no breakpoint.
	off := &SidecarClient{format: FormatNative, promptCaching: false}
	body, err = off.encodeRequest(req)
	if err != nil {
		t.Fatalf("encode (caching off): %v", err)
	}
	if bytes.Contains(body, []byte("stable_prefix_chars")) {
		t.Fatalf("cache breakpoint emitted with caching disabled: %s", body)
	}

	// openai + PromptCaching=true → no-op (openai path drops the hint).
	oai := &SidecarClient{format: FormatOpenAI, promptCaching: true}
	body, err = oai.encodeRequest(req)
	if err != nil {
		t.Fatalf("encode (openai): %v", err)
	}
	if bytes.Contains(body, []byte("stable_prefix_chars")) || bytes.Contains(body, []byte("scitrera")) {
		t.Fatalf("openai path leaked cache breakpoint: %s", body)
	}
}

func TestDecodeOpenAIToolCalls(t *testing.T) {
	data := []byte(`{"id":"a1","choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"SOUL.md\"}"}}]}}]}`)
	resp, err := decodeChatResponse(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Message.Content) != 1 {
		t.Fatalf("expected 1 tool_call part, got %d", len(resp.Message.Content))
	}
	tc, ok := resp.Message.Content[0].AsToolCall()
	if !ok {
		t.Fatalf("not a tool_call part: %s", resp.Message.Content[0].Raw())
	}
	if tc.ID != "call_1" || tc.Name != "read_file" {
		t.Fatalf("tool_call = %+v", tc)
	}
	if string(tc.Args["path"]) != `"SOUL.md"` {
		t.Fatalf("args = %v", tc.Args)
	}
}

func TestDecodeOpenAIToolCallsWithText(t *testing.T) {
	data := []byte(`{"id":"a1","choices":[{"message":{"role":"assistant","content":"let me check","tool_calls":[{"id":"c1","function":{"name":"list_dir","arguments":"{}"}}]}}]}`)
	resp, err := decodeChatResponse(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Message.Content) != 2 {
		t.Fatalf("expected text + tool_call, got %d parts", len(resp.Message.Content))
	}
	if tp, ok := resp.Message.Content[0].AsText(); !ok || tp.Text != "let me check" {
		t.Fatalf("first part should be text: %v", resp.Message.Content[0].Raw())
	}
}
