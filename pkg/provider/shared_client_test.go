// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	llmclient "github.com/scitrera/go-llm/client"
	llmprotocol "github.com/scitrera/go-llm/protocol"
	"github.com/scitrera/go-llm/protocol/codec"
	openaiprovider "github.com/scitrera/go-llm/provider/openai"
)

func TestResponsesApplyPatchUsesNativeOrSubscriptionCustomProfile(t *testing.T) {
	request, err := sharedProtocolRequest(ChatRequest{Tools: []ToolSpec{{Name: "apply_patch", Parameters: json.RawMessage(`{"type":"object"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	direct := &OpenAICompatClient{format: FormatResponses}
	direct.prepareSharedRequest(&request)
	if request.Tools[0].Type != "apply_patch" || string(request.Tools[0].Raw) != `{"type":"apply_patch"}` {
		t.Fatalf("native Responses tool = %#v", request.Tools[0])
	}

	request, err = sharedProtocolRequest(ChatRequest{Tools: []ToolSpec{{Name: "apply_patch", Parameters: json.RawMessage(`{"type":"object"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	subscription := &OpenAICompatClient{format: FormatResponses, prepareRequest: openaiprovider.PrepareSubscriptionRequest}
	subscription.prepareSharedRequest(&request)
	if request.Tools[0].Type != "custom" || !strings.Contains(string(request.Tools[0].Raw), `"syntax":"lark"`) {
		t.Fatalf("subscription custom tool = %#v", request.Tools[0])
	}
}

func TestResponsesClientSupportsDynamicRecoveringAuthAndRequestProfile(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	auth := &testRecoveringAuth{token: "expired"}
	var received map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Authorization") != "Bearer fresh" {
			writer.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(writer, `{"error":{"message":"expired"}}`)
			return
		}
		_ = json.NewDecoder(request.Body).Decode(&received)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"id":"resp_1","model":"served","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)
	}))
	defer server.Close()
	client, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL: server.URL, Format: FormatResponses, HTTPClient: server.Client(), Auth: auth,
		PrepareRequest: func(request *llmprotocol.Request) {
			store := false
			request.State.Store = &store
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	part, _ := protocol.NewTextPart("hello")
	if _, err := client.Chat(context.Background(), ChatRequest{Model: "model", Messages: []protocol.ChatMessage{{Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}}}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || auth.recoveries.Load() != 1 || string(received["store"]) != "false" {
		t.Fatalf("calls=%d recoveries=%d body=%v", calls.Load(), auth.recoveries.Load(), received)
	}
}

type testRecoveringAuth struct {
	token      string
	recoveries atomic.Int64
}

func (a *testRecoveringAuth) Apply(_ context.Context, request *http.Request) error {
	request.Header.Set("Authorization", "Bearer "+a.token)
	return nil
}

func (a *testRecoveringAuth) RecoverUnauthorized(context.Context) error {
	a.recoveries.Add(1)
	a.token = "fresh"
	return nil
}

var _ llmclient.Authenticator = (*testRecoveringAuth)(nil)

func TestResponsesClientUsesSharedProtocolAndTransport(t *testing.T) {
	t.Parallel()
	var received map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/responses" {
			t.Errorf("path = %q, want /v1/responses", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("X-Scitrera-Workspace") != "workspace-a" ||
			request.Header.Get("X-Scitrera-Thread-Id") != "thread-a" {
			t.Errorf("attribution headers = %#v", request.Header)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{
			"id":"resp_1","object":"response","model":"served","status":"completed",
			"output":[
				{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"hello"}]},
				{"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{\"city\":\"Chicago\"}"}
			],
			"usage":{"input_tokens":8,"output_tokens":3,"total_tokens":11}
		}`)
	}))
	defer server.Close()

	client, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL: server.URL, AuthHeader: "Bearer test-token",
		Format: FormatResponses, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	system, _ := protocol.NewTextPart("be precise")
	workspace, _ := protocol.NewTextPart("follow workspace guidance")
	user, _ := protocol.NewTextPart("weather?")
	ctx := WithAttribution(context.Background(), Attribution{Workspace: "workspace-a", ThreadID: "thread-a"})
	response, err := client.Chat(ctx, ChatRequest{
		Model: "logical", ReasoningEffort: "xhigh",
		Messages: []protocol.ChatMessage{
			{Role: protocol.RoleSystem, Content: []protocol.ContentPart{system}},
			{Role: protocol.RoleSystem, Content: []protocol.ContentPart{workspace}},
			{Role: protocol.RoleUser, Content: []protocol.ContentPart{user}},
		},
		Tools: []ToolSpec{{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var instructions string
	if err := json.Unmarshal(received["instructions"], &instructions); err != nil || instructions != "be precise\n\nfollow workspace guidance" {
		t.Fatalf("Responses instructions = %q, err = %v", instructions, err)
	}
	if _, ok := received["instructions"]; !ok ||
		!strings.Contains(string(received["input"]), "weather?") ||
		!strings.Contains(string(received["tools"]), "lookup") ||
		!strings.Contains(string(received["reasoning"]), `"effort":"xhigh"`) {
		t.Fatalf("Responses request = %#v", received)
	}
	if response.Model != "served" || response.Usage.TotalTokens != 11 || len(response.Message.Content) != 2 {
		t.Fatalf("response = %#v", response)
	}
	call, ok := response.Message.Content[1].AsToolCall()
	if !ok || call.ID != "call_1" || call.Name != "lookup" || string(call.Args["city"]) != `"Chicago"` {
		t.Fatalf("tool call = %#v, %v", call, ok)
	}
}

func TestResponsesRequestPreservesEvictedToolResultEnvelope(t *testing.T) {
	t.Parallel()
	call, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call_1", Name: "read_file"})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(strings.Repeat("large tool output\n", 20))
	result, err := protocol.NewToolResultPart("call_1", "read_file", payload, false)
	if err != nil {
		t.Fatal(err)
	}
	history := []protocol.ChatMessage{
		{Role: protocol.RoleAssistant, Content: []protocol.ContentPart{call}},
		{Role: protocol.RoleToolResult, Content: []protocol.ContentPart{result}},
	}
	for index := 0; index < 6; index++ {
		part, _ := protocol.NewTextPart("recent")
		history = append(history, protocol.ChatMessage{Role: protocol.RoleUser, Content: []protocol.ContentPart{part}})
	}
	report, err := (compaction.EvictingCompactor{
		Sink: responseTestEvictionSink{}, KeepRecent: 6, ToolResultEvictTokens: 1,
	}).Compact(context.Background(), history, compaction.Config{})
	if err != nil {
		t.Fatal(err)
	}
	request, err := sharedProtocolRequest(ChatRequest{Model: "logical", Messages: sanitizeTranscript(report.Messages)})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := (codec.OpenAIResponses{}).EncodeRequest(request, llmprotocol.StrictPolicy())
	if err != nil {
		t.Fatal(err)
	}
	body := string(encoded.Body)
	if !strings.Contains(body, `"type":"function_call_output"`) || !strings.Contains(body, `"call_id":"call_1"`) {
		t.Fatalf("evicted tool result lost Responses call linkage: %s", body)
	}
	if strings.Contains(body, `"role":"tool"`) {
		t.Fatalf("invalid tool-role message reached Responses input: %s", body)
	}
}

type responseTestEvictionSink struct{}

func (responseTestEvictionSink) Put(context.Context, string, []byte) (string, error) {
	return "workspace://.sahara/evicted/call_1.txt", nil
}

func TestResponsesClientStreamsTypedEvents(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/responses" {
			t.Errorf("path = %q", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		for _, event := range []string{
			"response.created|{\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"served\",\"status\":\"in_progress\"}}",
			"response.output_item.added|{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_1\",\"role\":\"assistant\"}}",
			"response.output_text.delta|{\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_1\",\"delta\":\"hello\"}",
			"response.output_item.done|{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_1\"}}",
			"response.completed|{\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"served\",\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":1,\"total_tokens\":5}}}",
		} {
			name, payload, _ := strings.Cut(event, "|")
			_, _ = io.WriteString(writer, "event: "+name+"\ndata: "+payload+"\n\n")
			writer.(http.Flusher).Flush()
		}
	}))
	defer server.Close()

	client, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL: server.URL, Format: FormatResponses, HTTPClient: server.Client(), StreamUsage: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	user, _ := protocol.NewTextPart("hello")
	var deltas strings.Builder
	response, err := client.ChatStream(context.Background(), ChatRequest{
		Model: "logical", Messages: []protocol.ChatMessage{{Role: protocol.RoleUser, Content: []protocol.ContentPart{user}}},
	}, func(_ DeltaKind, delta string) error {
		deltas.WriteString(delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text, ok := response.Message.Content[0].AsText()
	if !ok || text.Text != "hello" || deltas.String() != "hello" || response.Usage.TotalTokens != 5 {
		t.Fatalf("response = %#v, deltas = %q", response, deltas.String())
	}
}
