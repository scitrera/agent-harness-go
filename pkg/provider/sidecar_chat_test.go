package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func Test_OpenAICompatClient_Chat_posts_scitrera_messages_to_sidecar(t *testing.T) {
	// Given
	ctx := context.Background()
	var got ChatRequest
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{"message":{"id":"a1","role":"assistant","addr":{"thread_id":"thread-1"},"content":[{"type":"text","text":"ok"}]}}`))
	}))
	defer server.Close()
	client, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: server.URL, AuthHeader: "Bearer placeholder-rewrite", HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("sidecar client: %v", err)
	}
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}

	// When
	resp, err := client.Chat(ctx, ChatRequest{Model: "scitrera-test", Messages: []protocol.ChatMessage{{ID: "u1", Role: protocol.RoleUser, Addr: protocol.MessageAddress{ThreadID: "thread-1"}, Content: []protocol.ContentPart{part}}}})

	// Then
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if got.Model != "scitrera-test" || len(got.Messages) != 1 || got.Messages[0].ID != "u1" {
		t.Fatalf("unexpected outbound request: %#v", got)
	}
	if gotAuth != "Bearer placeholder-rewrite" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if resp.Message.ID != "a1" || resp.Message.Role != protocol.RoleAssistant {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func Test_OpenAICompatClient_Chat_accepts_versioned_base_url_with_shared_transport(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %q, want /v1/chat/completions", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chat_1","model":"served","choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer server.Close()

	client, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL: server.URL + "/v1", Format: FormatOpenAI, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	user, _ := protocol.NewTextPart("hello")
	response, err := client.Chat(context.Background(), ChatRequest{
		Model: "logical", Messages: []protocol.ChatMessage{{Role: protocol.RoleUser, Content: []protocol.ContentPart{user}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Model != "served" {
		t.Fatalf("response model = %q", response.Model)
	}
}

func Test_OpenAICompatClient_Chat_decodes_openai_compatible_response_when_sidecar_returns_choices(t *testing.T) {
	// Given
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"chatcmpl-1","choices":[{"message":{"role":"assistant","content":"hello"}}]}`))
	}))
	defer server.Close()
	client, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("sidecar client: %v", err)
	}

	// When
	resp, err := client.Chat(ctx, ChatRequest{Messages: []protocol.ChatMessage{}})

	// Then
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	tp, ok := resp.Message.Content[0].AsText()
	if !ok || tp.Text != "hello" {
		t.Fatalf("unexpected text: %q %v", tp.Text, ok)
	}
}
