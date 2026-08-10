package provider

import (
	"errors"
	"testing"
)

func Test_NewOpenAICompatClient_accepts_sidecar_url(t *testing.T) {
	client, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: "http://127.0.0.1:8787"})

	if err != nil {
		t.Fatalf("expected sidecar config: %v", err)
	}
	if client.BaseURL() != "http://127.0.0.1:8787" {
		t.Fatalf("unexpected base url %q", client.BaseURL())
	}
}

func TestOpenAICompatEndpointAcceptsVersionedBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		chatPath string
		want     string
	}{
		{name: "root base", baseURL: "https://api.example.test", want: "https://api.example.test/v1/chat/completions"},
		{name: "versioned base", baseURL: "https://api.example.test/v1", want: "https://api.example.test/v1/chat/completions"},
		{name: "versioned base slash", baseURL: "https://api.example.test/v1/", want: "https://api.example.test/v1/chat/completions"},
		{name: "prefixed versioned base", baseURL: "https://api.example.test/openai/v1", want: "https://api.example.test/openai/v1/chat/completions"},
		{name: "custom path", baseURL: "https://api.example.test/v1", chatPath: "/responses", want: "https://api.example.test/v1/responses"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: tt.baseURL, ChatPath: tt.chatPath})
			if err != nil {
				t.Fatal(err)
			}
			if got := client.endpoint(); got != tt.want {
				t.Fatalf("endpoint = %q, want %q", got, tt.want)
			}
		})
	}
}

func Test_NewOpenAICompatClient_rejects_direct_provider_url(t *testing.T) {
	_, err := NewOpenAICompatClient(OpenAICompatConfig{BaseURL: "https://api.openai.com/v1", Guard: SandboxGuard(false)})

	if !errors.Is(err, ErrDirectProviderURL) {
		t.Fatalf("expected ErrDirectProviderURL, got %v", err)
	}
}

func Test_NewOpenAICompatClient_rejects_provider_secret(t *testing.T) {
	_, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL:    "http://127.0.0.1:8787",
		AuthHeader: "sk-real-key",
		Guard:      SandboxGuard(false),
	})

	if !errors.Is(err, ErrProviderSecret) {
		t.Fatalf("expected ErrProviderSecret, got %v", err)
	}
}
