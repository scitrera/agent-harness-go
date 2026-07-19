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
