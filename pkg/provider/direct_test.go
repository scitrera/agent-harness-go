package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestSandboxGuardRejectsRealKey(t *testing.T) {
	_, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL:    "http://127.0.0.1:8787",
		AuthHeader: "sk-realisticlooking123",
		Guard:      SandboxGuard(false),
	})
	if !errors.Is(err, ErrProviderSecret) {
		t.Fatalf("expected ErrProviderSecret under sandbox guard, got %v", err)
	}
}

func TestSandboxGuardAllowDirectAcceptsRealKeyAndDirectHost(t *testing.T) {
	c, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL:    "https://api.openai.com",
		AuthHeader: "sk-realisticlooking123",
		Guard:      SandboxGuard(true),
	})
	if err != nil {
		t.Fatalf("allow-direct guard should accept real key + direct host: %v", err)
	}
	if c == nil {
		t.Fatal("nil client")
	}
}

func TestSandboxGuardRejectsDirectHost(t *testing.T) {
	_, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL:    "https://api.openai.com",
		AuthHeader: "Bearer placeholder-sidecar-rewrite",
		Guard:      SandboxGuard(false),
	})
	if !errors.Is(err, ErrDirectProviderURL) {
		t.Fatalf("expected ErrDirectProviderURL under sandbox guard, got %v", err)
	}
}

func TestNoGuardIsPermissive(t *testing.T) {
	// Core default: no guard => a direct host + real-looking key is accepted.
	c, err := NewOpenAICompatClient(OpenAICompatConfig{
		BaseURL:    "https://api.openai.com",
		AuthHeader: "sk-realisticlooking123",
	})
	if err != nil || c == nil {
		t.Fatalf("no guard should be permissive, got err=%v", err)
	}
}

func TestOpenAIEncodingDropsSpecFields(t *testing.T) {
	sys := spec.NewChatMessage("bootstrap-context", spec.RoleSystem)
	sys.Addr = spec.MessageAddress{ThreadID: "t1"}
	sys.Content = []spec.ContentPart{spec.NewTextPart("you are helpful")}
	user := spec.NewChatMessage("cli-user-1", spec.RoleUser)
	user.Content = []spec.ContentPart{spec.NewTextPart("hi "), spec.NewTextPart("there")}

	body, err := json.Marshal(toOpenAIRequest(ChatRequest{
		Model:    "m",
		Messages: []protocol.ChatMessage{sys, user},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Messages) != 2 {
		t.Fatalf("messages = %d", len(decoded.Messages))
	}
	if decoded.Messages[0].Role != "system" || decoded.Messages[0].Content != "you are helpful" {
		t.Fatalf("system msg = %+v", decoded.Messages[0])
	}
	if decoded.Messages[1].Role != "user" || decoded.Messages[1].Content != "hi there" {
		t.Fatalf("user msg = %+v", decoded.Messages[1])
	}
	// No native spec fields should leak onto the wire.
	for _, banned := range []string{"schema_version", "\"id\"", "addr", "\"meta\""} {
		if bytes.Contains(body, []byte(banned)) {
			t.Fatalf("openai request leaked %q: %s", banned, body)
		}
	}
}
