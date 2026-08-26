package model

import (
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestActiveModelMessageMetadata(t *testing.T) {
	message := protocol.ChatMessage{Meta: map[string]json.RawMessage{"other": json.RawMessage(`true`)}}
	StampActiveModel(&message, "  gpt-5.6-sol  ")
	if got := ActiveModelFromMessage(message); got != "gpt-5.6-sol" {
		t.Fatalf("active model = %q", got)
	}
	if string(message.Meta["other"]) != "true" {
		t.Fatalf("unrelated metadata was changed: %#v", message.Meta)
	}

	message.Meta[MetaActiveModel] = json.RawMessage(`{"not":"a string"}`)
	if got := ActiveModelFromMessage(message); got != "" {
		t.Fatalf("malformed active model = %q, want empty", got)
	}
}
