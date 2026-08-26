package model

import (
	"encoding/json"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// MetaActiveModel is the assistant-message metadata key carrying the worker's
// authoritative model selection for the addressed workspace/thread. Remote UI
// clients use it to mirror the worker state without parsing command text or
// assuming their local model registry has the same default.
const MetaActiveModel = "active_model"

// StampActiveModel records name on message when it is non-empty. Existing
// metadata is preserved.
func StampActiveModel(message *protocol.ChatMessage, name string) {
	if message == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	raw, err := json.Marshal(name)
	if err != nil {
		return
	}
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[MetaActiveModel] = raw
}

// ActiveModelFromMessage returns the authoritative model stamped by the
// worker. Malformed or absent metadata is treated as unknown.
func ActiveModelFromMessage(message protocol.ChatMessage) string {
	raw := message.Meta[MetaActiveModel]
	if len(raw) == 0 {
		return ""
	}
	var name string
	if err := json.Unmarshal(raw, &name); err != nil {
		return ""
	}
	return strings.TrimSpace(name)
}
