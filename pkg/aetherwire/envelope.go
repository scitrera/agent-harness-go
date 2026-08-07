// Package aetherwire is the Aether transport binding for the chat-stream
// protocol: the task-message topic and the message envelope that wraps a spec
// stream event. It is the shared, authoritative encoder/decoder for this wire
// shape — every process that speaks chat over Aether (this harness, the sandbox
// sidecar, distribution servers) encodes through it rather than keeping a
// private copy, which is how the shape stays consistent across them.
//
// The pure message spec lives in github.com/scitrera/ecosystem-messaging-spec/go;
// this package holds only the Aether-routing layer on top of it.
//
// A few readers here (AuthorityGrantID, Ephemeral, SynthesizeSystemPrompt) decode
// meta keys that the Scitrera distribution sets. They are kept here, in the one
// authoritative codec, so a distribution does not need a forked copy to read
// them; nothing in oss requires those keys to be present, and every reader
// degrades to a zero value when they are absent.
package aetherwire

import (
	"encoding/json"
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// TaskMessageTopic is the per-turn message lane every UI client for the turn
// subscribes to: tk::<workspace>::<task_id>::msg.
func TaskMessageTopic(workspace, taskID string) string {
	return "tk::" + workspace + "::" + taskID + "::msg"
}

// TurnMeta is the routing/identity context for a turn's stream egress.
type TurnMeta struct {
	// AppWorkspace is the app-server workspace the turn belongs to (routing key).
	AppWorkspace string
	// TaskID is the Aether chat task for this turn.
	TaskID string
	// ThreadID is the conversation thread.
	ThreadID string
	// WindowID is the originating UI window (request_id).
	WindowID string
	// SourceAgent is the emitter identity string, e.g. "agent-harness:<sandbox_id>".
	SourceAgent string
}

// StreamEnvelope wraps a spec stream event in the AIR message envelope and
// returns the JSON payload to publish on TaskMessageTopic. The shape mirrors
// the sandbox sidecar's emitter exactly so app-server's _on_air_message keys off
// options.chat_stream_event unchanged.
func StreamEnvelope(meta TurnMeta, event spec.StreamEvent) ([]byte, error) {
	if event == nil {
		return nil, fmt.Errorf("aetherwire: nil stream event")
	}
	evRaw, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("aetherwire: marshal stream event: %w", err)
	}
	options := map[string]any{
		"thread_id":         meta.ThreadID,
		"chat_stream_event": json.RawMessage(evRaw),
	}
	if meta.AppWorkspace != "" {
		options["app_workspace"] = meta.AppWorkspace
	}
	env := map[string]any{
		"source":     map[string]any{"workspace": "", "agent": meta.SourceAgent},
		"content":    map[string]any{"text": "", "role": "agent"},
		"request_id": meta.WindowID,
		"options":    options,
		"workspace":  nullableString(meta.AppWorkspace),
		"initiator":  map[string]any{"workspace": nullableString(meta.AppWorkspace), "request_id": meta.WindowID},
	}
	return json.Marshal(env)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// WithAgentName returns a copy of msg with the assistant's display persona name
// set at meta.scitrera.agent_name (nested, mirroring the authority_grant_id
// convention read by AuthorityGrantID). It merges into any existing scitrera
// meta object and preserves all other meta keys. No-op when name is empty.
func WithAgentName(msg spec.ChatMessage, name string) spec.ChatMessage {
	if name == "" {
		return msg
	}
	nameRaw, err := json.Marshal(name)
	if err != nil {
		return msg
	}
	meta := make(map[string]json.RawMessage, len(msg.Meta)+1)
	for k, v := range msg.Meta {
		meta[k] = v
	}
	scitrera := map[string]json.RawMessage{}
	if raw, ok := meta["scitrera"]; ok {
		// Best-effort: a non-object scitrera value is replaced rather than fatal.
		_ = json.Unmarshal(raw, &scitrera)
	}
	scitrera["agent_name"] = nameRaw
	scitreraRaw, err := json.Marshal(scitrera)
	if err != nil {
		return msg
	}
	meta["scitrera"] = scitreraRaw
	msg.Meta = meta
	return msg
}
