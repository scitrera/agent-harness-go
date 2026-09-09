// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aetherwire

import (
	"encoding/json"
	"fmt"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// InboundControl is an in-band control signal parsed from a turn payload.
type InboundControl struct {
	Kind   string
	TaskID string
	// RequestID + Scope carry the approve/deny decision for an approval_request.
	RequestID string
	Scope     string
}

// ParseInbound decodes an inbound turn payload as a spec ChatMessage and
// surfaces the first control signal if present. When ctrl is non-nil the turn
// is a control message (e.g. cancel), not a normal user turn.
func ParseInbound(payload []byte) (spec.ChatMessage, *InboundControl, error) {
	var msg spec.ChatMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return spec.ChatMessage{}, nil, fmt.Errorf("aetherwire: decode inbound: %w", err)
	}
	return msg, FirstControl(msg), nil
}

// FirstControl returns the first control content part, or nil if none.
func FirstControl(msg spec.ChatMessage) *InboundControl {
	for _, part := range msg.Content {
		if body, ok := part.AsControl(); ok {
			return &InboundControl{Kind: body.Kind, TaskID: body.TaskID, RequestID: body.RequestID, Scope: body.Scope}
		}
	}
	return nil
}

// AuthorityGrantID extracts the OBO authority grant id from message meta,
// checking meta.scitrera.authority_grant_id then meta.authority_grant_id.
func AuthorityGrantID(msg spec.ChatMessage) string {
	if msg.Meta == nil {
		return ""
	}
	if raw, ok := msg.Meta["scitrera"]; ok {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw, &nested) == nil {
			if id := decodeString(nested["authority_grant_id"]); id != "" {
				return id
			}
		}
	}
	return decodeString(msg.Meta["authority_grant_id"])
}

// Ephemeral reports whether the inbound message requests a one-shot EPHEMERAL
// turn (no durable history load, no memory recall/commit, no durable persist),
// read from meta.scitrera.ephemeral (bool). Mirrors AuthorityGrantID's nested
// scitrera-namespace convention. Absent/false when unset.
func Ephemeral(msg spec.ChatMessage) bool {
	if msg.Meta == nil {
		return false
	}
	if raw, ok := msg.Meta["scitrera"]; ok {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw, &nested) == nil {
			if b := decodeBool(nested["ephemeral"]); b {
				return true
			}
		}
	}
	return decodeBool(msg.Meta["ephemeral"])
}

// SynthesizeSystemPrompt extracts the per-turn system-prompt text a one-shot
// (agent.synthesize) request carries in meta.scitrera.synthesize_options — the
// `system` and `instructions` keys, concatenated (system first). "" when absent.
// The harness renders it as the Request-instructions system-prompt section
// (contextpack.WithSystemPromptExtra). Other options keys (e.g. timeout_s) are
// bridge-side and ignored here.
func SynthesizeSystemPrompt(msg spec.ChatMessage) string {
	if msg.Meta == nil {
		return ""
	}
	raw, ok := msg.Meta["scitrera"]
	if !ok {
		return ""
	}
	var nested map[string]json.RawMessage
	if json.Unmarshal(raw, &nested) != nil {
		return ""
	}
	optsRaw, ok := nested["synthesize_options"]
	if !ok {
		return ""
	}
	var opts map[string]json.RawMessage
	if json.Unmarshal(optsRaw, &opts) != nil {
		return ""
	}
	system := strings.TrimSpace(decodeString(opts["system"]))
	instructions := strings.TrimSpace(decodeString(opts["instructions"]))
	switch {
	case system != "" && instructions != "":
		return system + "\n\n" + instructions
	case system != "":
		return system
	default:
		return instructions
	}
}

// ConcatText concatenates the text of every text part (the user's prompt).
func ConcatText(msg spec.ChatMessage) string {
	var b strings.Builder
	for _, part := range msg.Content {
		if t, ok := part.AsText(); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func decodeString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

func decodeBool(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var b bool
	_ = json.Unmarshal(raw, &b)
	return b
}
