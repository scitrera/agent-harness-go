// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aetherwire

import (
	"encoding/json"
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// ParseStreamEnvelope is the decode half of StreamEnvelope: it unwraps a
// published envelope back into the routing meta and the spec stream event.
//
// ok is false for a payload that is not a stream envelope at all (the topic can
// carry other traffic), which is not an error. An error means the payload looked
// like an envelope but its event could not be decoded.
//
// Having this here, next to the encoder, is the point: a consumer that
// hand-rolls the unwrapping has to know that the discriminator is "event" (not
// "type") and that the event hangs off options.chat_stream_event — both easy to
// get wrong, and wrong in a way that silently yields no events rather than an
// error.
func ParseStreamEnvelope(payload []byte) (meta TurnMeta, event spec.StreamEvent, ok bool, err error) {
	var envelope struct {
		RequestID string `json:"request_id"`
		Workspace string `json:"workspace"`
		Source    struct {
			Agent string `json:"agent"`
		} `json:"source"`
		Options struct {
			ThreadID     string          `json:"thread_id"`
			AppWorkspace string          `json:"app_workspace"`
			StreamEvent  json.RawMessage `json:"chat_stream_event"`
		} `json:"options"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return TurnMeta{}, nil, false, nil
	}
	if len(envelope.Options.StreamEvent) == 0 {
		return TurnMeta{}, nil, false, nil
	}
	decoded, err := spec.DecodeStreamEvent(envelope.Options.StreamEvent)
	if err != nil {
		return TurnMeta{}, nil, false, fmt.Errorf("aetherwire: decode stream event: %w", err)
	}
	workspace := envelope.Options.AppWorkspace
	if workspace == "" {
		workspace = envelope.Workspace
	}
	return TurnMeta{
		AppWorkspace: workspace,
		ThreadID:     envelope.Options.ThreadID,
		WindowID:     envelope.RequestID,
		SourceAgent:  envelope.Source.Agent,
	}, decoded, true, nil
}
