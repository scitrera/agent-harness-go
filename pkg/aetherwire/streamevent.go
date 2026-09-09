// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aetherwire

import (
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
)

// StreamEventFor maps a harness stream event onto its wire form, stamping the
// assistant's display persona on the messages that carry one. ok is false for an
// event that has no wire representation — tool lifecycle and errors reach
// clients as part_appended, not as events of their own — and for a malformed
// event whose payload is missing.
//
// This lives here, next to StreamEnvelope and ParseStreamEnvelope, because every
// transport that publishes a turn needs the identical mapping: which harness
// events cross the wire, which spec event each becomes, and where the agent name
// is stamped. Two transports maintaining their own copy is how the shape drifts.
func StreamEventFor(event channel.Event, agentName string) (spec.StreamEvent, bool) {
	return channel.StreamEventFor(event, func(message spec.ChatMessage) spec.ChatMessage {
		return WithAgentName(message, agentName)
	})
}
