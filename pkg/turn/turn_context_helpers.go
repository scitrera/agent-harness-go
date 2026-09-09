// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import "github.com/scitrera/agent-harness-go/pkg/protocol"

// mergeInjected inserts injected context (daily notes, recalled memories) right
// after the system prompt.
func mergeInjected(contextMessages, injected []protocol.ChatMessage) []protocol.ChatMessage {
	if len(injected) == 0 || len(contextMessages) == 0 {
		return contextMessages
	}
	merged := make([]protocol.ChatMessage, 0, len(contextMessages)+len(injected))
	merged = append(merged, contextMessages[0])
	merged = append(merged, injected...)
	merged = append(merged, contextMessages[1:]...)
	return merged
}

// trimOldest drops the oldest fraction of history for overflow retry attempt N
// (attempt 0 = no trim). The most recent message is always retained; pairing is
// repaired by the provider's transcript sanitizer.
func trimOldest(history []protocol.ChatMessage, attempt int) []protocol.ChatMessage {
	if attempt <= 0 || len(history) <= 1 {
		return history
	}
	drop := len(history) * attempt / (maxOverflowRetries + 1)
	if drop >= len(history) {
		drop = len(history) - 1
	}
	return history[drop:]
}
