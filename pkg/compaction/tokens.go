package compaction

import "github.com/scitrera/agent-harness-go/pkg/protocol"

// tokensPerByte is the crude bytes->tokens divisor used for budget estimates
// (~4 bytes/token for typical English + JSON). This is an approximation, not an
// exact tokenizer; it only needs to be good enough to keep requests under a
// model's context window with margin.
const bytesPerToken = 4

// perMessageOverheadTokens approximates the role/formatting overhead a provider
// adds per message.
const perMessageOverheadTokens = 4

// EstimateTokens returns a rough token estimate for a message slice.
func EstimateTokens(messages []protocol.ChatMessage) int {
	total := 0
	for _, m := range messages {
		total += estimateMessageTokens(m)
	}
	return total
}

func estimateMessageTokens(m protocol.ChatMessage) int {
	bytes := 0
	for _, part := range m.Content {
		bytes += len(part.Raw())
	}
	return perMessageOverheadTokens + bytes/bytesPerToken
}

// trimToTokenBudget drops the oldest messages until the estimate fits maxTokens,
// always keeping at least the most recent message. Pairing is repaired
// downstream by the provider's transcript sanitizer, so dropping arbitrary
// leading messages is safe.
func trimToTokenBudget(messages []protocol.ChatMessage, maxTokens int) []protocol.ChatMessage {
	if maxTokens <= 0 || len(messages) <= 1 {
		return messages
	}
	total := EstimateTokens(messages)
	drop := 0
	for total > maxTokens && drop < len(messages)-1 {
		total -= estimateMessageTokens(messages[drop])
		drop++
	}
	if drop == 0 {
		return messages
	}
	return messages[drop:]
}
