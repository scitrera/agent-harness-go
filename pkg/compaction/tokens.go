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

// fixedImageTokens is the flat token cost charged per image content part. A
// vision model tiles an image to roughly a fixed number of tokens regardless of
// the encoded byte length, so counting a data_uri's base64 bytes as text badly
// over-charges an inline image (and a bare vfs_ref/uri image would be badly
// under-charged). This flat estimate keeps the budget honest for both carriers.
const fixedImageTokens = 1200

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
	imageTokens := 0
	for _, part := range m.Content {
		// An image is charged a flat per-image cost (vision models tile to ~a
		// fixed token count); its raw bytes — a base64 data_uri or a bare ref —
		// are NOT a meaningful text-token proxy, so skip the byte accumulation.
		if part.Type() == protocol.ContentImage {
			imageTokens += fixedImageTokens
			continue
		}
		bytes += len(part.Raw())
	}
	return perMessageOverheadTokens + bytes/bytesPerToken + imageTokens
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
