package compaction

import (
	"encoding/json"
	"fmt"
	"strconv"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const compactionSummaryID = "compaction-summary"

type Config struct {
	MaxMessages      int
	MaxTextPartBytes int
	// MaxTokens, when > 0, drops the oldest messages until the estimated token
	// count fits the budget (applied after the message-count/byte reductions).
	MaxTokens int
	// WorldState carries deterministic local state that must survive compaction.
	WorldState WorldState
}

func Reduce(messages []protocol.ChatMessage, cfg Config) ([]protocol.ChatMessage, error) {
	report, err := ReduceWithReport(messages, cfg)
	if err != nil {
		return nil, err
	}
	return report.Messages, nil
}

func ReduceWithReport(messages []protocol.ChatMessage, cfg Config) (Report, error) {
	if cfg.MaxMessages <= 0 && cfg.MaxTextPartBytes <= 0 && cfg.MaxTokens <= 0 {
		out := cloneMessages(messages)
		state := MergeWorldState(ExtractWorldState(messages), cfg.WorldState)
		budget := BudgetFor(out, cfg.MaxTokens, 0)
		out, err := attachReportMetadata(out, state, budget)
		if err != nil {
			return Report{}, err
		}
		return Report{Messages: out, WorldState: state, Budget: budget}, nil
	}
	reduced := cloneMessages(messages)
	if cfg.MaxTextPartBytes > 0 {
		var err error
		reduced, err = trimTextParts(reduced, cfg.MaxTextPartBytes)
		if err != nil {
			return Report{}, err
		}
	}
	compactedMessages := 0
	if cfg.MaxMessages > 0 && len(reduced) > cfg.MaxMessages {
		compactedMessages = len(reduced) - cfg.MaxMessages
		summary, err := summaryMessage(compactedMessages)
		if err != nil {
			return Report{}, err
		}
		tail := cloneMessages(reduced[len(reduced)-cfg.MaxMessages:])
		reduced = append([]protocol.ChatMessage{summary}, tail...)
	}
	if cfg.MaxTokens > 0 {
		beforeTokenTrim := reduced
		reduced = trimToTokenBudget(reduced, cfg.MaxTokens)
		for _, msg := range beforeTokenTrim[:len(beforeTokenTrim)-len(reduced)] {
			if msg.ID != compactionSummaryID {
				compactedMessages++
			}
		}
	}
	state := MergeWorldState(ExtractWorldState(messages), cfg.WorldState)
	budget := BudgetFor(reduced, cfg.MaxTokens, compactedMessages)
	reduced, err := attachReportMetadata(reduced, state, budget)
	if err != nil {
		return Report{}, err
	}
	return Report{Messages: reduced, WorldState: state, Budget: budget}, nil
}

func trimTextParts(messages []protocol.ChatMessage, maxBytes int) ([]protocol.ChatMessage, error) {
	out := cloneMessages(messages)
	for i, msg := range out {
		parts := make([]protocol.ContentPart, 0, len(msg.Content))
		for _, part := range msg.Content {
			tp, ok := part.AsText()
			if !ok || len([]byte(tp.Text)) <= maxBytes {
				parts = append(parts, part)
				continue
			}
			trimmed := []byte(tp.Text)
			if len(trimmed) > maxBytes {
				trimmed = trimmed[:maxBytes]
			}
			next, err := protocol.NewTextPart(string(trimmed) + "\n[truncated]")
			if err != nil {
				return nil, fmt.Errorf("trim text part: %w", err)
			}
			parts = append(parts, next)
		}
		out[i].Content = parts
	}
	return out, nil
}

func summaryMessage(omitted int) (protocol.ChatMessage, error) {
	part, err := protocol.NewTextPart(fmt.Sprintf("Compacted %d older messages.", omitted))
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	return protocol.ChatMessage{
		SchemaVersion: spec.MessagingSchemaVersion,
		ID:            compactionSummaryID,
		Role:          protocol.RoleSystem,
		Content:       []protocol.ContentPart{part},
		Meta:          map[string]json.RawMessage{"omitted_messages": json.RawMessage(strconv.Itoa(omitted))},
	}, nil
}

func cloneMessages(messages []protocol.ChatMessage) []protocol.ChatMessage {
	out := make([]protocol.ChatMessage, len(messages))
	for i, msg := range messages {
		out[i] = msg.Clone()
	}
	return out
}
