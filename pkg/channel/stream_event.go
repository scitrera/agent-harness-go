package channel

import (
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// StreamEventFor maps a harness event to the shared streaming vocabulary.
// mapMessage optionally decorates messages for a transport (for example, the
// Aether adapter adds the assistant display name). Non-wire or malformed events
// return ok=false.
func StreamEventFor(event Event, mapMessage func(protocol.ChatMessage) protocol.ChatMessage) (stream spec.StreamEvent, ok bool) {
	decorate := func(message protocol.ChatMessage) protocol.ChatMessage {
		if mapMessage != nil {
			return mapMessage(message)
		}
		return message
	}
	switch event.Type {
	case EventMessageStarted:
		if event.Message == nil {
			return nil, false
		}
		return spec.MessageStartedEvent{Message: decorate(*event.Message)}, true
	case EventPartAppended:
		if event.Part == nil {
			return nil, false
		}
		return spec.PartAppendedEvent{MessageID: event.MessageID, Index: event.Index, Part: *event.Part}, true
	case EventTokenDelta:
		return spec.TokenDeltaEvent{MessageID: event.MessageID, Index: event.Index, Text: event.Delta}, true
	case EventPartUpdated:
		return spec.PartUpdatedEvent{MessageID: event.MessageID, Index: event.Index, Patch: event.Patch}, true
	case EventMessageFinal:
		if event.Message == nil {
			return nil, false
		}
		message := decorate(*event.Message)
		return spec.MessageFinalizedEvent{MessageID: message.ID, Message: message}, true
	default:
		return nil, false
	}
}
