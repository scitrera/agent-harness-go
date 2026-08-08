package channel

import (
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestStreamEventForMapsSharedWireVocabulary(t *testing.T) {
	message := spec.NewChatMessage("message-1", spec.RoleAssistant)
	part := spec.NewTextPart("hello")
	patch := map[string]json.RawMessage{"text": json.RawMessage(`"updated"`)}
	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{name: "started", event: Event{Type: EventMessageStarted, Message: &message}, want: spec.EventMessageStarted},
		{name: "part", event: Event{Type: EventPartAppended, MessageID: message.ID, Part: &part}, want: spec.EventPartAppended},
		{name: "delta", event: Event{Type: EventTokenDelta, MessageID: message.ID, Delta: "!"}, want: spec.EventTokenDelta},
		{name: "updated", event: Event{Type: EventPartUpdated, MessageID: message.ID, Patch: patch}, want: spec.EventPartUpdated},
		{name: "final", event: Event{Type: EventMessageFinal, Message: &message}, want: spec.EventMessageFinalized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := StreamEventFor(test.event, func(message spec.ChatMessage) spec.ChatMessage {
				message.CreatedAt = "decorated"
				return message
			})
			if !ok || got.EventName() != test.want {
				t.Fatalf("StreamEventFor() = (%#v, %t), want event %q", got, ok, test.want)
			}
		})
	}
}

func TestStreamEventForRejectsNonWireAndMalformedEvents(t *testing.T) {
	for _, event := range []Event{
		{Type: EventToolLifecycle},
		{Type: EventMessageStarted},
		{Type: EventPartAppended},
		{Type: EventMessageFinal},
	} {
		if got, ok := StreamEventFor(event, nil); ok || got != nil {
			t.Fatalf("StreamEventFor(%q) = (%#v, %t)", event.Type, got, ok)
		}
	}
}
