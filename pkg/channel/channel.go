// Package channel is the transport seam: how inbound turns arrive (Receiver)
// and how streaming events leave (Publisher). The Aether transport and the CLI
// are implementations. The types here are protocol-neutral (built only from
// messaging-spec types) so the core depends on this package, not on any
// concrete transport.
package channel

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// ErrNoTask signals that no inbound message is currently available; a Receiver
// returns it to indicate idle (the runtime loop treats it as "keep polling").
var ErrNoTask = errors.New("channel: no task")

// EventType discriminates an egress stream event.
type EventType string

const (
	EventMessageStarted EventType = "message_started"
	EventPartAppended   EventType = "part_appended"
	EventTokenDelta     EventType = "token_delta"
	EventPartUpdated    EventType = "part_updated"
	EventMessageFinal   EventType = "message_final"
	EventToolResult     EventType = "tool_result"
	EventToolLifecycle  EventType = "tool_lifecycle"
	EventError          EventType = "error"
)

// Event is an egress stream event. Different fields are populated per Type:
// Message for started/final; MessageID+Index+Part for part_appended;
// MessageID+Index+Delta for token_delta; MessageID+Index+Patch for part_updated.
type Event struct {
	Type      EventType                  `json:"type"`
	Addr      protocol.MessageAddress    `json:"addr"`
	Message   *protocol.ChatMessage      `json:"message,omitempty"`
	MessageID string                     `json:"message_id,omitempty"`
	Index     int                        `json:"index,omitempty"`
	Part      *protocol.ContentPart      `json:"part,omitempty"`
	Delta     string                     `json:"delta,omitempty"`
	Patch     map[string]json.RawMessage `json:"patch,omitempty"`
	Payload   json.RawMessage            `json:"payload,omitempty"`
}

// Inbound is an inbound turn: an addressed message plus opaque transport meta.
type Inbound struct {
	Addr    protocol.MessageAddress `json:"addr"`
	Message protocol.ChatMessage    `json:"message"`
	Meta    json.RawMessage         `json:"meta,omitempty"`
}

// Receiver delivers inbound turns. FetchTask blocks until one is available or
// returns ErrNoTask when idle.
type Receiver interface {
	FetchTask(ctx context.Context) (Inbound, error)
}

// Enqueuer injects an inbound turn into the transport's ingress so the runtime
// loop drives a fresh turn for it. It is the ingress-write half that the web/tui
// channels already expose as Enqueue; naming it as an interface lets the turn
// layer push a turn (e.g. a background sub-agent's completion notice addressed to
// its parent thread) without importing a concrete transport. The Aether transport
// satisfies it by sending a MessageEnvelope to the target agent. cli (stdout-only)
// does not implement it, so a background push simply degrades to unavailable.
type Enqueuer interface {
	Enqueue(ctx context.Context, in Inbound) error
}

// Publisher emits egress stream events for a turn.
type Publisher interface {
	PublishEvent(ctx context.Context, event Event) error
}

// Channel is a bidirectional transport (ingress + egress).
type Channel interface {
	Receiver
	Publisher
}
