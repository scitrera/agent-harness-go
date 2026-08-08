package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
)

// RecordingPublisherConfig wires a bounded log in front of an optional
// downstream transport. DefaultWorkspace resolves workspace-less local events.
type RecordingPublisherConfig struct {
	Events           EventStore
	Next             channel.Publisher
	DefaultWorkspace string
}

// RecordingPublisher records every shared chat-stream event before forwarding
// it. Harness-only lifecycle/error events still pass downstream but are not put
// into the protocol replay log.
type RecordingPublisher struct {
	events           EventStore
	next             channel.Publisher
	defaultWorkspace string
}

func NewRecordingPublisher(config RecordingPublisherConfig) (*RecordingPublisher, error) {
	if config.Events == nil {
		return nil, errors.New("sessionlog: recording publisher event store is required")
	}
	return &RecordingPublisher{
		events:           config.Events,
		next:             config.Next,
		defaultWorkspace: config.DefaultWorkspace,
	}, nil
}

func (p *RecordingPublisher) PublishEvent(ctx context.Context, event channel.Event) error {
	workspaceID := event.Addr.WorkspaceID
	if workspaceID == "" {
		workspaceID = p.defaultWorkspace
		event.Addr.WorkspaceID = workspaceID
	}
	if event.Message != nil {
		message := event.Message.Clone()
		if message.Addr.WorkspaceID != "" && message.Addr.WorkspaceID != workspaceID {
			return errors.New("sessionlog: event and message workspace IDs differ")
		}
		if message.Addr.ThreadID != "" && message.Addr.ThreadID != event.Addr.ThreadID {
			return errors.New("sessionlog: event and message session IDs differ")
		}
		message.Addr.WorkspaceID = workspaceID
		message.Addr.ThreadID = event.Addr.ThreadID
		event.Message = &message
	}
	streamEvent, record := channel.StreamEventFor(event, nil)
	if isChatStreamType(event.Type) && !record {
		return fmt.Errorf("sessionlog: malformed %s event", event.Type)
	}
	if record {
		if workspaceID == "" || event.Addr.ThreadID == "" {
			return fmt.Errorf("sessionlog: cannot record event without workspace and session IDs")
		}
		if err := validateStreamEvent(streamEvent); err != nil {
			return err
		}
		payload, err := json.Marshal(streamEvent)
		if err != nil {
			return fmt.Errorf("sessionlog: encode stream event: %w", err)
		}
		if _, err := p.events.Append(ctx, Ref{WorkspaceID: workspaceID, SessionID: event.Addr.ThreadID}, spec.SessionEventChatStream, payload); err != nil {
			return err
		}
	}
	if p.next != nil {
		return p.next.PublishEvent(ctx, event)
	}
	return nil
}

func isChatStreamType(eventType channel.EventType) bool {
	switch eventType {
	case channel.EventMessageStarted, channel.EventPartAppended, channel.EventTokenDelta,
		channel.EventPartUpdated, channel.EventMessageFinal:
		return true
	default:
		return false
	}
}

func validateStreamEvent(event spec.StreamEvent) error {
	switch event := event.(type) {
	case spec.MessageStartedEvent:
		if event.Message.ID == "" {
			return errors.New("sessionlog: message_started requires a message ID")
		}
	case spec.PartAppendedEvent:
		if event.MessageID == "" || event.Index < 0 {
			return errors.New("sessionlog: part_appended requires a message ID and non-negative index")
		}
	case spec.TokenDeltaEvent:
		if event.MessageID == "" || event.Index < 0 {
			return errors.New("sessionlog: token_delta requires a message ID and non-negative index")
		}
	case spec.PartUpdatedEvent:
		if event.MessageID == "" || event.Index < 0 {
			return errors.New("sessionlog: part_updated requires a message ID and non-negative index")
		}
	case spec.MessageFinalizedEvent:
		if event.MessageID == "" || event.Message.ID == "" || event.MessageID != event.Message.ID {
			return errors.New("sessionlog: message_finalized requires one consistent message ID")
		}
	}
	return nil
}

var _ channel.Publisher = (*RecordingPublisher)(nil)
