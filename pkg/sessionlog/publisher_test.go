// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"context"
	"fmt"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestRecordingPublisherRecordsProjectionAndForwardsNormalizedEvents(t *testing.T) {
	ctx := context.Background()
	log := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	downstream := &capturingPublisher{}
	sessionEvents := &capturingSessionPublisher{}
	publisher, err := NewRecordingPublisher(RecordingPublisherConfig{
		Events:           log,
		Next:             downstream,
		SessionEvents:    sessionEvents,
		DefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := Ref{WorkspaceID: "project-a", SessionID: "session-1"}
	before, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}

	started := spec.NewChatMessage("message-1", spec.RoleAssistant)
	started.Content = []spec.ContentPart{}
	part := spec.NewTextPart("")
	final := started.Clone()
	final.Content = []spec.ContentPart{spec.NewTextPart("hello")}
	events := []channel.Event{
		{Type: channel.EventMessageStarted, Addr: protocol.MessageAddress{ThreadID: ref.SessionID}, Message: &started},
		{Type: channel.EventPartAppended, Addr: protocol.MessageAddress{ThreadID: ref.SessionID}, MessageID: started.ID, Index: 0, Part: &part},
		{Type: channel.EventTokenDelta, Addr: protocol.MessageAddress{ThreadID: ref.SessionID}, MessageID: started.ID, Index: 0, Delta: "hello"},
		{Type: channel.EventMessageFinal, Addr: protocol.MessageAddress{ThreadID: ref.SessionID}, Message: &final},
	}
	for _, event := range events {
		if err := publisher.PublishEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	if started.Addr.WorkspaceID != "" || started.Addr.ThreadID != "" {
		t.Fatalf("caller-owned message was mutated: %#v", started.Addr)
	}
	if len(downstream.events) != len(events) {
		t.Fatalf("forwarded %d events, want %d", len(downstream.events), len(events))
	}
	if len(sessionEvents.events) != len(events) {
		t.Fatalf("published %d session events, want %d", len(sessionEvents.events), len(events))
	}
	for i, event := range sessionEvents.events {
		if event.Cursor.Sequence != uint64(i+1) || event.WorkspaceID != ref.WorkspaceID || event.SessionID != ref.SessionID {
			t.Fatalf("session event %d = %#v", i, event)
		}
	}
	for i, event := range downstream.events {
		if event.Addr.WorkspaceID != ref.WorkspaceID {
			t.Fatalf("forwarded event %d workspace = %q", i, event.Addr.WorkspaceID)
		}
		if event.Message != nil && (event.Message.Addr.WorkspaceID != ref.WorkspaceID || event.Message.Addr.ThreadID != ref.SessionID) {
			t.Fatalf("forwarded event %d message address = %#v", i, event.Message.Addr)
		}
	}

	capture, err := log.Capture(ctx, ref, &before)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Cursor.Sequence != 4 || capture.Replay == nil || capture.Replay.Status != spec.SessionReplayComplete || len(capture.Replay.Events) != 4 {
		t.Fatalf("capture = %#v", capture)
	}
	if !equalCursors(capture.Replay.Through, capture.Cursor) {
		t.Fatalf("replay through = %#v, cursor = %#v", capture.Replay.Through, capture.Cursor)
	}
	if len(capture.Messages) != 1 {
		t.Fatalf("projection messages = %d", len(capture.Messages))
	}
	text, ok := capture.Messages[0].Content[0].AsText()
	if !ok || text.Text != "hello" {
		t.Fatalf("projected content = %#v", capture.Messages[0].Content)
	}
	if capture.Messages[0].Addr.WorkspaceID != ref.WorkspaceID || capture.Messages[0].Addr.ThreadID != ref.SessionID {
		t.Fatalf("projected address = %#v", capture.Messages[0].Addr)
	}

	if err := publisher.PublishEvent(ctx, channel.Event{
		Type: channel.EventToolLifecycle,
		Addr: protocol.MessageAddress{ThreadID: ref.SessionID},
	}); err != nil {
		t.Fatal(err)
	}
	afterLifecycle, err := log.Cursor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !equalCursors(afterLifecycle, capture.Cursor) {
		t.Fatalf("harness-only event advanced cursor: before=%#v after=%#v", capture.Cursor, afterLifecycle)
	}
	if len(downstream.events) != len(events)+1 {
		t.Fatalf("harness-only event was not forwarded")
	}
}

func TestRecordingPublisherRejectsMalformedAndConflictingEvents(t *testing.T) {
	log := NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()})
	downstream := &capturingPublisher{}
	publisher, err := NewRecordingPublisher(RecordingPublisherConfig{
		Events:           log,
		Next:             downstream,
		DefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	addr := protocol.MessageAddress{ThreadID: "session-1"}
	if err := publisher.PublishEvent(ctx, channel.Event{Type: channel.EventMessageStarted, Addr: addr}); err == nil {
		t.Fatal("expected nil message to be rejected")
	}
	conflicting := spec.NewChatMessage("message-1", spec.RoleAssistant)
	conflicting.Addr.WorkspaceID = "project-b"
	conflicting.Addr.ThreadID = addr.ThreadID
	if err := publisher.PublishEvent(ctx, channel.Event{Type: channel.EventMessageStarted, Addr: addr, Message: &conflicting}); err == nil {
		t.Fatal("expected conflicting workspace to be rejected")
	}
	if len(downstream.events) != 0 {
		t.Fatalf("forwarded %d rejected events", len(downstream.events))
	}
}

func TestRecordingPublisherKeepsConcurrentSessionNotificationsOrdered(t *testing.T) {
	const count = 64
	log := NewMemoryEventLog(MemoryEventLogConfig{MaxEvents: count, NewGeneration: sequenceGenerations()})
	sink := &capturingSessionPublisher{}
	publisher, err := NewRecordingPublisher(RecordingPublisherConfig{
		Events:           log,
		SessionEvents:    sink,
		DefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}

	errors := make(chan error, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			message := spec.NewChatMessage(fmt.Sprintf("message-%d", i), spec.RoleAssistant)
			errors <- publisher.PublishEvent(context.Background(), channel.Event{
				Type:    channel.EventMessageFinal,
				Addr:    protocol.MessageAddress{ThreadID: "session-1"},
				Message: &message,
			})
		}(i)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}

	events := sink.snapshot()
	if len(events) != count {
		t.Fatalf("session notifications = %d, want %d", len(events), count)
	}
	for i, event := range events {
		if event.Cursor.Sequence != uint64(i+1) {
			t.Fatalf("notification %d sequence = %d", i, event.Cursor.Sequence)
		}
	}
}

type capturingPublisher struct {
	mu     sync.Mutex
	events []channel.Event
}

type capturingSessionPublisher struct {
	mu     sync.Mutex
	events []spec.SessionEvent
}

func (p *capturingSessionPublisher) PublishSessionEvent(_ context.Context, event spec.SessionEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *capturingSessionPublisher) snapshot() []spec.SessionEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]spec.SessionEvent(nil), p.events...)
}

func (p *capturingPublisher) PublishEvent(_ context.Context, event channel.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}
