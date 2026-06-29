package tui

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestChannelFetchTask_whenMessageEnqueued(t *testing.T) {
	// Given
	ch := NewChannel()
	in := channel.Inbound{Addr: protocol.MessageAddress{ThreadID: "t1"}}

	// When
	if err := ch.Enqueue(context.Background(), in); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := ch.FetchTask(context.Background())

	// Then
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got.Addr.ThreadID != "t1" {
		t.Fatalf("thread = %q", got.Addr.ThreadID)
	}
}

func TestChannelPublishEvent_whenUIReceivesStream(t *testing.T) {
	// Given
	ch := NewChannel()
	event := channel.Event{Type: channel.EventTokenDelta, Addr: protocol.MessageAddress{ThreadID: "t1"}, Delta: "hello"}

	// When
	if err := ch.PublishEvent(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Then
	got := <-ch.Events()
	if got.Type != channel.EventTokenDelta || got.Delta != "hello" {
		t.Fatalf("event mismatch: %+v", got)
	}
}
