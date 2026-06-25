package web

import (
	"context"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestChannelEnqueueFetchRoundTrip(t *testing.T) {
	c := NewChannel()
	in := channel.Inbound{Addr: protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"}}
	if err := c.Enqueue(context.Background(), in); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, err := c.FetchTask(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got.Addr.ThreadID != "t1" || got.Addr.TaskID != "task-1" {
		t.Fatalf("round-trip mismatch: %+v", got.Addr)
	}
}

func TestFetchTaskBlocksUntilCancel(t *testing.T) {
	c := NewChannel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.FetchTask(ctx); err == nil {
		t.Fatal("expected ctx error on cancelled fetch")
	}
}

func TestPublishEventFanOutFiltersByThread(t *testing.T) {
	c := NewChannel()
	evs, cancel := c.Subscribe("t1")
	defer cancel()
	other, cancelOther := c.Subscribe("t2")
	defer cancelOther()

	// Event for t1 should reach t1's subscriber only.
	if err := c.PublishEvent(context.Background(), channel.Event{
		Type: channel.EventTokenDelta,
		Addr: protocol.MessageAddress{ThreadID: "t1"},
		Delta: "hi",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case e := <-evs:
		if e.Delta != "hi" {
			t.Fatalf("unexpected delta %q", e.Delta)
		}
	case <-time.After(time.Second):
		t.Fatal("t1 subscriber did not receive event")
	}

	select {
	case e := <-other:
		t.Fatalf("t2 subscriber should not have received event: %+v", e)
	case <-time.After(50 * time.Millisecond):
		// expected: nothing
	}
}

func TestSubscribeCancelRemovesListener(t *testing.T) {
	c := NewChannel()
	_, cancel := c.Subscribe("t1")
	cancel()
	// After cancel, publishing must not panic and the thread set is cleaned up.
	if err := c.PublishEvent(context.Background(), channel.Event{
		Type: channel.EventTokenDelta,
		Addr: protocol.MessageAddress{ThreadID: "t1"},
	}); err != nil {
		t.Fatalf("publish after cancel: %v", err)
	}
	c.mu.RLock()
	_, ok := c.subs["t1"]
	c.mu.RUnlock()
	if ok {
		t.Fatal("expected thread subscriber set to be removed after cancel")
	}
}
