package channel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// fakeChan is a Channel whose FetchTask blocks on an inbox until a message
// arrives or ctx is cancelled, and which records published events.
type fakeChan struct {
	inbox chan Inbound
	mu    sync.Mutex
	pub   []Event
}

func newFakeChan() *fakeChan { return &fakeChan{inbox: make(chan Inbound, 8)} }

func (f *fakeChan) FetchTask(ctx context.Context) (Inbound, error) {
	select {
	case in := <-f.inbox:
		return in, nil
	case <-ctx.Done():
		return Inbound{}, ctx.Err()
	}
}

func (f *fakeChan) PublishEvent(_ context.Context, e Event) error {
	f.mu.Lock()
	f.pub = append(f.pub, e)
	f.mu.Unlock()
	return nil
}

func (f *fakeChan) published() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Event, len(f.pub))
	copy(out, f.pub)
	return out
}

func inbound(thread string) Inbound {
	return Inbound{Addr: protocol.MessageAddress{ThreadID: thread}}
}

func TestMultiFanInAndRouteByOrigin(t *testing.T) {
	a, b := newFakeChan(), newFakeChan()
	m := NewMulti(a, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A turn arrives on channel A (thread "ta"); FetchTask must surface it and
	// record A as its origin.
	a.inbox <- inbound("ta")
	got, err := m.FetchTask(ctx)
	if err != nil || got.Addr.ThreadID != "ta" {
		t.Fatalf("fan-in A: %v %q", err, got.Addr.ThreadID)
	}
	// And one on B (thread "tb").
	b.inbox <- inbound("tb")
	got, err = m.FetchTask(ctx)
	if err != nil || got.Addr.ThreadID != "tb" {
		t.Fatalf("fan-in B: %v %q", err, got.Addr.ThreadID)
	}

	// Egress for "ta" routes only to A; "tb" only to B.
	_ = m.PublishEvent(ctx, Event{Type: EventMessageFinal, Addr: protocol.MessageAddress{ThreadID: "ta"}})
	_ = m.PublishEvent(ctx, Event{Type: EventMessageFinal, Addr: protocol.MessageAddress{ThreadID: "tb"}})
	if len(a.published()) != 1 || a.published()[0].Addr.ThreadID != "ta" {
		t.Fatalf("A should have exactly ta: %#v", a.published())
	}
	if len(b.published()) != 1 || b.published()[0].Addr.ThreadID != "tb" {
		t.Fatalf("B should have exactly tb: %#v", b.published())
	}

	// Unknown origin with multiple channels -> dropped (best-effort).
	_ = m.PublishEvent(ctx, Event{Type: EventMessageFinal, Addr: protocol.MessageAddress{ThreadID: "ghost"}})
	if len(a.published()) != 1 || len(b.published()) != 1 {
		t.Fatalf("unknown-origin event should be dropped, got A=%d B=%d", len(a.published()), len(b.published()))
	}
}

func TestMultiSingleChannelFallbackDelivers(t *testing.T) {
	a := newFakeChan()
	m := NewMulti(a)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// kick the pump so the ctx is bound, though not strictly required for publish.
	a.inbox <- inbound("t1")
	if _, err := m.FetchTask(ctx); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	// Unknown thread but a single channel -> delivered there.
	if err := m.PublishEvent(ctx, Event{Type: EventMessageFinal, Addr: protocol.MessageAddress{ThreadID: "proactive"}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(a.published()) != 1 {
		t.Fatalf("single-channel fallback should deliver, got %d", len(a.published()))
	}
}

func TestMultiFetchTaskRespectsContextCancel(t *testing.T) {
	a := newFakeChan()
	m := NewMulti(a)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := m.FetchTask(ctx); done <- err }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("FetchTask did not return after cancel")
	}
}
