// Package web is a reference web transport for the OSS agent-harness: a
// browser chat UI served by an in-process net/http server, wired into the
// transport seam. The Channel implements channel.Channel — browser POSTs become
// inbound turns (Receiver) and turn stream events fan out to per-thread SSE
// subscribers (Publisher). It is stdlib-only (no external deps); pair it with a
// turn.Runner whose Publisher is this Channel and drive it with runtime.RunLoop.
package web

import (
	"context"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/channel"
)

const (
	// inboxBuffer bounds queued-but-unstarted turns before Enqueue blocks.
	inboxBuffer = 64
	// subscriberBuffer bounds per-SSE-client event backlog; a full buffer drops
	// events for that client rather than stalling the turn that produced them.
	subscriberBuffer = 256
)

// Channel is an in-process web transport. Inbound turns arrive via Enqueue (the
// HTTP chat handler) and are drained by FetchTask; egress stream events are
// routed to SSE subscribers keyed by thread id.
type Channel struct {
	inbox chan channel.Inbound

	mu   sync.RWMutex
	subs map[string]map[*subscriber]struct{} // threadID -> set of listeners
}

type subscriber struct {
	ch chan channel.Event
}

// NewChannel returns a ready Channel.
func NewChannel() *Channel {
	return &Channel{
		inbox: make(chan channel.Inbound, inboxBuffer),
		subs:  make(map[string]map[*subscriber]struct{}),
	}
}

// Enqueue submits an inbound turn. It blocks only while the inbox buffer is full
// (or until ctx is cancelled).
func (c *Channel) Enqueue(ctx context.Context, in channel.Inbound) error {
	select {
	case c.inbox <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FetchTask implements channel.Receiver. It blocks until an inbound turn is
// available or ctx is cancelled; idleness simply blocks (it never returns
// ErrNoTask), which suits runtime.RunLoop.
func (c *Channel) FetchTask(ctx context.Context) (channel.Inbound, error) {
	select {
	case in := <-c.inbox:
		return in, nil
	case <-ctx.Done():
		return channel.Inbound{}, ctx.Err()
	}
}

// PublishEvent implements channel.Publisher: fan the event out to every
// subscriber on its thread. Sends are non-blocking — a full subscriber buffer
// drops the event for that client so a slow browser never stalls a turn.
func (c *Channel) PublishEvent(_ context.Context, e channel.Event) error {
	c.mu.RLock()
	set := c.subs[e.Addr.ThreadID]
	listeners := make([]*subscriber, 0, len(set))
	for s := range set {
		listeners = append(listeners, s)
	}
	c.mu.RUnlock()
	for _, s := range listeners {
		select {
		case s.ch <- e:
		default: // slow consumer — drop
		}
	}
	return nil
}

// Subscribe registers an SSE listener for a thread. The returned channel
// receives events until cancel is called. The channel is intentionally never
// closed (PublishEvent may race a cancel); abandoned buffers are GC'd once the
// caller drops its reference.
func (c *Channel) Subscribe(threadID string) (<-chan channel.Event, func()) {
	s := &subscriber{ch: make(chan channel.Event, subscriberBuffer)}
	c.mu.Lock()
	set := c.subs[threadID]
	if set == nil {
		set = make(map[*subscriber]struct{})
		c.subs[threadID] = set
	}
	set[s] = struct{}{}
	c.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			c.mu.Lock()
			if set := c.subs[threadID]; set != nil {
				delete(set, s)
				if len(set) == 0 {
					delete(c.subs, threadID)
				}
			}
			c.mu.Unlock()
		})
	}
	return s.ch, cancel
}

var _ channel.Channel = (*Channel)(nil)
