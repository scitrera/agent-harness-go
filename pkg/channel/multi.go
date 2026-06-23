package channel

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// idlePoll is how long a fan-in pump waits before retrying an underlying channel
// that reported ErrNoTask (idle), to avoid busy-spinning on poll-based channels.
const idlePoll = 50 * time.Millisecond

// Multi composes several channels into one: it fans inbound turns in from all of
// them, and routes each egress event back to the channel its thread arrived on.
// This lets a single process (one runtime.Runner + one Publisher) serve, say,
// Aether and a CLI/HTTP channel at once.
//
// Lifecycle: a pump goroutine per underlying channel starts on the first
// FetchTask call, bound to that call's context (the runtime loop's ctx). The
// pumps exit when that context is cancelled. The underlying channels' own
// start/close lifecycle is the caller's responsibility.
type Multi struct {
	channels []Channel
	inbox    chan tagged

	startOnce sync.Once
	wg        sync.WaitGroup

	mu     sync.RWMutex
	origin map[string]int // routeKey(addr) -> index into channels
}

type tagged struct {
	in  Inbound
	idx int
}

// NewMulti composes the given channels. Order is irrelevant; egress routing is by
// the thread each turn arrived on, not by position.
func NewMulti(channels ...Channel) *Multi {
	return &Multi{
		channels: channels,
		inbox:    make(chan tagged, 64),
		origin:   make(map[string]int),
	}
}

func (m *Multi) start(ctx context.Context) {
	m.startOnce.Do(func() {
		for i, ch := range m.channels {
			m.wg.Add(1)
			go m.pump(ctx, i, ch)
		}
	})
}

// pump continuously fetches from one underlying channel and forwards onto the
// shared inbox until ctx is cancelled.
func (m *Multi) pump(ctx context.Context, idx int, ch Channel) {
	defer m.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		in, err := ch.FetchTask(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			// ErrNoTask (idle) or a transient error: back off briefly, then retry.
			select {
			case <-ctx.Done():
				return
			case <-time.After(idlePoll):
			}
			continue
		}
		select {
		case m.inbox <- tagged{in: in, idx: idx}:
		case <-ctx.Done():
			return
		}
	}
}

// FetchTask returns the next inbound turn from any underlying channel, recording
// which channel it came from so its egress can be routed back. It blocks until a
// turn is available or ctx is cancelled (it never returns ErrNoTask — idleness is
// absorbed by the pumps).
func (m *Multi) FetchTask(ctx context.Context) (Inbound, error) {
	m.start(ctx)
	select {
	case t := <-m.inbox:
		if key := routeKey(t.in.Addr); key != "" {
			m.mu.Lock()
			m.origin[key] = t.idx
			m.mu.Unlock()
		}
		return t.in, nil
	case <-ctx.Done():
		return Inbound{}, ctx.Err()
	}
}

// PublishEvent routes the event to the channel its thread arrived on. If the
// origin is unknown (e.g. a proactive turn with no inbound), it falls back to the
// sole channel when there is exactly one, otherwise it is dropped (best-effort).
func (m *Multi) PublishEvent(ctx context.Context, e Event) error {
	idx, ok := m.lookup(routeKey(e.Addr))
	if !ok {
		if len(m.channels) == 1 {
			return m.channels[0].PublishEvent(ctx, e)
		}
		return nil
	}
	return m.channels[idx].PublishEvent(ctx, e)
}

func (m *Multi) lookup(key string) (int, bool) {
	if key == "" {
		return 0, false
	}
	m.mu.RLock()
	idx, ok := m.origin[key]
	m.mu.RUnlock()
	return idx, ok
}

// Wait blocks until all pump goroutines have exited (after the FetchTask context
// is cancelled). Useful for graceful shutdown.
func (m *Multi) Wait() { m.wg.Wait() }

// routeKey identifies a conversation for origin routing, preferring the most
// specific stable identifier present.
func routeKey(a protocol.MessageAddress) string {
	switch {
	case a.ThreadID != "":
		return "t:" + a.ThreadID
	case a.TaskID != "":
		return "k:" + a.TaskID
	case a.RequestID != "":
		return "r:" + a.RequestID
	default:
		return ""
	}
}

var (
	_ Channel   = (*Multi)(nil)
	_ Receiver  = (*Multi)(nil)
	_ Publisher = (*Multi)(nil)
)
