package turn

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// turnStreamer emits the per-turn stream-event lifecycle (message_started,
// part_appended, token_delta, message_finalized) for one logical assistant
// message identified by msgID. A nil publisher makes every method a no-op, so
// non-Aether paths (e.g. --chat) are unaffected.
type turnStreamer struct {
	publisher EventPublisher
	addr      protocol.MessageAddress
	msgID     string
	now       func() time.Time
	createdAt string // captured once when start() first fires; reused at finalize
	index     int
	started   bool

	// Token-delta coalescing. When flushInterval > 0, streamed deltas are
	// buffered and emitted as a single token_delta at most once per interval
	// (plus a forced flush before any other stream event), collapsing a fast
	// token stream into far fewer messages — which keeps the egress under the
	// gateway's per-identity message-rate quota. 0 disables coalescing (every
	// delta is emitted immediately — the original behavior).
	flushInterval time.Duration
	pending       strings.Builder
	pendingIndex  int
	lastFlush     time.Time
}

func newTurnStreamer(pub EventPublisher, addr protocol.MessageAddress, msgID string, now func() time.Time, flushInterval time.Duration) *turnStreamer {
	return &turnStreamer{publisher: pub, addr: addr, msgID: msgID, now: now, flushInterval: flushInterval}
}

// nowTime returns the current time from the injected clock (falling back to the
// wall clock), used for interval-based delta flushing.
func (s *turnStreamer) nowTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// stampNow returns the current time as an RFC3339Nano UTC string — the
// created_at format. UTC + RFC3339Nano sorts lexicographically = chronologically
// (sub-second precision avoids ties when ordering history on reload). Falls back
// to the wall clock when no clock was injected.
func (s *turnStreamer) stampNow() string {
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	return now().UTC().Format(time.RFC3339Nano)
}

func streamMessageID(addr protocol.MessageAddress) string {
	switch {
	case addr.TaskID != "":
		return addr.TaskID + "-assistant"
	case addr.RequestID != "":
		return addr.RequestID + "-assistant"
	case addr.ThreadID != "":
		return addr.ThreadID + "-assistant"
	default:
		return "assistant"
	}
}

// start emits message_started once (idempotent) with an empty assistant message.
func (s *turnStreamer) start(ctx context.Context) error {
	if s == nil || s.publisher == nil || s.started {
		return nil
	}
	s.started = true
	s.createdAt = s.stampNow()
	msg := protocol.ChatMessage{
		SchemaVersion: "1.0",
		ID:            s.msgID,
		Role:          protocol.RoleAssistant,
		CreatedAt:     s.createdAt,
		Addr:          s.addr,
		Content:       []protocol.ContentPart{},
	}
	return s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventMessageStarted, Addr: s.addr, Message: &msg})
}

// appendPart emits part_appended for a content part (tool_call, tool_result, …)
// and returns the stream index it was placed at (-1 when there is no publisher).
func (s *turnStreamer) appendPart(ctx context.Context, part protocol.ContentPart) (int, error) {
	if s == nil || s.publisher == nil {
		return -1, nil
	}
	if err := s.start(ctx); err != nil {
		return -1, err
	}
	if err := s.flushPending(ctx); err != nil {
		return -1, err
	}
	idx := s.index
	s.index++
	p := part
	if err := s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventPartAppended, Addr: s.addr, MessageID: s.msgID, Index: idx, Part: &p}); err != nil {
		return -1, err
	}
	return idx, nil
}

// appendTextStream appends an empty text part to stream tokens into, returning
// its index (or -1 when there is no publisher).
func (s *turnStreamer) appendTextStream(ctx context.Context) (int, error) {
	if s == nil || s.publisher == nil {
		return -1, nil
	}
	if err := s.start(ctx); err != nil {
		return -1, err
	}
	if err := s.flushPending(ctx); err != nil {
		return -1, err
	}
	idx := s.index
	s.index++
	part, err := protocol.NewTextPart("")
	if err != nil {
		return -1, err
	}
	if err := s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventPartAppended, Addr: s.addr, MessageID: s.msgID, Index: idx, Part: &part}); err != nil {
		return -1, err
	}
	return idx, nil
}

// updatePart emits part_updated, merging patch into the part already streamed at
// index (used to evolve an in-place part such as a todo checklist). No-op
// without a publisher or an empty patch.
func (s *turnStreamer) updatePart(ctx context.Context, index int, patch map[string]json.RawMessage) error {
	if s == nil || s.publisher == nil || len(patch) == 0 {
		return nil
	}
	if err := s.start(ctx); err != nil {
		return err
	}
	if err := s.flushPending(ctx); err != nil {
		return err
	}
	return s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventPartUpdated, Addr: s.addr, MessageID: s.msgID, Index: index, Patch: patch})
}

// tokenDelta emits token_delta for streamed text into the part at index. With
// coalescing enabled (flushInterval > 0) it buffers the text and flushes at most
// once per interval (always on the first delta, for low time-to-first-token);
// the buffer is forced out before any other stream event and at finalize.
func (s *turnStreamer) tokenDelta(ctx context.Context, index int, text string) error {
	if s == nil || s.publisher == nil || text == "" {
		return nil
	}
	if s.flushInterval <= 0 {
		return s.publishDelta(ctx, index, text)
	}
	// A delta for a different part: flush the buffered one first so the two
	// parts' deltas never interleave under one index.
	if s.pending.Len() > 0 && s.pendingIndex != index {
		if err := s.flushPending(ctx); err != nil {
			return err
		}
	}
	s.pendingIndex = index
	s.pending.WriteString(text)
	if s.lastFlush.IsZero() || s.nowTime().Sub(s.lastFlush) >= s.flushInterval {
		return s.flushPending(ctx)
	}
	return nil
}

// flushPending emits any buffered token-delta text as a single token_delta. No-op
// when nothing is buffered (or no publisher). Called before every other stream
// event and at finalize so coalesced deltas stay correctly ordered and complete.
func (s *turnStreamer) flushPending(ctx context.Context) error {
	if s == nil || s.publisher == nil || s.pending.Len() == 0 {
		return nil
	}
	text := s.pending.String()
	s.pending.Reset()
	s.lastFlush = s.nowTime()
	return s.publishDelta(ctx, s.pendingIndex, text)
}

func (s *turnStreamer) publishDelta(ctx context.Context, index int, text string) error {
	return s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventTokenDelta, Addr: s.addr, MessageID: s.msgID, Index: index, Delta: text})
}

// finalize emits message_finalized with the authoritative message (id forced to
// the stream id so started/appended/finalized share one id).
func (s *turnStreamer) finalize(ctx context.Context, msg protocol.ChatMessage) error {
	if s == nil || s.publisher == nil {
		return nil
	}
	if err := s.start(ctx); err != nil {
		return err
	}
	// Flush any buffered token deltas so the streamed text is complete and
	// ordered before the authoritative finalized message.
	if err := s.flushPending(ctx); err != nil {
		return err
	}
	msg.ID = s.msgID
	// Carry the message's creation time (captured at start) so persisted history
	// orders correctly on reload; don't clobber a caller-provided value.
	if msg.CreatedAt == "" {
		msg.CreatedAt = s.createdAt
	}
	if msg.Addr.ThreadID == "" {
		msg.Addr = s.addr
	}
	return s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventMessageFinal, Addr: s.addr, Message: &msg})
}
