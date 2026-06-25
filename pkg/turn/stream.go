package turn

import (
	"context"
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
}

func newTurnStreamer(pub EventPublisher, addr protocol.MessageAddress, msgID string, now func() time.Time) *turnStreamer {
	return &turnStreamer{publisher: pub, addr: addr, msgID: msgID, now: now}
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

// appendPart emits part_appended for a content part (tool_call, tool_result, …).
func (s *turnStreamer) appendPart(ctx context.Context, part protocol.ContentPart) error {
	if s == nil || s.publisher == nil {
		return nil
	}
	if err := s.start(ctx); err != nil {
		return err
	}
	idx := s.index
	s.index++
	p := part
	return s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventPartAppended, Addr: s.addr, MessageID: s.msgID, Index: idx, Part: &p})
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

// tokenDelta emits token_delta for streamed text into the part at index.
func (s *turnStreamer) tokenDelta(ctx context.Context, index int, text string) error {
	if s == nil || s.publisher == nil || text == "" {
		return nil
	}
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
