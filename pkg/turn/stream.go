package turn

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

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

	// Authoritative reconstruction of the events emitted so far, keyed by
	// msgID. Every emit method folds its event into this state via the spec
	// reducer, so finalize can publish a message_finalized whose content
	// equals the reconstruction of the streamed events (spec §7) — i.e. the
	// full turn (text + tool_call/tool_result + image/file + todo …), not just
	// the model's trailing answer message.
	state spec.MessageState

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
	s.state = spec.ApplyEvent(spec.MessageState{}, spec.MessageStartedEvent{Message: msg})
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
	s.state = spec.ApplyEvent(s.state, spec.PartAppendedEvent{MessageID: s.msgID, Index: idx, Part: p})
	return idx, nil
}

// appendUnstreamed appends part only when an equivalent part isn't already in
// the reconstruction, so a part already streamed live (e.g. a text part fed by
// token_delta on the streaming path, or a tool_call streamed earlier this turn)
// isn't duplicated. Returns whether it was appended. Used to surface an
// assistant message's parts (preamble text/reasoning, tool_call) in content
// order regardless of whether the provider streamed them — a no-op on the
// streaming path, the sole emit site on the non-streaming path.
func (s *turnStreamer) appendUnstreamed(ctx context.Context, part protocol.ContentPart) (bool, error) {
	if s == nil || s.publisher == nil {
		return false, nil
	}
	// Flush buffered token deltas into the reconstruction FIRST, so the dedup
	// check below sees the COMPLETE streamed text. With coalescing on
	// (SAHARA_STREAM_FLUSH_MS>0) s.pending holds the un-flushed tail of a streamed
	// text part, so s.state would otherwise carry only a partial prefix — the
	// provider's returned full text part wouldn't match, and this method would
	// re-append it, DOUBLING the inter-tool-call assistant text. finalize()
	// already flushes-before-dedup for exactly this reason (see below).
	if err := s.flushPending(ctx); err != nil {
		return false, err
	}
	if containsEquivalentPart(s.state[s.msgID].Content, part) {
		return false, nil
	}
	if _, err := s.appendPart(ctx, part); err != nil {
		return false, err
	}
	return true, nil
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
	s.state = spec.ApplyEvent(s.state, spec.PartAppendedEvent{MessageID: s.msgID, Index: idx, Part: part})
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
	if err := s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventPartUpdated, Addr: s.addr, MessageID: s.msgID, Index: index, Patch: patch}); err != nil {
		return err
	}
	s.state = spec.ApplyEvent(s.state, spec.PartUpdatedEvent{MessageID: s.msgID, Index: index, Patch: patch})
	return nil
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
	if err := s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventTokenDelta, Addr: s.addr, MessageID: s.msgID, Index: index, Delta: text}); err != nil {
		return err
	}
	s.state = spec.ApplyEvent(s.state, spec.TokenDeltaEvent{MessageID: s.msgID, Index: index, Text: text})
	return nil
}

// finalize emits message_finalized with the authoritative message and returns
// the canonical message it published (id forced to the stream id so
// started/appended/finalized share one id).
//
// The finalized message's content is the reconstruction of everything streamed
// this turn (text + tool_call/tool_result + image/file + todo …), not just the
// caller's trailing model message. Any parts of msg that were NOT streamed
// (e.g. the answer text when the provider is non-streaming, or reasoning a
// provider only returns at the end) are appended via part_appended first, so
// the finalized payload equals the reconstruction of the streamed events
// (spec §7). With no publisher (e.g. --chat), the caller's msg stands unchanged.
func (s *turnStreamer) finalize(ctx context.Context, msg protocol.ChatMessage) (protocol.ChatMessage, error) {
	if s == nil || s.publisher == nil {
		return msg, nil
	}
	if err := s.start(ctx); err != nil {
		return protocol.ChatMessage{}, err
	}
	// Flush any buffered token deltas so the streamed text is complete and
	// ordered before the authoritative finalized message.
	if err := s.flushPending(ctx); err != nil {
		return protocol.ChatMessage{}, err
	}
	// Append any caller parts the stream didn't already carry (keeps the
	// reconstruction == finalized invariant; a no-op on the streaming path,
	// where the model's answer text was already streamed via token_delta).
	recon := s.state[s.msgID]
	for _, p := range msg.Content {
		if containsEquivalentPart(recon.Content, p) {
			continue
		}
		if _, err := s.appendPart(ctx, p); err != nil {
			return protocol.ChatMessage{}, err
		}
		recon = s.state[s.msgID]
	}
	out := msg
	out.ID = s.msgID
	out.Content = recon.Content
	// Carry the message's creation time (captured at start) so persisted history
	// orders correctly on reload; don't clobber a caller-provided value.
	if out.CreatedAt == "" {
		out.CreatedAt = s.createdAt
	}
	if out.Addr.ThreadID == "" {
		out.Addr = s.addr
	}
	if err := s.publisher.PublishEvent(ctx, channel.Event{Type: channel.EventMessageFinal, Addr: s.addr, Message: &out}); err != nil {
		return protocol.ChatMessage{}, err
	}
	return out, nil
}

// containsEquivalentPart reports whether parts already holds a part equivalent
// to p: by id when p is id-bearing (tool_call/tool_result/todo/…), by
// (type, text) for text/reasoning parts (so streamed answer text isn't appended
// twice at finalize), and by raw-JSON equality otherwise.
func containsEquivalentPart(parts []protocol.ContentPart, p protocol.ContentPart) bool {
	if id := partID(p); id != "" {
		for _, q := range parts {
			if partID(q) == id {
				return true
			}
		}
		return false
	}
	if pt, isText := partText(p); isText {
		for _, q := range parts {
			if q.Type() != p.Type() {
				continue
			}
			if qt, ok := partText(q); ok && qt == pt {
				return true
			}
		}
		return false
	}
	praw := p.Raw()
	for _, q := range parts {
		if partID(q) == "" && bytes.Equal(q.Raw(), praw) {
			return true
		}
	}
	return false
}

// partText returns the "text" field of a text/reasoning part (ok=false for any
// other part type).
func partText(p protocol.ContentPart) (string, bool) {
	if p.Type() != protocol.ContentText && p.Type() != protocol.ContentReasoning {
		return "", false
	}
	var h struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(p.Raw(), &h)
	return h.Text, true
}
