// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// The harness must stamp created_at on its assistant messages so persisted
// history orders correctly on reload, and started+finalized must share one
// creation time.
func TestTurnStreamer_StampsCreatedAt(t *testing.T) {
	fixed := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1", ThreadID: "th1"}
	s := newTurnStreamer(pub, addr, streamMessageID(addr), func() time.Time { return fixed }, 0)

	if err := s.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := s.finalize(context.Background(), protocol.ChatMessage{Role: protocol.RoleAssistant}); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	want := fixed.Format(time.RFC3339Nano)
	if len(pub.events) < 2 {
		t.Fatalf("expected started+finalized events, got %d", len(pub.events))
	}
	started, last := pub.events[0], pub.events[len(pub.events)-1]
	if started.Message == nil || started.Message.CreatedAt != want {
		t.Fatalf("started created_at != %q", want)
	}
	if last.Message == nil || last.Message.CreatedAt != want {
		t.Fatalf("finalized created_at != %q", want)
	}
	if started.Message.CreatedAt != last.Message.CreatedAt {
		t.Fatalf("started/finalized created_at differ: %q vs %q", started.Message.CreatedAt, last.Message.CreatedAt)
	}
}

// updatePart emits a part_updated event carrying the patch, for evolving an
// in-place part (e.g. a todo checklist) after it was first appended.
func TestTurnStreamer_UpdatePartEmitsPatch(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1", ThreadID: "th1"}
	s := newTurnStreamer(pub, addr, streamMessageID(addr), nil, 0)

	patch := map[string]json.RawMessage{"items": json.RawMessage(`[{"id":"a","status":"completed"}]`)}
	if err := s.updatePart(context.Background(), 2, patch); err != nil {
		t.Fatalf("updatePart: %v", err)
	}
	last := pub.events[len(pub.events)-1]
	if last.Type != channel.EventPartUpdated || last.Index != 2 {
		t.Fatalf("expected part_updated at index 2, got %#v", last)
	}
	if string(last.Patch["items"]) != `[{"id":"a","status":"completed"}]` {
		t.Fatalf("patch not carried: %#v", last.Patch)
	}
	before := len(pub.events)
	if err := s.updatePart(context.Background(), 2, nil); err != nil {
		t.Fatalf("updatePart nil: %v", err)
	}
	if len(pub.events) != before {
		t.Fatalf("empty patch should not emit an event")
	}
}

// finalize must not clobber a caller-provided created_at.
func TestTurnStreamer_FinalizeKeepsCallerCreatedAt(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1"}
	s := newTurnStreamer(pub, addr, streamMessageID(addr), func() time.Time {
		return time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	}, 0)
	if _, err := s.finalize(context.Background(), protocol.ChatMessage{Role: protocol.RoleAssistant, CreatedAt: "caller-set"}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	last := pub.events[len(pub.events)-1]
	if last.Message == nil || last.Message.CreatedAt != "caller-set" {
		t.Fatalf("finalize clobbered caller created_at")
	}
}

func streamedDeltas(pub *fakePublisher) []string {
	var out []string
	for _, e := range pub.events {
		if e.Type == channel.EventTokenDelta {
			out = append(out, e.Delta)
		}
	}
	return out
}

// Regression: with coalescing on, appendUnstreamed must FLUSH the buffered token
// deltas before its dedup check — otherwise the reconstruction is missing the
// still-buffered tail, the provider's full text part fails to match, and it gets
// re-appended, DOUBLING the inter-tool-call assistant text (the frontend then
// rendered each narration line twice on tool-use turns).
func TestTurnStreamer_AppendUnstreamed_NoDuplicateWithBufferedDeltas(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1"}
	clock := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC) // never advances → only 1st token flushes
	s := newTurnStreamer(pub, addr, streamMessageID(addr), func() time.Time { return clock }, 50*time.Millisecond)
	ctx := context.Background()

	idx, err := s.appendTextStream(ctx)
	if err != nil {
		t.Fatalf("appendTextStream: %v", err)
	}
	for _, tok := range []string{"I'll ", "update ", "the ", "file."} {
		if err := s.tokenDelta(ctx, idx, tok); err != nil {
			t.Fatalf("tokenDelta: %v", err)
		}
	}

	// The provider surfaces the FULL text part (tool loop → appendUnstreamed).
	full, err := protocol.NewTextPart("I'll update the file.")
	if err != nil {
		t.Fatalf("NewTextPart: %v", err)
	}
	appended, err := s.appendUnstreamed(ctx, full)
	if err != nil {
		t.Fatalf("appendUnstreamed: %v", err)
	}
	if appended {
		t.Fatalf("appendUnstreamed re-appended an already-streamed text part (doubling bug)")
	}
	textParts := 0
	for _, p := range s.state[s.msgID].Content {
		if tp, ok := p.AsText(); ok {
			textParts++
			if tp.Text != "I'll update the file." {
				t.Fatalf("reconstructed text = %q, want full text", tp.Text)
			}
		}
	}
	if textParts != 1 {
		t.Fatalf("reconstruction has %d text parts, want exactly 1 (no duplicate)", textParts)
	}
}

// With coalescing enabled, deltas within an interval are buffered into a single
// token_delta (the first delta flushes immediately for low TTFT), and the
// trailing buffer is flushed at finalize — before message_finalized — with the
// reassembled text intact.
func TestTurnStreamer_CoalescesTokenDeltas(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1"}
	clock := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	s := newTurnStreamer(pub, addr, streamMessageID(addr), func() time.Time { return clock }, 50*time.Millisecond)
	ctx := context.Background()
	emit := func(text string) {
		if err := s.tokenDelta(ctx, 0, text); err != nil {
			t.Fatalf("tokenDelta: %v", err)
		}
	}

	emit("Hel") // first delta → flush immediately
	emit("lo")  // buffered
	emit(", ")  // buffered
	if got := streamedDeltas(pub); len(got) != 1 || got[0] != "Hel" {
		t.Fatalf("after buffering want [Hel], got %v", got)
	}

	clock = clock.Add(60 * time.Millisecond)
	emit("world") // interval elapsed → flush buffered "lo, world"
	if got := streamedDeltas(pub); len(got) != 2 || got[1] != "lo, world" {
		t.Fatalf("after interval flush want 2nd delta 'lo, world', got %v", got)
	}

	emit("!") // buffered (no time advance)
	if _, err := s.finalize(ctx, protocol.ChatMessage{Role: protocol.RoleAssistant}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	got := streamedDeltas(pub)
	if len(got) != 3 || got[2] != "!" {
		t.Fatalf("finalize should flush trailing buffer; want 3 deltas ending '!', got %v", got)
	}
	if strings.Join(got, "") != "Hello, world!" {
		t.Fatalf("reassembled stream = %q, want %q", strings.Join(got, ""), "Hello, world!")
	}

	// The trailing flush must be ordered before message_finalized.
	lastDelta, final := -1, -1
	for i, e := range pub.events {
		switch e.Type {
		case channel.EventTokenDelta:
			lastDelta = i
		case channel.EventMessageFinal:
			final = i
		}
	}
	if final < 0 || lastDelta > final {
		t.Fatalf("token_delta must precede message_finalized (lastDelta=%d final=%d)", lastDelta, final)
	}
}

// flushInterval=0 preserves the original behavior: every delta is emitted
// immediately.
func TestTurnStreamer_NoCoalesceWhenDisabled(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1"}
	s := newTurnStreamer(pub, addr, streamMessageID(addr), nil, 0)
	ctx := context.Background()
	for _, tok := range []string{"a", "b", "c"} {
		if err := s.tokenDelta(ctx, 0, tok); err != nil {
			t.Fatalf("tokenDelta: %v", err)
		}
	}
	if got := streamedDeltas(pub); len(got) != 3 {
		t.Fatalf("flushInterval=0 should emit each delta; want 3, got %d (%v)", len(got), got)
	}
}

func TestTerminalHistoryMessageKeepsOnlyNovelPartialContent(t *testing.T) {
	already, err := protocol.NewTextPart("already persisted")
	if err != nil {
		t.Fatal(err)
	}
	partial, err := protocol.NewTextPart("partial response")
	if err != nil {
		t.Fatal(err)
	}
	finalized := protocol.ChatMessage{
		ID:      "task-1-assistant",
		Role:    protocol.RoleAssistant,
		Content: []protocol.ContentPart{already, partial},
		Meta:    map[string]json.RawMessage{metaCancelledKey: json.RawMessage("true")},
	}

	terminal := terminalHistoryMessage(finalized, []protocol.ChatMessage{{Role: protocol.RoleAssistant, Content: []protocol.ContentPart{already}}})
	if terminal.ID != "task-1-assistant-terminal" {
		t.Fatalf("terminal id = %q", terminal.ID)
	}
	if len(terminal.Content) != 1 {
		t.Fatalf("terminal content = %#v, want only novel part", terminal.Content)
	}
	text, ok := terminal.Content[0].AsText()
	if !ok || text.Text != "partial response" {
		t.Fatalf("terminal partial = %#v", terminal.Content[0])
	}
	if string(terminal.Meta[metaCancelledKey]) != "true" {
		t.Fatalf("terminal meta = %#v", terminal.Meta)
	}
}
