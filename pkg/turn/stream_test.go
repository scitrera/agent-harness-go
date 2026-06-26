package turn

import (
	"context"
	"encoding/json"
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
	s := newTurnStreamer(pub, addr, streamMessageID(addr), func() time.Time { return fixed })

	if err := s.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := s.finalize(context.Background(), protocol.ChatMessage{Role: protocol.RoleAssistant}); err != nil {
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
	s := newTurnStreamer(pub, addr, streamMessageID(addr), nil)

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
	})
	if err := s.finalize(context.Background(), protocol.ChatMessage{Role: protocol.RoleAssistant, CreatedAt: "caller-set"}); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	last := pub.events[len(pub.events)-1]
	if last.Message == nil || last.Message.CreatedAt != "caller-set" {
		t.Fatalf("finalize clobbered caller created_at")
	}
}
