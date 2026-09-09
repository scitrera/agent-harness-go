// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package steering

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func userMessage(t *testing.T, id, text string) protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
}

func messageText(msg protocol.ChatMessage) string {
	var b strings.Builder
	for _, p := range msg.Content {
		if tp, ok := p.AsText(); ok {
			b.WriteString(tp.Text)
		}
	}
	return b.String()
}

func TestParkIsRejectedWhenNoTurnIsRunning(t *testing.T) {
	// Given: nothing running on the lane.
	inbox := New()

	// When / Then: the caller must be told to dispatch it as an ordinary turn,
	// not left believing the message was accepted.
	if inbox.Park(Key("ws", "thread"), userMessage(t, "m1", "hi")) {
		t.Fatal("Park accepted a message with no turn to deliver it to")
	}
}

func TestParkedMessagesAreDrainedByTheRunningTurn(t *testing.T) {
	// Given
	inbox := New()
	key := Key("ws", "thread")
	done := inbox.Begin(key)

	// When
	if !inbox.Park(key, userMessage(t, "m1", "actually, skip the tests")) {
		t.Fatal("Park rejected a message while a turn was running")
	}
	if !inbox.Park(key, userMessage(t, "m2", "and use yarn")) {
		t.Fatal("Park rejected the second message")
	}
	drained := inbox.Drain(key)

	// Then
	if len(drained) != 2 || drained[0].ID != "m1" || drained[1].ID != "m2" {
		t.Fatalf("drained = %#v, want m1 then m2 in order", drained)
	}
	// A second drain must not redeliver: the model already saw them.
	if again := inbox.Drain(key); len(again) != 0 {
		t.Fatalf("second drain = %#v, want empty", again)
	}
	if leftover := done(); len(leftover) != 0 {
		t.Fatalf("close leftover = %#v, want empty after a full drain", leftover)
	}
}

func TestCloseReturnsUndrainedMessagesForRedelivery(t *testing.T) {
	// Given: a message parked after the turn's last drain — the race this whole
	// type exists to make safe.
	inbox := New()
	key := Key("ws", "thread")
	done := inbox.Begin(key)
	inbox.Park(key, userMessage(t, "late", "one more thing"))

	// When
	leftover := done()

	// Then: it comes back to the caller instead of being stranded in the inbox
	// with no turn left to deliver it.
	if len(leftover) != 1 || leftover[0].ID != "late" {
		t.Fatalf("leftover = %#v, want the undelivered message returned", leftover)
	}
	// And the lane is free again, so the redelivered turn can Begin cleanly.
	if inbox.Park(key, userMessage(t, "m2", "x")) {
		t.Fatal("lane still accepts parks after its turn closed")
	}
}

func TestCloseDoesNotClobberASuccessorTurn(t *testing.T) {
	// Given: a stale close arriving after a new turn already claimed the lane
	// (an out-of-order close must not delete someone else's queue).
	inbox := New()
	key := Key("ws", "thread")
	stale := inbox.Begin(key)
	fresh := inbox.Begin(key)
	inbox.Park(key, userMessage(t, "m1", "for the fresh turn"))

	// When
	if leftover := stale(); len(leftover) != 0 {
		t.Fatalf("stale close returned %#v, want nothing — it parked nothing", leftover)
	}

	// Then: the successor still owns its lane and its message.
	drained := inbox.Drain(key)
	if len(drained) != 1 || drained[0].ID != "m1" {
		t.Fatalf("successor lost its parked message: %#v", drained)
	}
	if leftover := fresh(); len(leftover) != 0 {
		t.Fatalf("fresh close leftover = %#v", leftover)
	}
}

func TestLanesAreIsolatedByWorkspaceAndThread(t *testing.T) {
	// Given: two lanes, only one running.
	inbox := New()
	running := Key("ws-a", "thread-1")
	idle := Key("ws-b", "thread-1")
	done := inbox.Begin(running)
	defer done()

	// When / Then: a message for the idle lane must not ride the running one.
	if inbox.Park(idle, userMessage(t, "m1", "x")) {
		t.Fatal("a message for an idle lane was parked into a different lane's turn")
	}
}

// Two opaque ids must not concatenate into one lane: ("ab","c") and ("a","bc")
// would collide without the length prefix, letting one thread's steering land in
// another thread's turn.
func TestLaneKeysCannotCollideAcrossOpaqueIDs(t *testing.T) {
	if Key("ab", "c") == Key("a", "bc") {
		t.Fatalf("lane key collision: %q", Key("ab", "c"))
	}
}

func TestConcurrentParksAndDrainsLoseNothing(t *testing.T) {
	// Given: parks racing a turn that drains repeatedly, then closes. Every
	// accepted message must surface exactly once — via a drain or the close.
	inbox := New()
	key := Key("ws", "thread")
	done := inbox.Begin(key)

	const senders = 32
	accepted := make([]bool, senders)
	var wg sync.WaitGroup
	seen := map[string]int{}
	var seenMu sync.Mutex

	stop := make(chan struct{})
	var drainer sync.WaitGroup
	drainer.Add(1)
	go func() {
		defer drainer.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, msg := range inbox.Drain(key) {
				seenMu.Lock()
				seen[msg.ID]++
				seenMu.Unlock()
			}
		}
	}()

	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			accepted[i] = inbox.Park(key, userMessage(t, string(rune('a'+i%26))+string(rune('0'+i/26)), "x"))
		}(i)
	}
	wg.Wait()
	close(stop)
	drainer.Wait()

	// When: the turn closes, sweeping anything parked after the last drain.
	for _, msg := range done() {
		seen[msg.ID]++
	}

	// Then
	for i := 0; i < senders; i++ {
		if !accepted[i] {
			t.Fatalf("sender %d was rejected while the turn was running", i)
		}
	}
	total := 0
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("message %q delivered %d times, want exactly 1", id, count)
		}
		total++
	}
	if total != senders {
		t.Fatalf("delivered %d distinct messages, want %d", total, senders)
	}
}

func TestNilInboxIsInert(t *testing.T) {
	// Given: a host that never wired steering. Every path must behave as before.
	var inbox *Inbox
	key := Key("ws", "thread")

	if inbox.Park(key, protocol.ChatMessage{}) {
		t.Fatal("nil inbox accepted a park")
	}
	if got := inbox.Drain(key); got != nil {
		t.Fatalf("nil inbox drained %#v", got)
	}
	if got := inbox.Begin(key)(); got != nil {
		t.Fatalf("nil inbox close returned %#v", got)
	}
}

func TestEmptyKeyIsInert(t *testing.T) {
	// A message with no thread identity has no lane to be steered into; it must
	// fall through to the ordinary path rather than be parked somewhere arbitrary.
	inbox := New()
	if inbox.Park("", protocol.ChatMessage{}) {
		t.Fatal("empty key accepted a park")
	}
	if got := inbox.Begin("")(); got != nil {
		t.Fatalf("empty key close returned %#v", got)
	}
}

func TestMarkAndIsRequestedRoundTrip(t *testing.T) {
	msg := userMessage(t, "m1", "hi")
	if IsRequested(msg) {
		t.Fatal("an unmarked message reported as a steering send")
	}
	marked := Mark(msg)
	if !IsRequested(marked) {
		t.Fatal("Mark did not survive IsRequested")
	}
	// Marking is idempotent — a relayed message must not accumulate markers.
	if again := Mark(marked); len(again.Meta) != len(marked.Meta) {
		t.Fatalf("re-marking changed meta: %#v", again.Meta)
	}
}

func TestMalformedMarkerIsNotTreatedAsSteering(t *testing.T) {
	// Failing toward "ordinary message" costs a wait; failing the other way
	// swallows the user's words into an unrelated turn.
	msg := userMessage(t, "m1", "hi")
	msg.Meta = map[string]json.RawMessage{MetaKey: json.RawMessage(`"yes"`)}
	if IsRequested(msg) {
		t.Fatal("a malformed marker was treated as a steering send")
	}
}

func TestDeliverWrapsTheWholeTextAndKeepsAttachmentsAfterIt(t *testing.T) {
	// Given a steering send with text plus an image.
	text, _ := protocol.NewTextPart("use yarn, not npm")
	image, err := protocol.NewImagePart(protocol.ImagePart{Mime: "image/png", URI: "https://example.test/a.png"})
	if err != nil {
		t.Fatalf("image part: %v", err)
	}
	msg := Mark(protocol.ChatMessage{
		ID: "m1", Role: protocol.RoleUser,
		Content: []protocol.ContentPart{text, image},
	})
	addr := protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "thread"}

	// When
	delivered, err := Deliver(addr, msg)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}

	// Then: the block is the entire text — otherwise the model reads it as a new
	// task and abandons the work in flight.
	body := messageText(delivered)
	if !strings.HasPrefix(body, OpenTag) || !strings.HasSuffix(body, CloseTag) {
		t.Fatalf("delivered text is not wholly wrapped: %q", body)
	}
	if !strings.Contains(body, "use yarn, not npm") {
		t.Fatalf("delivered text lost the user's words: %q", body)
	}
	// The attachment rides after the block rather than inside it.
	if len(delivered.Content) != 2 || delivered.Content[1].Type() != protocol.ContentImage {
		t.Fatalf("attachment not preserved after the block: %#v", delivered.Content)
	}
	if delivered.Role != protocol.RoleUser {
		t.Fatalf("delivered role = %s, want user", delivered.Role)
	}
	if delivered.Addr.WorkspaceID != addr.WorkspaceID || delivered.Addr.ThreadID != addr.ThreadID {
		t.Fatalf("delivered addr = %#v, want the running turn's address", delivered.Addr)
	}
	// The marker must not persist into history, or a replay would re-park it.
	if IsRequested(delivered) {
		t.Fatal("the steering marker leaked into the delivered message")
	}
}

func TestDeliverKeepsAnImageOnlyInterjectionRecognizable(t *testing.T) {
	// Given: no text at all.
	image, err := protocol.NewImagePart(protocol.ImagePart{Mime: "image/png", URI: "https://example.test/a.png"})
	if err != nil {
		t.Fatalf("image part: %v", err)
	}
	msg := protocol.ChatMessage{ID: "m1", Role: protocol.RoleUser, Content: []protocol.ContentPart{image}}

	// When
	delivered, err := Deliver(protocol.MessageAddress{ThreadID: "t"}, msg)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}

	// Then: still delivered as steering, not as a bare image that reads like a
	// fresh task.
	body := messageText(delivered)
	if !strings.HasPrefix(body, OpenTag) || !strings.HasSuffix(body, CloseTag) {
		t.Fatalf("image-only interjection lost its steering identity: %q", body)
	}
}
