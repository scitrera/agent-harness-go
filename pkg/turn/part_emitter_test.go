package turn

import (
	"context"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// The emitter appends an id-bearing part once (part_appended), patches it in
// place on subsequent upserts (part_updated), and folds only the latest version
// into the finalized message.
func Test_turnPartEmitter_appends_then_updates_and_folds_latest(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1", ThreadID: "th1"}
	em := newTurnPartEmitter(newTurnStreamer(pub, addr, streamMessageID(addr), nil))

	first := spec.NewTodoPart(spec.TodoPart{ID: "todo_main", Items: []spec.TodoItem{{Content: "A", Status: spec.TodoPending}}})
	second := spec.NewTodoPart(spec.TodoPart{ID: "todo_main", Items: []spec.TodoItem{{Content: "A", Status: spec.TodoCompleted}}})
	if err := em.UpsertPart(context.Background(), first); err != nil {
		t.Fatalf("upsert first: %v", err)
	}
	if err := em.UpsertPart(context.Background(), second); err != nil {
		t.Fatalf("upsert second: %v", err)
	}

	var appended, updated int
	for _, e := range pub.events {
		switch e.Type {
		case channel.EventPartAppended:
			appended++
		case channel.EventPartUpdated:
			updated++
		}
	}
	if appended != 1 || updated != 1 {
		t.Fatalf("expected 1 append + 1 update, got %d/%d", appended, updated)
	}

	durable := em.durableParts()
	if len(durable) != 1 {
		t.Fatalf("expected 1 durable part, got %d", len(durable))
	}
	body, ok := durable[0].AsTodo()
	if !ok || len(body.Items) != 1 || body.Items[0].Status != spec.TodoCompleted {
		t.Fatalf("durable part is not the latest todo: %#v", body)
	}
}

// A part with no id is appended each call and never folded as durable.
func Test_turnPartEmitter_idless_parts_not_tracked(t *testing.T) {
	pub := &fakePublisher{}
	addr := protocol.MessageAddress{TaskID: "t1"}
	em := newTurnPartEmitter(newTurnStreamer(pub, addr, streamMessageID(addr), nil))

	p, err := protocol.NewTextPart("hi")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	if err := em.UpsertPart(context.Background(), p); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if dp := em.durableParts(); len(dp) != 0 {
		t.Fatalf("id-less part should not be durable, got %d", len(dp))
	}
}
