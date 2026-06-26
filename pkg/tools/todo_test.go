package tools

import (
	"context"
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type capturingEmitter struct{ parts []protocol.ContentPart }

func (c *capturingEmitter) UpsertPart(_ context.Context, p protocol.ContentPart) error {
	c.parts = append(c.parts, p)
	return nil
}

func Test_todoWrite_emits_todo_part_and_summarizes(t *testing.T) {
	em := &capturingEmitter{}
	ctx := WithPartEmitter(context.Background(), em)
	args := `{"title":"Release","items":[` +
		`{"id":"t1","content":"Port","status":"completed"},` +
		`{"id":"t2","content":"Wire","status":"in_progress","active_form":"Wiring"}]}`

	res, err := todoWrite(ctx, Request{CallID: "c1", Name: "todo_write", Arguments: json.RawMessage(args)})
	if err != nil {
		t.Fatalf("todoWrite: %v", err)
	}
	if len(em.parts) != 1 {
		t.Fatalf("expected 1 emitted part, got %d", len(em.parts))
	}
	body, ok := em.parts[0].AsTodo()
	if !ok {
		t.Fatalf("emitted part is not a todo: %s", em.parts[0].Raw())
	}
	if body.ID != "todo_main" || len(body.Items) != 2 {
		t.Fatalf("todo body = %#v", body)
	}
	if body.Items[1].Status != spec.TodoInProgress || body.Items[1].ActiveForm != "Wiring" {
		t.Fatalf("items wrong: %#v", body.Items)
	}

	var summary struct {
		TodoID   string         `json:"todo_id"`
		Count    int            `json:"count"`
		ByStatus map[string]int `json:"by_status"`
	}
	if err := json.Unmarshal(res.Payload, &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.TodoID != "todo_main" || summary.Count != 2 {
		t.Fatalf("summary = %#v", summary)
	}
	if summary.ByStatus["completed"] != 1 || summary.ByStatus["in_progress"] != 1 {
		t.Fatalf("by_status = %#v", summary.ByStatus)
	}
}

func Test_todoWrite_is_noop_without_emitter(t *testing.T) {
	res, err := todoWrite(context.Background(), Request{
		CallID: "c1", Name: "todo_write",
		Arguments: json.RawMessage(`{"items":[{"content":"x","status":"pending"}]}`),
	})
	if err != nil {
		t.Fatalf("todoWrite without emitter: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result: %s", res.Payload)
	}
}
