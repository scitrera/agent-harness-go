// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

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

func Test_todoWrite_stable_ids_and_activeform_alias(t *testing.T) {
	em := &capturingEmitter{}
	ctx := WithPartEmitter(context.Background(), em)
	// Two id-less items (the model omits per-item ids) + the no-underscore
	// "activeform" alias, plus one item with an explicit id to confirm it's kept.
	args := `{"items":[` +
		`{"content":"First","status":"completed"},` +
		`{"content":"Second","status":"in_progress","activeform":"Doing second"},` +
		`{"id":"keep-me","content":"Third","status":"pending"}]}`
	if _, err := todoWrite(ctx, Request{CallID: "c1", Name: "todo_write", Arguments: json.RawMessage(args)}); err != nil {
		t.Fatalf("todoWrite: %v", err)
	}
	body, ok := em.parts[0].AsTodo()
	if !ok {
		t.Fatalf("emitted part is not a todo: %s", em.parts[0].Raw())
	}
	// id-less items get a stable positional id <board>#<index> (so a re-wording at
	// the same position updates the same world-state entry instead of piling up).
	if body.Items[0].ID != "todo_main#0" || body.Items[1].ID != "todo_main#1" {
		t.Fatalf("stable positional ids wrong: %q %q", body.Items[0].ID, body.Items[1].ID)
	}
	// an explicit model id is preserved.
	if body.Items[2].ID != "keep-me" {
		t.Fatalf("explicit id not preserved: %q", body.Items[2].ID)
	}
	// "activeform" (no underscore) is honored as active_form.
	if body.Items[1].ActiveForm != "Doing second" {
		t.Fatalf("activeform alias not honored: %q", body.Items[1].ActiveForm)
	}
	// content is preserved on every item.
	if body.Items[0].Content != "First" || body.Items[1].Content != "Second" || body.Items[2].Content != "Third" {
		t.Fatalf("content wrong: %#v", body.Items)
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
