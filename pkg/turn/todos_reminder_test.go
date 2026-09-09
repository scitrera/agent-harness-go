// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestRemainingTodosMessage_OnlyOutstanding(t *testing.T) {
	todos := []compaction.TodoState{
		{ID: "1", Content: "wire the thing", Status: "completed"},
		{ID: "2", Content: "test the thing", Status: "in_progress"},
		{ID: "3", Content: "ship the thing", Status: "pending"},
		{ID: "4", Content: "abandoned", Status: "cancelled"},
		{ID: "5", Content: "   ", Status: "pending"}, // blank -> skipped
	}
	msg, ok := remainingTodosMessage(todos, protocol.MessageAddress{})
	if !ok {
		t.Fatal("expected a reminder message for outstanding todos")
	}
	if msg.Role != protocol.RoleSystem || msg.ID != "remaining-todos" {
		t.Fatalf("unexpected envelope: role=%s id=%s", msg.Role, msg.ID)
	}
	text := textOf(msg)
	for _, want := range []string{
		"Remaining todo items",
		"[in progress] test the thing",
		"[pending] ship the thing",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
	// completed / cancelled / blank are excluded.
	for _, no := range []string{"wire the thing", "abandoned"} {
		if strings.Contains(text, no) {
			t.Fatalf("did not expect %q in:\n%s", no, text)
		}
	}
}

func TestRemainingTodosMessage_NoneOutstanding(t *testing.T) {
	cases := [][]compaction.TodoState{
		nil,
		{{ID: "1", Content: "done", Status: "completed"}},
		{{ID: "2", Content: "x", Status: "cancelled"}},
		{{ID: "3", Content: "  ", Status: "pending"}}, // only blank -> nothing
	}
	for i, todos := range cases {
		if _, ok := remainingTodosMessage(todos, protocol.MessageAddress{}); ok {
			t.Fatalf("case %d: expected no message for %+v", i, todos)
		}
	}
}
