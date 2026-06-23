package compaction

import (
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func Test_Reduce_preserves_recent_messages_when_history_exceeds_limit(t *testing.T) {
	// Given
	messages := make([]protocol.ChatMessage, 0, 4)
	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		part, err := protocol.NewTextPart(id)
		if err != nil {
			t.Fatalf("text part: %v", err)
		}
		messages = append(messages, protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{part}})
	}

	// When
	reduced, err := Reduce(messages, Config{MaxMessages: 2})

	// Then
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if len(reduced) != 3 {
		t.Fatalf("expected summary plus two messages, got %#v", reduced)
	}
	if reduced[0].ID != "compaction-summary" || reduced[1].ID != "m3" || reduced[2].ID != "m4" {
		t.Fatalf("unexpected reduced messages: %#v", reduced)
	}
}

func Test_Reduce_trims_large_text_parts_when_part_exceeds_limit(t *testing.T) {
	// Given
	part, err := protocol.NewTextPart("abcdefghijklmnopqrstuvwxyz")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	messages := []protocol.ChatMessage{{ID: "m1", Role: protocol.RoleToolResult, Content: []protocol.ContentPart{part}}}

	// When
	reduced, err := Reduce(messages, Config{MaxTextPartBytes: 5})

	// Then
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	tp, ok := reduced[0].Content[0].AsText()
	if !ok {
		t.Fatalf("expected text part")
	}
	if tp.Text != "abcde\n[truncated]" {
		t.Fatalf("unexpected trimmed text: %q", tp.Text)
	}
}
