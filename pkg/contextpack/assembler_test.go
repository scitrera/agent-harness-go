package contextpack

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func Test_Assembler_Build_includes_bootstrap_then_compacted_history(t *testing.T) {
	// Given
	ctx := context.Background()
	assembler := NewAssembler(Config{MaxHistoryMessages: 1})
	part1, err := protocol.NewTextPart("first")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	part2, err := protocol.NewTextPart("second")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	history := []protocol.ChatMessage{
		{ID: "m1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part1}},
		{ID: "m2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part2}},
	}
	bootstrap := []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}

	// When
	messages, err := assembler.Build(ctx, bootstrap, history)

	// Then
	if err != nil {
		t.Fatalf("build context: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("expected system prompt, summary, and last history message; got %#v", messages)
	}
	if messages[0].Role != protocol.RoleSystem || messages[0].ID != "system-prompt" {
		t.Fatalf("expected system-prompt message first, got %#v", messages[0])
	}
	tp, ok := messages[0].Content[0].AsText()
	if !ok {
		t.Fatalf("system prompt is not text: %#v", messages[0])
	}
	// Base instructions present, and the SOUL.md bootstrap content framed under
	// project context.
	for _, want := range []string{"autonomous assistant", "## Project context", "### SOUL.md", "name: Scitrera", "Runtime:"} {
		if !strings.Contains(tp.Text, want) {
			t.Fatalf("system prompt missing %q:\n%s", want, tp.Text)
		}
	}
	if messages[1].ID != "compaction-summary" || messages[2].ID != "m2" {
		t.Fatalf("unexpected message order: %#v", messages)
	}
}
