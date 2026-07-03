package contextpack

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
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

// A WorldState carrying an active sub-agent (its thread_id handle surviving in
// history meta) is surfaced as a "## Sub-agents" prompt line — the spawn→sink→
// WorldState→prompt flow end-to-end.
func Test_Assembler_Build_surfaces_active_subagents(t *testing.T) {
	ctx := compaction.WithTurnNumber(context.Background(), 6)
	history := worldStateHistory(t, compaction.WorldState{
		Turn: 5,
		ActiveSubagents: []compaction.SubagentHandle{
			{ID: "parent::sub::3", Name: "reviewer", Status: "completed", Summary: "found the bug", LastTurn: 5},
		},
	})
	a := NewAssembler(Config{})
	msgs, err := a.Build(ctx, nil, history)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	txt := promptText(t, msgs)
	for _, want := range []string{
		"## Sub-agents",
		"reviewer (parent::sub::3): completed — found the bug (1 turn ago)",
	} {
		if !strings.Contains(txt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, txt)
		}
	}
	// No sub-agents in history -> no section.
	empty := promptText(t, mustBuild(t, NewAssembler(Config{})))
	if strings.Contains(empty, "## Sub-agents") {
		t.Fatalf("no sub-agents should mean no section:\n%s", empty)
	}
}
