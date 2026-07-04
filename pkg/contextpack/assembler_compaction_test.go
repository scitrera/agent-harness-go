package contextpack

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// The assembler bumps the ctx compaction-event counter when a Build actually drops
// messages (tight budget), and leaves it at zero when nothing is dropped.
func Test_Assembler_Build_notes_compaction_events(t *testing.T) {
	blob := strings.Repeat("word ", 3000)
	mk := func(id string) protocol.ChatMessage {
		p, err := protocol.NewTextPart(blob)
		if err != nil {
			t.Fatalf("text part: %v", err)
		}
		return protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{p}}
	}
	history := []protocol.ChatMessage{mk("m1"), mk("m2"), mk("m3"), mk("m4"), mk("m5")}
	a := NewAssembler(Config{MaxHistoryMessages: 100, MaxTextPartBytes: 1 << 20, MaxContextTokens: 100_000_000})

	// Tight per-call budget forces message drops -> one compaction event noted.
	ctx, counter := compaction.WithCompactionCounter(context.Background())
	ctx = compaction.WithContextBudget(ctx, 2000)
	if _, err := a.Build(ctx, nil, history); err != nil {
		t.Fatalf("build (tight): %v", err)
	}
	if *counter == 0 {
		t.Fatalf("expected a compaction event to be noted when messages are dropped")
	}

	// No budget pressure -> nothing dropped -> counter stays 0.
	ctx2, counter2 := compaction.WithCompactionCounter(context.Background())
	if _, err := a.Build(ctx2, nil, history); err != nil {
		t.Fatalf("build (loose): %v", err)
	}
	if *counter2 != 0 {
		t.Fatalf("no compaction expected without budget pressure, got %d", *counter2)
	}
}
