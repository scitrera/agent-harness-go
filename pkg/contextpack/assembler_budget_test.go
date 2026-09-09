// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package contextpack

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// A per-call context budget on ctx (the model the runner is about to call) must
// override the static MaxContextTokens ceiling and trim history to fit — the core
// of the escalate-to-smaller-context-model fix.
func Test_Assembler_Build_perCallBudget_overrides_static_ceiling(t *testing.T) {
	blob := strings.Repeat("word ", 3000) // ~sizable text part, well under any byte cap
	mk := func(id string) protocol.ChatMessage {
		p, err := protocol.NewTextPart(blob)
		if err != nil {
			t.Fatalf("text part: %v", err)
		}
		return protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{p}}
	}
	history := []protocol.ChatMessage{mk("m1"), mk("m2"), mk("m3"), mk("m4"), mk("m5")}

	// High static ceiling + no message/byte cap → keeps all history.
	a := NewAssembler(Config{MaxHistoryMessages: 100, MaxTextPartBytes: 1 << 20, MaxContextTokens: 100_000_000})

	full, err := a.Build(context.Background(), nil, history)
	if err != nil {
		t.Fatalf("build full: %v", err)
	}

	// A small per-call budget (the model we're about to call) overrides the ceiling.
	trimmed, err := a.Build(compaction.WithContextBudget(context.Background(), 2000), nil, history)
	if err != nil {
		t.Fatalf("build trimmed: %v", err)
	}

	if len(trimmed) >= len(full) || compaction.EstimateTokens(trimmed) >= compaction.EstimateTokens(full) {
		t.Fatalf("per-call budget did not override static ceiling: full=%d msgs/%d toks, trimmed=%d msgs/%d toks",
			len(full), compaction.EstimateTokens(full), len(trimmed), compaction.EstimateTokens(trimmed))
	}
}

// A zero/absent per-call budget leaves the static MaxContextTokens in force (no
// behavior change when no per-model window is configured).
func Test_Assembler_Build_noPerCallBudget_keeps_static(t *testing.T) {
	p, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	history := []protocol.ChatMessage{{ID: "m1", Role: protocol.RoleUser, Content: []protocol.ContentPart{p}}}
	a := NewAssembler(Config{MaxHistoryMessages: 100, MaxContextTokens: 100_000})

	base, err := a.Build(context.Background(), nil, history)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// A zero budget is ignored (WithContextBudget<=0 → ContextBudgetFrom returns 0).
	withZero, err := a.Build(compaction.WithContextBudget(context.Background(), 0), nil, history)
	if err != nil {
		t.Fatalf("build zero: %v", err)
	}
	if len(withZero) != len(base) {
		t.Fatalf("zero per-call budget changed the build: base=%d withZero=%d", len(base), len(withZero))
	}
}
