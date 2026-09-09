// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package compaction

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func msg(t *testing.T, id, text string) protocol.ChatMessage {
	t.Helper()
	p, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	return protocol.ChatMessage{ID: id, Role: protocol.RoleUser, Content: []protocol.ContentPart{p}}
}

func TestEstimateTokensGrowsWithContent(t *testing.T) {
	small := EstimateTokens([]protocol.ChatMessage{msg(t, "a", "hi")})
	big := EstimateTokens([]protocol.ChatMessage{msg(t, "b", strings.Repeat("x", 4000))})
	if big <= small {
		t.Fatalf("expected bigger content to estimate more tokens: small=%d big=%d", small, big)
	}
	if big < 900 || big > 1100 {
		t.Fatalf("4000 bytes should be ~1000 tokens, got %d", big)
	}
}

func TestReduceTrimsToTokenBudget(t *testing.T) {
	// Three ~1000-token messages; budget of ~1200 should keep only the newest
	// (always keeps at least the last message).
	body := strings.Repeat("x", 4000)
	in := []protocol.ChatMessage{
		msg(t, "old", body),
		msg(t, "mid", body),
		msg(t, "new", body),
	}
	out, err := Reduce(in, Config{MaxTokens: 1200})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if len(out) != 1 || out[0].ID != "new" {
		t.Fatalf("expected only newest message within budget, got %#v", ids(out))
	}
}

func TestReduceWithReportCountsTokenBudgetCompactedMessages(t *testing.T) {
	// Given
	body := strings.Repeat("x", 4000)
	in := []protocol.ChatMessage{
		msg(t, "m1", body),
		msg(t, "m2", body),
		msg(t, "m3", body),
		msg(t, "m4", body),
		msg(t, "m5", body),
	}

	// When
	report, err := ReduceWithReport(in, Config{MaxMessages: 4, MaxTokens: 2500})

	// Then
	if err != nil {
		t.Fatalf("reduce with report: %v", err)
	}
	if got := ids(report.Messages); len(got) != 2 || got[0] != "m4" || got[1] != "m5" {
		t.Fatalf("expected newest messages after token trim, got %#v", got)
	}
	if report.Budget.CompactedMessages != 3 {
		t.Fatalf("expected all removed original messages counted, got %#v", report.Budget)
	}
	var meta ContextBudget
	if err := json.Unmarshal(report.Messages[0].Meta[MetaContextBudget], &meta); err != nil {
		t.Fatalf("decode budget meta: %v", err)
	}
	if meta.MessageCount != 2 || meta.CompactedMessages != 3 || meta.EstimatedTokens > meta.MaxTokens {
		t.Fatalf("unexpected budget meta: %#v", meta)
	}
}

func TestReduceTokenBudgetKeepsAllWhenUnderBudget(t *testing.T) {
	in := []protocol.ChatMessage{msg(t, "a", "short"), msg(t, "b", "also short")}
	out, err := Reduce(in, Config{MaxTokens: 100000})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected all messages under budget, got %#v", ids(out))
	}
}

func ids(messages []protocol.ChatMessage) []string {
	out := make([]string, len(messages))
	for i, m := range messages {
		out[i] = m.ID
	}
	return out
}
