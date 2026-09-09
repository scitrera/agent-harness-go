// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// overflowThenOKProvider returns a classified context_overflow error for the
// first N calls, then succeeds. It records the message count of each request.
type overflowThenOKProvider struct {
	failTimes int
	calls     int
	msgCounts []int
}

func (p *overflowThenOKProvider) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	p.calls++
	p.msgCounts = append(p.msgCounts, len(req.Messages))
	if p.calls <= p.failTimes {
		return provider.ChatResponse{}, &provider.ProviderError{Kind: provider.FailureContextOverflow, Status: 400, Body: "maximum context length exceeded"}
	}
	part, _ := protocol.NewTextPart("recovered")
	return provider.ChatResponse{Message: protocol.ChatMessage{ID: "assistant-ok", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}, nil
}

func Test_Runner_Run_recovers_from_context_overflow(t *testing.T) {
	prov := &overflowThenOKProvider{failTimes: 1}
	// Seed enough history that trimming actually drops messages.
	store := &fakeStore{}
	for i := 0; i < 6; i++ {
		store.messages = append(store.messages, userMessage(t, "history line"))
	}
	r, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{},
		Provider:  prov,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	assistant, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "new question"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if assistant.ID != "assistant-ok" {
		t.Fatalf("expected recovered assistant, got %#v", assistant)
	}
	if prov.calls != 2 {
		t.Fatalf("expected one overflow + one success = 2 calls, got %d", prov.calls)
	}
	// The retry should have sent fewer messages than the first attempt.
	if len(prov.msgCounts) != 2 || prov.msgCounts[1] >= prov.msgCounts[0] {
		t.Fatalf("retry should trim history: counts=%v", prov.msgCounts)
	}
}

func Test_Runner_Run_surfaces_overflow_after_max_retries(t *testing.T) {
	prov := &overflowThenOKProvider{failTimes: 99} // never succeeds
	store := &fakeStore{}
	for i := 0; i < 6; i++ {
		store.messages = append(store.messages, userMessage(t, "history line"))
	}
	r, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{},
		Provider:  prov,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err == nil {
		t.Fatal("expected overflow to surface after retries are exhausted")
	}
	// initial attempt + maxOverflowRetries.
	if prov.calls != 1+maxOverflowRetries {
		t.Fatalf("expected %d attempts, got %d", 1+maxOverflowRetries, prov.calls)
	}
}
