// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// countingStore records how many times history is loaded/saved so an ephemeral
// turn can be asserted to touch neither.
type countingStore struct {
	messages []protocol.ChatMessage
	loads    int
	saves    int
}

func (s *countingStore) LoadHistory(_ context.Context, _ string) ([]protocol.ChatMessage, error) {
	s.loads++
	out := make([]protocol.ChatMessage, len(s.messages))
	copy(out, s.messages)
	return out, nil
}

func (s *countingStore) SaveHistory(_ context.Context, _ string, messages []protocol.ChatMessage) error {
	s.saves++
	s.messages = make([]protocol.ChatMessage, len(messages))
	copy(s.messages, messages)
	return nil
}

func Test_Runner_Run_ephemeral_does_not_load_or_persist_history(t *testing.T) {
	// Given prior durable history AND a memory service with recall+commit enabled.
	prior, err := protocol.NewTextPart("earlier durable message")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	store := &countingStore{messages: []protocol.ChatMessage{
		{ID: "prior-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{prior}},
	}}
	mem := &fakeMemory{hits: []tools.MemoryHit{{ID: "m1", Content: "should not be recalled"}}}
	provider := &fakeProvider{}
	runner, err := NewRunner(Config{
		Store:                    store,
		Loader:                   fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:                 provider,
		Publisher:                &fakePublisher{},
		Assembler:                contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:                    "test-model",
		Memory:                   mem,
		MemoryAutoCommit:         true,
		MemoryAutoRecall:         true,
		MemoryRecallIncludeInput: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	user := userMessage(t, "one-shot synthesize this")

	// When: the turn is marked ephemeral on ctx.
	ctx := WithEphemeral(context.Background())
	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "ws1"}, user)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Then: no durable history is loaded and nothing is persisted.
	if store.loads != 0 {
		t.Fatalf("ephemeral turn loaded durable history %d time(s); want 0", store.loads)
	}
	if store.saves != 0 {
		t.Fatalf("ephemeral turn wrote durable history %d time(s); want 0", store.saves)
	}
	// And: memory recall + commit did not run even though the runner enables them.
	if mem.recallWorkspace != "" {
		t.Fatalf("ephemeral turn ran memory recall (workspace=%q); want none", mem.recallWorkspace)
	}
	if len(mem.appended) != 0 {
		t.Fatalf("ephemeral turn committed to memory: %#v", mem.appended)
	}
	// And: the context sent to the model is ONLY the inbound message (plus the
	// system prompt) — no prior durable message, no recalled-memories injection.
	for _, msg := range provider.request.Messages {
		if msg.ID == "prior-1" {
			t.Fatalf("prior durable history leaked into ephemeral context: %#v", provider.request.Messages)
		}
		if msg.ID == "recalled-memories" {
			t.Fatalf("memory recall leaked into ephemeral context: %#v", provider.request.Messages)
		}
	}
	// And: a final consolidated assistant message is still produced.
	if len(assistant.Content) == 0 {
		t.Fatalf("expected a final assistant message, got empty: %#v", assistant)
	}
}

func Test_Runner_Run_non_ephemeral_still_loads_and_persists(t *testing.T) {
	// Guard the invariant: without the flag, the same runner loads history and
	// persists (default behavior unchanged).
	store := &countingStore{}
	runner, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:  &fakeProvider{},
		Publisher: &fakePublisher{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:     "test-model",
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hi")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if store.loads != 1 {
		t.Fatalf("non-ephemeral turn loaded history %d time(s); want 1", store.loads)
	}
	if store.saves == 0 {
		t.Fatalf("non-ephemeral turn did not persist history")
	}
}
