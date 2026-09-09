// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
)

func TestRunnerPersistsContextOnlyShellWithoutCallingProvider(t *testing.T) {
	store := &fakeStore{}
	model := &fakeProvider{}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{},
		Provider:  model,
		Publisher: publisher,
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
		Model:     "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	record := shellcontext.Record{Command: "pwd", CWD: "/work", Output: "/work\n", ExitCode: 0, TriggerAgent: false}
	part, err := protocol.NewTextPart(shellcontext.ContextText(record))
	if err != nil {
		t.Fatal(err)
	}
	user := protocol.ChatMessage{ID: "msg-shell-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
	if err := shellcontext.Put(&user, record); err != nil {
		t.Fatal(err)
	}
	final, err := runner.Run(context.Background(), protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "thread", TaskID: "task"}, user)
	if err != nil {
		t.Fatal(err)
	}
	if len(model.request.Messages) != 0 {
		t.Fatalf("provider was called: %#v", model.request)
	}
	history := store.HistoryForTest()
	if len(history) != 1 || history[0].ID != user.ID || !shellcontext.IsContextOnly(history[0]) {
		t.Fatalf("history = %#v", history)
	}
	if !shellcontext.IsCommitAck(final) || len(final.Content) != 0 {
		t.Fatalf("final = %#v", final)
	}
	if len(publisher.events) != 2 || publisher.events[0].Type != channel.EventMessageStarted || publisher.events[1].Type != channel.EventMessageFinal {
		t.Fatalf("events = %#v", publisher.events)
	}
}
