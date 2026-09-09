// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package runtime

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type fakeSource struct {
	envelope channel.Inbound
	err      error
}

func (s fakeSource) FetchTask(_ context.Context) (channel.Inbound, error) {
	if s.err != nil {
		return channel.Inbound{}, s.err
	}
	return s.envelope, nil
}

type fakeExecutor struct {
	addr protocol.MessageAddress
	msg  protocol.ChatMessage
	err  error
}

func (e *fakeExecutor) Run(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
	e.addr = addr
	e.msg = user
	if e.err != nil {
		return protocol.ChatMessage{}, e.err
	}
	part, err := protocol.NewTextPart("done")
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	return protocol.ChatMessage{ID: "assistant-1", Role: protocol.RoleAssistant, Addr: addr, Content: []protocol.ContentPart{part}}, nil
}

func Test_Runner_RunOnce_fetches_aether_task_and_executes_turn(t *testing.T) {
	// Given
	ctx := context.Background()
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	executor := &fakeExecutor{}
	runner, err := NewRunner(fakeSource{envelope: channel.Inbound{
		Addr:    protocol.MessageAddress{ThreadID: "thread-1"},
		Message: protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}},
	}}, executor)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	assistant, err := runner.RunOnce(ctx)

	// Then
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if assistant.ID != "assistant-1" || executor.addr.ThreadID != "thread-1" || executor.msg.ID != "user-1" {
		t.Fatalf("unexpected execution: assistant=%#v addr=%#v msg=%#v", assistant, executor.addr, executor.msg)
	}
}

func Test_Runner_RunOnce_uses_message_address_when_envelope_address_missing(t *testing.T) {
	// Given
	ctx := context.Background()
	executor := &fakeExecutor{}
	runner, err := NewRunner(fakeSource{envelope: channel.Inbound{
		Message: protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Addr: protocol.MessageAddress{ThreadID: "message-thread"}},
	}}, executor)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = runner.RunOnce(ctx)

	// Then
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if executor.addr.ThreadID != "message-thread" {
		t.Fatalf("expected message address fallback, got %#v", executor.addr)
	}
}

func Test_Runner_RunOnce_delegates_missing_thread_to_executor(t *testing.T) {
	// Given: an inbound task with NO thread id. The runtime loop no longer rejects
	// it — thread-id policy belongs to the executor (which mints one). Here the
	// fakeExecutor just records the address it was handed.
	executor := &fakeExecutor{}
	runner, err := NewRunner(fakeSource{envelope: channel.Inbound{}}, executor)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When
	_, err = runner.RunOnce(context.Background())

	// Then: no error, and the empty-thread address was delegated to the executor
	// (which is where minting happens, out of the runtime loop's concern).
	if err != nil {
		t.Fatalf("expected delegation, got error: %v", err)
	}
	if executor.addr.ThreadID != "" {
		t.Fatalf("expected the empty-thread addr passed through to the executor, got %q", executor.addr.ThreadID)
	}
}
