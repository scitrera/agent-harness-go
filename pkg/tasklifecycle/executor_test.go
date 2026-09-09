// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tasklifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type lifecycleOps struct {
	claimed, completed, failed     int
	claimErr, completeErr, failErr error
}

func (o *lifecycleOps) ClaimTask(context.Context, string) error { o.claimed++; return o.claimErr }
func (o *lifecycleOps) CompleteTask(context.Context, string) error {
	o.completed++
	return o.completeErr
}
func (o *lifecycleOps) FailTask(context.Context, string, string) error { o.failed++; return o.failErr }

type lifecycleExecutor struct{ err error }

func (e lifecycleExecutor) Run(context.Context, protocol.MessageAddress, protocol.ChatMessage) (protocol.ChatMessage, error) {
	return protocol.ChatMessage{ID: "assistant"}, e.err
}

func TestWrapWhenOnlyMutatesSelectedTasks(t *testing.T) {
	ops := &lifecycleOps{}
	executor := WrapWhen(lifecycleExecutor{}, ops, func(_ protocol.MessageAddress, user protocol.ChatMessage) bool {
		return user.ID == "managed"
	})
	if _, err := executor.Run(context.Background(), protocol.MessageAddress{TaskID: "local"}, protocol.ChatMessage{ID: "local"}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Run(context.Background(), protocol.MessageAddress{TaskID: "task"}, protocol.ChatMessage{ID: "managed"}); err != nil {
		t.Fatal(err)
	}
	if ops.claimed != 1 || ops.completed != 1 || ops.failed != 0 {
		t.Fatalf("ops = %#v", ops)
	}
}

func TestWrapFailsManagedTaskOnTurnError(t *testing.T) {
	ops := &lifecycleOps{}
	executor := Wrap(lifecycleExecutor{err: errors.New("boom")}, ops)
	if _, err := executor.Run(context.Background(), protocol.MessageAddress{TaskID: "task"}, protocol.ChatMessage{}); err == nil {
		t.Fatal("turn error was swallowed")
	}
	if ops.claimed != 1 || ops.failed != 1 || ops.completed != 0 {
		t.Fatalf("ops = %#v", ops)
	}
}

type acknowledgingExecutor struct {
	runs         int
	acknowledged int
	ackErr       error
}

func (e *acknowledgingExecutor) Run(ctx context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) (protocol.ChatMessage, error) {
	e.runs++
	if !IsManagedTask(ctx) {
		return protocol.ChatMessage{}, errors.New("managed task marker was not propagated")
	}
	return protocol.ChatMessage{ID: "assistant"}, nil
}

func (e *acknowledgingExecutor) AcknowledgeTaskTerminal(context.Context, string, string) error {
	e.acknowledged++
	return e.ackErr
}

func TestWrapFailsClosedWhenClaimIsNotAuthoritative(t *testing.T) {
	ops := &lifecycleOps{claimErr: errors.New("claim rejected")}
	inner := &acknowledgingExecutor{}
	executor := Wrap(inner, ops)
	if _, err := executor.Run(context.Background(), protocol.MessageAddress{TaskID: "task"}, protocol.ChatMessage{}); err == nil {
		t.Fatal("claim rejection was ignored")
	}
	if inner.runs != 0 || inner.acknowledged != 0 || ops.completed != 0 || ops.failed != 0 {
		t.Fatalf("execution crossed rejected claim: inner=%#v ops=%#v", inner, ops)
	}
}

func TestWrapLeavesTerminalIntentUnacknowledgedWhenTaskHostFails(t *testing.T) {
	ops := &lifecycleOps{completeErr: errors.New("completion response lost")}
	inner := &acknowledgingExecutor{}
	executor := Wrap(inner, ops)
	message, err := executor.Run(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a", TaskID: "task"}, protocol.ChatMessage{})
	if err == nil || message.ID != "assistant" {
		t.Fatalf("message=%+v err=%v", message, err)
	}
	if inner.runs != 1 || inner.acknowledged != 0 || ops.completed != 1 {
		t.Fatalf("ambiguous completion was acknowledged: inner=%#v ops=%#v", inner, ops)
	}
}

func TestWrapAcknowledgesTurnOnlyAfterTaskTerminalConfirmation(t *testing.T) {
	ops := &lifecycleOps{}
	inner := &acknowledgingExecutor{}
	executor := Wrap(inner, ops)
	if _, err := executor.Run(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a", TaskID: "task"}, protocol.ChatMessage{}); err != nil {
		t.Fatal(err)
	}
	if inner.runs != 1 || inner.acknowledged != 1 || ops.completed != 1 {
		t.Fatalf("terminal handshake = inner=%#v ops=%#v", inner, ops)
	}
}
