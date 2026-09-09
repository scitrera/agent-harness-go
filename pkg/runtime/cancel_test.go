// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

type blockingExecutor struct {
	started chan struct{}
}

func (e *blockingExecutor) Run(ctx context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) (protocol.ChatMessage, error) {
	close(e.started)
	<-ctx.Done()
	return protocol.ChatMessage{}, ctx.Err()
}

func Test_Runner_RunOnce_cancellable_by_task_id(t *testing.T) {
	exec := &blockingExecutor{started: make(chan struct{})}
	runner, err := NewRunner(fakeSource{envelope: channel.Inbound{
		Addr:    protocol.MessageAddress{ThreadID: "th1", TaskID: "task1"},
		Message: protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser},
	}}, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	canceller := turncancel.New()
	runner.SetCanceller(canceller)

	errCh := make(chan error, 1)
	go func() {
		_, runErr := runner.RunOnce(context.Background())
		errCh <- runErr
	}()

	<-exec.started
	if !canceller.Cancel("task1") {
		t.Fatal("expected an active turn registered under task1")
	}
	select {
	case runErr := <-errCh:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("RunOnce did not return after cancel")
	}
}

func Test_Runner_RunLoop_counts_cancelled_turn(t *testing.T) {
	exec := &fakeExecutor{err: context.Canceled}
	runner, err := NewRunner(fakeSource{envelope: channel.Inbound{
		Addr:    protocol.MessageAddress{ThreadID: "th1", TaskID: "task1"},
		Message: protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser},
	}}, exec)
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	stats, err := runner.RunLoop(context.Background(), LoopConfig{MaxTurns: 1})
	if err != nil {
		t.Fatalf("loop: %v", err)
	}
	if stats.Turns != 1 || stats.Cancelled != 1 || stats.Errors != 0 {
		t.Fatalf("expected 1 cancelled turn, got %+v", stats)
	}
}
