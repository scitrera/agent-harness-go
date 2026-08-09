package tasklifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type lifecycleOps struct{ claimed, completed, failed int }

func (o *lifecycleOps) ClaimTask(context.Context, string) error        { o.claimed++; return nil }
func (o *lifecycleOps) CompleteTask(context.Context, string) error     { o.completed++; return nil }
func (o *lifecycleOps) FailTask(context.Context, string, string) error { o.failed++; return nil }

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
