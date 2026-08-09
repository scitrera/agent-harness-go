// Package tasklifecycle decorates a turn executor with an authoritative task's
// claim and terminal transitions. It is transport-neutral; Aether is one host.
package tasklifecycle

import (
	"context"
	"errors"
	"log/slog"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

type TaskOps interface {
	ClaimTask(ctx context.Context, taskID string) error
	CompleteTask(ctx context.Context, taskID string) error
	FailTask(ctx context.Context, taskID, reason string) error
}

// Predicate selects turns backed by a real task. Nil selects every non-empty
// task ID. Hosts that also mint local correlation IDs can restrict lifecycle
// operations to their trusted task envelope.
type Predicate func(addr protocol.MessageAddress, user protocol.ChatMessage) bool

type Executor struct {
	inner     runtime.TurnExecutor
	tasks     TaskOps
	predicate Predicate
}

func Wrap(inner runtime.TurnExecutor, tasks TaskOps) runtime.TurnExecutor {
	return WrapWhen(inner, tasks, nil)
}

func WrapWhen(inner runtime.TurnExecutor, tasks TaskOps, predicate Predicate) runtime.TurnExecutor {
	if tasks == nil {
		return inner
	}
	return Executor{inner: inner, tasks: tasks, predicate: predicate}
}

func (e Executor) Run(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error) {
	taskID := addr.TaskID
	managed := taskID != "" && (e.predicate == nil || e.predicate(addr, user))
	if managed {
		slog.InfoContext(ctx, "task lifecycle: claiming", slog.String("task", taskID), slog.String("thread", addr.ThreadID))
		if err := e.tasks.ClaimTask(ctx, taskID); err != nil {
			slog.WarnContext(ctx, "task lifecycle: claim failed", slog.String("task", taskID), slog.Any("err", err))
		}
	}
	message, runErr := e.inner.Run(ctx, addr, user)
	if !managed {
		return message, runErr
	}
	switch {
	case errors.Is(runErr, turncancel.ErrTurnCancelled):
		slog.InfoContext(ctx, "task lifecycle: cancelled by user", slog.String("task", taskID))
		runErr = nil
	case runErr != nil:
		slog.ErrorContext(ctx, "task lifecycle: turn failed", slog.String("task", taskID), slog.Any("err", runErr))
		if err := e.tasks.FailTask(ctx, taskID, runErr.Error()); err != nil {
			slog.WarnContext(ctx, "task lifecycle: fail call failed", slog.String("task", taskID), slog.Any("err", err))
		}
	default:
		slog.InfoContext(ctx, "task lifecycle: turn completed", slog.String("task", taskID))
		if err := e.tasks.CompleteTask(ctx, taskID); err != nil {
			slog.WarnContext(ctx, "task lifecycle: complete call failed", slog.String("task", taskID), slog.Any("err", err))
		}
	}
	return message, runErr
}

var _ runtime.TurnExecutor = Executor{}
