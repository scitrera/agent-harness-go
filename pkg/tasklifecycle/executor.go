// Package tasklifecycle decorates a turn executor with an authoritative task's
// claim and terminal transitions. It is transport-neutral; Aether is one host.
package tasklifecycle

import (
	"context"
	"errors"
	"fmt"
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

// TerminalAcknowledger closes the durable turn-side half of a task terminal
// transition after TaskOps has confirmed the authoritative task state. A turn
// executor that does not keep a durable execution journal need not implement
// it.
type TerminalAcknowledger interface {
	AcknowledgeTaskTerminal(ctx context.Context, workspaceID, taskID string) error
}

type managedTaskContextKey struct{}

// WithManagedTask marks a turn whose claim and terminal state are owned by an
// external TaskOps implementation. Durable turn executors use this marker to
// leave a terminal intent recoverable until Aether (or another task host) has
// acknowledged it.
func WithManagedTask(ctx context.Context) context.Context {
	return context.WithValue(ctx, managedTaskContextKey{}, true)
}

// IsManagedTask reports whether the turn is inside an authoritative external
// task lifecycle.
func IsManagedTask(ctx context.Context) bool {
	managed, _ := ctx.Value(managedTaskContextKey{}).(bool)
	return managed
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
			slog.ErrorContext(ctx, "task lifecycle: claim failed", slog.String("task", taskID), slog.Any("err", err))
			return protocol.ChatMessage{}, fmt.Errorf("task lifecycle: claim %q before execution: %w", taskID, err)
		}
		ctx = WithManagedTask(ctx)
	}
	message, runErr := e.inner.Run(ctx, addr, user)
	if !managed {
		return message, runErr
	}
	// Terminal operations must outlive a cancelled turn context. In particular,
	// a provider/tool error may carry context cancellation while the task host is
	// still reachable and must be reconciled before the journal is acknowledged.
	terminalCtx := context.WithoutCancel(ctx)
	var terminalErr error
	switch {
	case errors.Is(runErr, turncancel.ErrTurnCancelled):
		slog.InfoContext(ctx, "task lifecycle: cancelled by user", slog.String("task", taskID))
		// The transport/gateway owns the out-of-band CANCEL transition. The turn
		// runner records this as interrupted directly rather than leaving a task
		// terminal intent for this decorator to acknowledge.
		return message, nil
	case runErr != nil:
		slog.ErrorContext(ctx, "task lifecycle: turn failed", slog.String("task", taskID), slog.Any("err", runErr))
		terminalErr = e.tasks.FailTask(terminalCtx, taskID, runErr.Error())
	default:
		slog.InfoContext(ctx, "task lifecycle: turn completed", slog.String("task", taskID))
		terminalErr = e.tasks.CompleteTask(terminalCtx, taskID)
	}
	if terminalErr == nil {
		if acknowledger, ok := e.inner.(TerminalAcknowledger); ok {
			terminalErr = acknowledger.AcknowledgeTaskTerminal(terminalCtx, addr.WorkspaceID, taskID)
		}
	}
	if terminalErr != nil {
		terminalErr = fmt.Errorf("task lifecycle: reconcile terminal state for %q: %w", taskID, terminalErr)
		slog.ErrorContext(ctx, "task lifecycle: terminal reconciliation failed", slog.String("task", taskID), slog.Any("err", terminalErr))
		return message, errors.Join(runErr, terminalErr)
	}
	return message, runErr
}

var _ runtime.TurnExecutor = Executor{}
