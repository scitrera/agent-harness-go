// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/goal"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/tasklifecycle"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

type aetherTurnRecoveryHost interface {
	Topic() string
	LookupTaskInfo(ctx context.Context, taskID string) (*sdk.TaskInfo, bool, error)
	PrepareTurnRecovery(ctx context.Context, info *sdk.TaskInfo) (func(), error)
	CompleteTask(ctx context.Context, taskID string) error
	FailTask(ctx context.Context, taskID, reason string) error
}

type recoverableTurnRunner interface {
	ActiveTurnExecutions(ctx context.Context) ([]turnjournal.Record, error)
	ResumeTurn(ctx context.Context, workspaceID, taskID string) (protocol.ChatMessage, error)
	InterruptTurn(ctx context.Context, workspaceID, taskID, reason string) error
	AcknowledgeTaskTerminal(ctx context.Context, workspaceID, taskID string) error
}

func withAetherTaskLifecycle(ch *aetherchan.Channel, runner runtime.TurnExecutor) runtime.TurnExecutor {
	return tasklifecycle.WrapWhen(runner, ch, func(_ protocol.MessageAddress, message protocol.ChatMessage) bool {
		return goal.IsContinuationMessage(message) || aetherchan.IsScheduledTurnMessage(message)
	})
}

// startAetherTurnRecovery reconciles durable turn-journal records after the
// connection is live. Queued goal tasks have no turn record yet and are handled
// by continuation task reconciliation before this scan; RUNNING records are
// resumed only at the journal's safe boundary, otherwise interrupted and
// failed closed.
func startAetherTurnRecovery(ctx context.Context, host aetherTurnRecoveryHost, runner recoverableTurnRunner) error {
	records, err := runner.ActiveTurnExecutions(ctx)
	if err != nil {
		return fmt.Errorf("turn recovery scan: %w", err)
	}
	for _, record := range records {
		record := record
		go func() {
			if err := recoverAetherTurn(ctx, host, runner, record); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "aether turn recovery failed",
					slog.String("workspace", record.WorkspaceID), slog.String("thread", record.SessionID),
					slog.String("task", record.TaskID), slog.Any("err", err))
			}
		}()
	}
	if len(records) > 0 {
		slog.InfoContext(ctx, "aether turn recovery started", slog.Int("executions", len(records)), slog.String("owner", host.Topic()))
	}
	return nil
}

func recoverAetherTurn(ctx context.Context, host aetherTurnRecoveryHost, runner recoverableTurnRunner, record turnjournal.Record) error {
	info, found, err := host.LookupTaskInfo(ctx, record.TaskID)
	if err != nil {
		return fmt.Errorf("query parent task: %w", err)
	}
	if !found {
		reason := "no authoritative Aether task exists for active turn journal"
		return errors.Join(errors.New(reason), runner.InterruptTurn(ctx, record.WorkspaceID, record.TaskID, reason))
	}
	if info.TaskID != record.TaskID || strings.TrimSpace(info.AssignedTo) != host.Topic() {
		return errors.New("authoritative parent task is not assigned to the journal owner")
	}
	if logical := strings.TrimSpace(info.Metadata["scitrera.logical_workspace"]); logical != "" && logical != record.WorkspaceID {
		return fmt.Errorf("parent task logical workspace %q does not match journal %q", logical, record.WorkspaceID)
	}
	authority, err := aetherchan.TaskInfoAuthority(info)
	if err != nil {
		return err
	}
	runnerCtx := tools.WithMemoryAuthority(ctx, authority)
	if record.PendingTerminal() {
		return reconcilePendingAetherTurn(runnerCtx, host, runner, record, info)
	}
	if info.Status != pb.TaskStatus_TASK_STATUS_QUEUED.String() && info.Status != pb.TaskStatus_TASK_STATUS_RUNNING.String() {
		reason := fmt.Sprintf("authoritative parent task is not active (status %q)", info.Status)
		return errors.Join(errors.New(reason), runner.InterruptTurn(runnerCtx, record.WorkspaceID, record.TaskID, reason))
	}
	cleanup, prepareErr := host.PrepareTurnRecovery(runnerCtx, info)
	if prepareErr != nil {
		if failErr := host.FailTask(context.WithoutCancel(ctx), record.TaskID, prepareErr.Error()); failErr != nil {
			return errors.Join(prepareErr, fmt.Errorf("fail unrecoverable parent task: %w", failErr))
		}
		if interruptErr := runner.InterruptTurn(context.WithoutCancel(runnerCtx), record.WorkspaceID, record.TaskID, prepareErr.Error()); interruptErr != nil {
			return errors.Join(prepareErr, fmt.Errorf("close unrecoverable turn: %w", interruptErr))
		}
		return prepareErr
	}
	defer cleanup()
	assistant, resumeErr := runner.ResumeTurn(tasklifecycle.WithManagedTask(runnerCtx), record.WorkspaceID, record.TaskID)
	if resumeErr != nil {
		if failErr := host.FailTask(context.WithoutCancel(ctx), record.TaskID, resumeErr.Error()); failErr != nil {
			return errors.Join(resumeErr, fmt.Errorf("fail recovered parent task: %w", failErr))
		}
		if interruptErr := runner.InterruptTurn(context.WithoutCancel(runnerCtx), record.WorkspaceID, record.TaskID, resumeErr.Error()); interruptErr != nil {
			return errors.Join(resumeErr, fmt.Errorf("close recovered turn interruption: %w", interruptErr))
		}
		return resumeErr
	}
	if err := host.CompleteTask(context.WithoutCancel(ctx), record.TaskID); err != nil {
		return fmt.Errorf("complete recovered parent task: %w", err)
	}
	if err := runner.AcknowledgeTaskTerminal(context.WithoutCancel(runnerCtx), record.WorkspaceID, record.TaskID); err != nil {
		return fmt.Errorf("acknowledge recovered parent completion: %w", err)
	}
	slog.InfoContext(ctx, "aether turn recovery completed",
		slog.String("workspace", record.WorkspaceID), slog.String("thread", record.SessionID),
		slog.String("task", record.TaskID), slog.String("assistant", assistant.ID))
	return nil
}

func reconcilePendingAetherTurn(
	ctx context.Context,
	host aetherTurnRecoveryHost,
	runner recoverableTurnRunner,
	record turnjournal.Record,
	info *sdk.TaskInfo,
) error {
	desired := pb.TaskStatus_TASK_STATUS_FAILED.String()
	operation := func(ctx context.Context) error {
		return host.FailTask(ctx, record.TaskID, record.FailureReason)
	}
	if record.Phase == turnjournal.PhaseCompleting {
		desired = pb.TaskStatus_TASK_STATUS_COMPLETED.String()
		operation = func(ctx context.Context) error { return host.CompleteTask(ctx, record.TaskID) }
	}
	if info.Status != desired {
		if info.Status != pb.TaskStatus_TASK_STATUS_QUEUED.String() && info.Status != pb.TaskStatus_TASK_STATUS_RUNNING.String() {
			reason := fmt.Sprintf("pending journal phase %q conflicts with authoritative task status %q", record.Phase, info.Status)
			return errors.Join(errors.New(reason), runner.InterruptTurn(context.WithoutCancel(ctx), record.WorkspaceID, record.TaskID, reason))
		}
		if err := operation(context.WithoutCancel(ctx)); err != nil {
			return fmt.Errorf("reconcile pending journal phase %q: %w", record.Phase, err)
		}
	}
	if err := runner.AcknowledgeTaskTerminal(context.WithoutCancel(ctx), record.WorkspaceID, record.TaskID); err != nil {
		return fmt.Errorf("acknowledge pending journal phase %q: %w", record.Phase, err)
	}
	slog.InfoContext(ctx, "aether turn terminal intent reconciled",
		slog.String("workspace", record.WorkspaceID), slog.String("thread", record.SessionID),
		slog.String("task", record.TaskID), slog.String("task_status", desired))
	return nil
}
