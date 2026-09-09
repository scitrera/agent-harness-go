// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

type externalAdmissionCheckpoint func(context.Context, string, subagent.ExecutionEnvelope) error

type externalAdmissionCheckpointKey struct{}

func withExternalAdmissionCheckpoint(ctx context.Context, checkpoint externalAdmissionCheckpoint) context.Context {
	if checkpoint == nil {
		return ctx
	}
	return context.WithValue(ctx, externalAdmissionCheckpointKey{}, checkpoint)
}

func checkpointExternalAdmission(ctx context.Context, taskID string, envelope subagent.ExecutionEnvelope) error {
	checkpoint, _ := ctx.Value(externalAdmissionCheckpointKey{}).(externalAdmissionCheckpoint)
	if checkpoint == nil {
		return nil
	}
	return checkpoint(ctx, taskID, envelope)
}
