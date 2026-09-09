// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/steering"
)

// deliverSteering appends any messages the user sent while this turn was running
// to the turn's history, so the next provider call sees them. It reports whether
// a delivered message carried an image, which the caller uses to escalate to a
// vision-capable model — a user who interjects with a screenshot expects it to
// be looked at, and a text-only model would reject the request outright.
//
// Delivery is also the point at which steering becomes ordinary history: the
// messages are persisted like any user input, so a resumed or compacted session
// still shows what the user asked for and when.
func (r *Runner) deliverSteering(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, required modelpkg.Capabilities) (bool, error) {
	if r.steering == nil {
		return false, nil
	}
	parked := r.steering.Drain(steering.Key(addr.WorkspaceID, addr.ThreadID))
	if len(parked) == 0 {
		return false, nil
	}
	sawImage := false
	for _, message := range parked {
		delivered, err := steering.Deliver(addr, message)
		if err != nil {
			// The user's words are already lost from the inbox by the drain, so a
			// render failure must not also be silent.
			return false, fmt.Errorf("render steering message %q: %w", message.ID, err)
		}
		if err := session.Append(ctx, delivered); err != nil {
			return false, fmt.Errorf("append steering message %q: %w", message.ID, err)
		}
		if !required.Vision && containsImage(delivered.Content) {
			sawImage = true
		}
		slog.InfoContext(ctx, "turn: delivered user steering mid-turn",
			slog.String("thread", addr.ThreadID),
			slog.String("message", message.ID),
		)
	}
	return sawImage, nil
}
