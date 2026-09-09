// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package team

import "errors"

var (
	ErrAgentCycle             = errors.New("team: agent graph cycle")
	ErrAgentExists            = errors.New("team: agent already exists")
	ErrAgentNotFound          = errors.New("team: agent not found")
	ErrConcurrencyLimit       = errors.New("team: concurrency limit reached")
	ErrCorruptState           = errors.New("team: graph state corrupt")
	ErrInvalidAgent           = errors.New("team: invalid agent")
	ErrLeaderApprovalRequired = errors.New("team: leader approval required")
	ErrTaskOwnerMismatch      = errors.New("team: task owner mismatch")
)
