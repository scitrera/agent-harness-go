// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package runtime

import "errors"

var (
	ErrMissingTaskSource   = errors.New("runtime: task source required")
	ErrMissingTurnExecutor = errors.New("runtime: turn executor required")
)
