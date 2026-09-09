// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package harness

import "errors"

var (
	ErrMissingThreadID     = errors.New("harness: thread id required")
	ErrMissingHistoryStore = errors.New("harness: history store required")
)
