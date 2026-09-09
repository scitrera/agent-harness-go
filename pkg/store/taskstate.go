// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package store

import (
	"path/filepath"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func NewTaskStateStore(stateDir string) *taskstate.FileStore {
	return taskstate.NewFileStore(filepath.Join(stateDir, "tasks", "state.json"))
}
