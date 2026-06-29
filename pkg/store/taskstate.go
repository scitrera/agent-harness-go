package store

import (
	"path/filepath"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func NewTaskStateStore(stateDir string) *taskstate.FileStore {
	return taskstate.NewFileStore(filepath.Join(stateDir, "tasks", "state.json"))
}
