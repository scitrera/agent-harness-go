package team

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func seededTaskStore(t *testing.T) *taskstate.FileStore {
	t.Helper()
	ctx := context.Background()
	store := taskstate.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	if _, err := store.CreatePlan(ctx, taskstate.PlanRef{ID: "plan-1", Title: "Team plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	for _, task := range []taskstate.TaskNode{
		{ID: "ready-a", PlanID: "plan-1", Title: "Ready A"},
		{ID: "ready-b", PlanID: "plan-1", Title: "Ready B"},
		{ID: "blocked", PlanID: "plan-1", Title: "Blocked", BlockedBy: []string{"ready-a"}},
	} {
		if _, err := store.CreateTask(ctx, task); err != nil {
			t.Fatalf("create task %s: %v", task.ID, err)
		}
	}
	return store
}

func tasksByID(tasks []taskstate.TaskNode) map[string]taskstate.TaskNode {
	byID := make(map[string]taskstate.TaskNode, len(tasks))
	for _, task := range tasks {
		byID[task.ID] = task
	}
	return byID
}

func writeTestFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func sameAgentIDs(got []AgentNode, want []AgentID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].ID != want[i] {
			return false
		}
	}
	return true
}
