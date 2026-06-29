package store

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func TestNewTaskStateStore_whenUsingStateDir(t *testing.T) {
	// Given: the OSS file-store state directory helper.
	ctx := context.Background()
	tasks := NewTaskStateStore(t.TempDir())

	// When: a plan is created through the task-state store.
	created, err := tasks.CreatePlan(ctx, taskstate.PlanRef{ID: "plan-1", Title: "Store helper"})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}

	// Then: the helper returns a durable file-backed backend.
	reloaded, err := tasks.GetPlan(ctx, created.ID)
	if err != nil {
		t.Fatalf("reload plan: %v", err)
	}
	if reloaded.ID != "plan-1" {
		t.Fatalf("unexpected plan: %#v", reloaded)
	}
}
