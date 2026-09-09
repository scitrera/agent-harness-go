// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package taskstate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFileStoreLifecycle_whenDependencyCompletes(t *testing.T) {
	// Given: a file-backed store with an approved plan and a dependent task DAG.
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	clock := fixedClock(time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC))
	store.SetClock(clock.Now)

	plan, err := store.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Ship task state"})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := store.DecidePlan(ctx, plan.ID, ApprovalDecision{
		State:     ApprovalApproved,
		Actor:     "user",
		Reason:    "ready",
		DecidedAt: clock.Now(),
	}); err != nil {
		t.Fatalf("approve plan: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "setup", PlanID: plan.ID, Title: "Set up"}); err != nil {
		t.Fatalf("create setup task: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "finish", PlanID: plan.ID, Title: "Finish", BlockedBy: []string{"setup"}}); err != nil {
		t.Fatalf("create finish task: %v", err)
	}

	// When: the available task is claimed and completed.
	claimed, err := store.ClaimNext(ctx, ClaimRequest{PlanID: plan.ID, Owner: "agent-a", StaleAfter: time.Hour})
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if claimed.ID != "setup" || claimed.Status != TaskStatusInProgress {
		t.Fatalf("unexpected claim: %#v", claimed)
	}
	done, err := store.UpdateTask(ctx, TaskUpdate{
		ID:       "setup",
		Status:   TaskStatusCompleted,
		Evidence: []EvidenceLink{{Label: "unit", Path: ".omo/evidence/unit.txt"}},
	})
	if err != nil {
		t.Fatalf("complete setup: %v", err)
	}

	// Then: the dependent task becomes claimable and the evidence is durable state.
	if done.CompletedAt == nil || len(done.Evidence) != 1 {
		t.Fatalf("completion metadata missing: %#v", done)
	}
	next, err := store.ClaimNext(ctx, ClaimRequest{PlanID: plan.ID, Owner: "agent-b", StaleAfter: time.Hour})
	if err != nil {
		t.Fatalf("claim dependent: %v", err)
	}
	if next.ID != "finish" {
		t.Fatalf("expected finish task, got %#v", next)
	}
}

func TestFileStoreClaimNext_whenTaskBlockedOrOwned(t *testing.T) {
	// Given: one blocked task and one task owned by a fresh claimant.
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now })
	if _, err := store.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Blocked plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "blocked", PlanID: "plan-1", Title: "Blocked", BlockedBy: []string{"missing"}}); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("missing dependency should fail, got %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "root", PlanID: "plan-1", Title: "Root"}); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "child", PlanID: "plan-1", Title: "Child", BlockedBy: []string{"root"}}); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := store.ClaimTask(ctx, ClaimRequest{TaskID: "root", Owner: "agent-a", StaleAfter: time.Hour}); err != nil {
		t.Fatalf("claim root: %v", err)
	}

	// When: another owner tries blocked and owned claims.
	_, blockedErr := store.ClaimTask(ctx, ClaimRequest{TaskID: "child", Owner: "agent-b", StaleAfter: time.Hour})
	_, ownedErr := store.ClaimTask(ctx, ClaimRequest{TaskID: "root", Owner: "agent-b", StaleAfter: time.Hour})

	// Then: typed failures are returned without changing task ownership.
	if !errors.Is(blockedErr, ErrTaskBlocked) {
		t.Fatalf("blocked claim should fail, got %v", blockedErr)
	}
	if !errors.Is(ownedErr, ErrTaskOwned) {
		t.Fatalf("owned claim should fail, got %v", ownedErr)
	}
	tasks, err := store.ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	byID := map[string]TaskNode{}
	for _, task := range tasks {
		byID[task.ID] = task
	}
	if byID["root"].Owner != "agent-a" || byID["child"].Owner != "" {
		t.Fatalf("claims corrupted tasks: %#v", tasks)
	}
}

func TestFileStoreValidation_whenStatusInvalid(t *testing.T) {
	// Given: a plan that can accept tasks.
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	if _, err := store.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Status plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}

	// When: a task is created with an unknown lifecycle status.
	_, err := store.CreateTask(ctx, TaskNode{ID: "bad", PlanID: "plan-1", Title: "Bad", Status: TaskStatus("unknown")})

	// Then: the status transition validator rejects it.
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("invalid status should fail, got %v", err)
	}
}

func TestFileStoreValidation_whenCycleExists(t *testing.T) {
	// Given: a plan with two valid tasks.
	ctx := context.Background()
	store := NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	if _, err := store.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Cycle plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "a", PlanID: "plan-1", Title: "A"}); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "b", PlanID: "plan-1", Title: "B", BlockedBy: []string{"a"}}); err != nil {
		t.Fatalf("create b: %v", err)
	}

	// When: an update would create a cycle.
	_, err := store.UpdateTask(ctx, TaskUpdate{ID: "a", BlockedBy: []string{"b"}})

	// Then: validation rejects the mutation.
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("cycle should fail, got %v", err)
	}
}

func TestFileStoreReload_whenStateWasSaved(t *testing.T) {
	// Given: a store with persisted plan approval and task lifecycle state.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasks.json")
	store := NewFileStore(path)
	if _, err := store.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Reload plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if _, err := store.DecidePlan(ctx, "plan-1", ApprovalDecision{State: ApprovalRejected, Actor: "reviewer", Reason: "needs edits"}); err != nil {
		t.Fatalf("reject plan: %v", err)
	}
	if _, err := store.CreateTask(ctx, TaskNode{ID: "task-1", PlanID: "plan-1", Title: "Persist me"}); err != nil {
		t.Fatalf("create task: %v", err)
	}

	// When: a new store instance loads the same file.
	reloaded := NewFileStore(path)
	plan, err := reloaded.GetPlan(ctx, "plan-1")
	if err != nil {
		t.Fatalf("get plan: %v", err)
	}
	tasks, err := reloaded.ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}

	// Then: approval and task records survive process reload.
	if plan.Approval.State != ApprovalRejected || plan.Approval.Actor != "reviewer" {
		t.Fatalf("approval not reloaded: %#v", plan.Approval)
	}
	if len(tasks) != 1 || tasks[0].ID != "task-1" {
		t.Fatalf("tasks not reloaded: %#v", tasks)
	}
}

func TestFileStoreConcurrentInstances_whenSharingStateFile(t *testing.T) {
	// Given: two store instances pointed at the same local state file.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasks.json")
	first := NewFileStore(path)
	second := NewFileStore(path)
	if _, err := first.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Concurrent plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}

	// When: both instances concurrently persist independent task updates.
	const taskCount = 80
	var wg sync.WaitGroup
	errs := make(chan error, taskCount)
	for i := range taskCount {
		store := first
		if i%2 == 1 {
			store = second
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			taskID := fmt.Sprintf("task-%02d", index)
			_, err := store.CreateTask(ctx, TaskNode{ID: taskID, PlanID: "plan-1", Title: taskID})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	// Then: no temp-file collision or lost read-modify-write drops any task.
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent create failed: %v", err)
		}
	}
	tasks, err := NewFileStore(path).ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != taskCount {
		t.Fatalf("expected %d durable tasks, got %d: %#v", taskCount, len(tasks), tasks)
	}
}

func TestFileStoreTaskApproval_whenReloaded(t *testing.T) {
	// Given: a task created without an explicit approval decision.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasks.json")
	store := NewFileStore(path)
	if _, err := store.CreatePlan(ctx, PlanRef{ID: "plan-1", Title: "Task approval plan"}); err != nil {
		t.Fatalf("create plan: %v", err)
	}
	task, err := store.CreateTask(ctx, TaskNode{ID: "task-1", PlanID: "plan-1", Title: "Approve me"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if task.Approval.State != ApprovalPending {
		t.Fatalf("new task should start approval pending: %#v", task.Approval)
	}

	// When: the task approval decision is recorded and a new store reloads it.
	decidedAt := time.Date(2026, 6, 29, 15, 0, 0, 0, time.UTC)
	if _, err := store.DecideTask(ctx, "task-1", ApprovalDecision{
		State:     ApprovalApproved,
		Actor:     "lead",
		Reason:    "criterion satisfied",
		DecidedAt: decidedAt,
	}); err != nil {
		t.Fatalf("approve task: %v", err)
	}
	tasks, err := NewFileStore(path).ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}

	// Then: task-level approval state and record are durable.
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %#v", tasks)
	}
	approval := tasks[0].Approval
	if approval.State != ApprovalApproved || approval.Actor != "lead" || approval.DecidedAt == nil || !approval.DecidedAt.Equal(decidedAt) {
		t.Fatalf("task approval not reloaded: %#v", approval)
	}
}

func TestFileStoreLoad_whenFileCorrupt(t *testing.T) {
	// Given: a corrupt task state file.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasks.json")
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}

	// When: the store tries to load it.
	_, err := NewFileStore(path).ListPlans(ctx)

	// Then: callers receive a typed corrupt-state error.
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("corrupt file should fail, got %v", err)
	}
}

type fixedClock time.Time

func (c fixedClock) Now() time.Time {
	return time.Time(c)
}
