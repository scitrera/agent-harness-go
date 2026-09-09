// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package team

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func TestCoordinatorClaimTask_rejectsEmptyAgentID(t *testing.T) {
	// Given: a shared task list with available work.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	coord := NewCoordinator(tasks, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{})

	// When: a caller attempts to claim a specific task without an agent identity.
	_, err := coord.ClaimTask(ctx, ClaimTaskRequest{
		PlanID:     "plan-1",
		TaskID:     "ready-a",
		AgentID:    "",
		StaleAfter: time.Hour,
	})

	// Then: the claim is rejected and does not create an anonymous owner.
	if !errors.Is(err, ErrInvalidAgent) {
		t.Fatalf("empty agent claim should fail, got %v", err)
	}
	all, err := tasks.ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	got := tasksByID(all)["ready-a"]
	if got.Status != taskstate.TaskStatusPending || got.Owner != "" || got.ClaimedAt != nil {
		t.Fatalf("empty agent claim mutated task: %#v", got)
	}
}

func TestCoordinatorClaimNext_rejectsEmptyAgentID(t *testing.T) {
	// Given: a shared task list with available work.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	coord := NewCoordinator(tasks, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{})

	// When: a caller attempts to acquire the next task without an agent identity.
	_, err := coord.ClaimNext(ctx, ClaimNextRequest{
		PlanID:     "plan-1",
		AgentID:    "",
		StaleAfter: time.Hour,
	})

	// Then: the acquisition is rejected and no task is assigned anonymously.
	if !errors.Is(err, ErrInvalidAgent) {
		t.Fatalf("empty agent claim next should fail, got %v", err)
	}
	all, err := tasks.ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	for _, task := range all {
		if task.Owner != "" || task.Status == taskstate.TaskStatusInProgress {
			t.Fatalf("empty agent claim next mutated task: %#v", task)
		}
	}
}

func TestCoordinatorOwnedTaskMutation_rejectsStaleOwner_whenTaskReclaimed(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *Coordinator) (taskstate.TaskNode, error)
	}{
		{
			name: "release",
			run: func(ctx context.Context, coord *Coordinator) (taskstate.TaskNode, error) {
				return coord.ReleaseTask(ctx, TaskOwnerRequest{PlanID: "plan-1", TaskID: "ready-a", AgentID: "agent-a"})
			},
		},
		{
			name: "cancel",
			run: func(ctx context.Context, coord *Coordinator) (taskstate.TaskNode, error) {
				return coord.CancelTask(ctx, TaskOwnerRequest{PlanID: "plan-1", TaskID: "ready-a", AgentID: "agent-a"})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given: agent-a owns a task, but its claim can become stale before mutation.
			ctx := context.Background()
			tasks := seededTaskStore(t)
			now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
			tasks.SetClock(func() time.Time { return now })
			proxy := &reclaimBeforeUpdateStore{Store: tasks}
			coord := NewCoordinator(proxy, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{})
			if _, err := coord.ClaimTask(ctx, ClaimTaskRequest{
				PlanID:     "plan-1",
				TaskID:     "ready-a",
				AgentID:    "agent-a",
				StaleAfter: time.Hour,
			}); err != nil {
				t.Fatalf("claim ready-a: %v", err)
			}
			proxy.beforeUpdate = func() {
				now = now.Add(2 * time.Hour)
				if _, err := tasks.ClaimTask(ctx, taskstate.ClaimRequest{
					PlanID:     "plan-1",
					TaskID:     "ready-a",
					Owner:      "agent-b",
					StaleAfter: time.Hour,
				}); err != nil {
					t.Fatalf("reclaim stale task: %v", err)
				}
			}

			// When: the stale owner tries to mutate after the newer claim lands.
			_, err := tc.run(ctx, coord)

			// Then: the stale mutation is rejected and the newer owner remains intact.
			if !errors.Is(err, ErrTaskOwnerMismatch) {
				t.Fatalf("stale owner mutation should fail, got %v", err)
			}
			all, err := tasks.ListTasks(ctx, "plan-1")
			if err != nil {
				t.Fatalf("list tasks: %v", err)
			}
			got := tasksByID(all)["ready-a"]
			if got.Status != taskstate.TaskStatusInProgress || got.Owner != "agent-b" || got.ClaimedAt == nil {
				t.Fatalf("stale owner mutation corrupted newer claim: %#v", got)
			}
		})
	}
}

type reclaimBeforeUpdateStore struct {
	taskstate.Store
	beforeUpdate func()
}

func (s *reclaimBeforeUpdateStore) UpdateTaskIfOwned(ctx context.Context, update taskstate.GuardedTaskUpdate) (taskstate.TaskNode, error) {
	if s.beforeUpdate != nil {
		beforeUpdate := s.beforeUpdate
		s.beforeUpdate = nil
		beforeUpdate()
	}
	return s.Store.UpdateTaskIfOwned(ctx, update)
}
