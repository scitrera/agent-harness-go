package team

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func TestCoordinatorClaimNext_assignsDifferentUnblockedTasks_whenAgentsRace(t *testing.T) {
	// Given: two local agents, two unblocked tasks, and one blocked task.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	graph := NewFileGraphStore(filepath.Join(t.TempDir(), "team.json"))
	coord := NewCoordinator(tasks, graph, CoordinatorOptions{LeaderID: "leader"})
	for _, agentID := range []AgentID{"leader", "agent-a", "agent-b"} {
		if err := graph.RegisterAgent(ctx, AgentNode{ID: agentID, Type: subagent.AgentType("local")}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	// When: two agents concurrently claim from the same shared task list.
	start := make(chan struct{})
	var wg sync.WaitGroup
	claims := make(chan taskstate.TaskNode, 2)
	errs := make(chan error, 2)
	for _, agentID := range []AgentID{"agent-a", "agent-b"} {
		wg.Add(1)
		go func(id AgentID) {
			defer wg.Done()
			<-start
			claimed, err := coord.ClaimNext(ctx, ClaimNextRequest{
				PlanID:     "plan-1",
				AgentID:    id,
				StaleAfter: time.Hour,
			})
			if err != nil {
				errs <- err
				return
			}
			claims <- claimed
		}(agentID)
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errs)

	// Then: each agent owns a different unblocked task and the blocked task is untouched.
	for err := range errs {
		t.Fatalf("claim failed: %v", err)
	}
	gotIDs := make([]string, 0, 2)
	for claim := range claims {
		gotIDs = append(gotIDs, claim.ID)
	}
	sort.Strings(gotIDs)
	if len(gotIDs) != 2 || gotIDs[0] != "ready-a" || gotIDs[1] != "ready-b" {
		t.Fatalf("unexpected claims: %#v", gotIDs)
	}
	all, err := tasks.ListTasks(ctx, "plan-1")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	byID := tasksByID(all)
	if byID["blocked"].Owner != "" || byID["blocked"].Status != taskstate.TaskStatusPending {
		t.Fatalf("blocked task changed: %#v", byID["blocked"])
	}
}

func TestCoordinatorClaimTask_rejectsBlockedTask(t *testing.T) {
	// Given: a task blocked by an incomplete dependency.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	coord := NewCoordinator(tasks, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{})

	// When: an agent explicitly tries to claim the blocked task.
	_, err := coord.ClaimTask(ctx, ClaimTaskRequest{
		PlanID:     "plan-1",
		TaskID:     "blocked",
		AgentID:    "agent-a",
		StaleAfter: time.Hour,
	})

	// Then: the claim is rejected with the taskstate typed error.
	if !errors.Is(err, taskstate.ErrTaskBlocked) {
		t.Fatalf("blocked task should be unclaimable, got %v", err)
	}
}

func TestCoordinatorCancelTask_marksCancelledAndClearsOwner(t *testing.T) {
	// Given: a claimed task owned by agent-a.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	coord := NewCoordinator(tasks, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{})
	claimed, err := coord.ClaimTask(ctx, ClaimTaskRequest{
		PlanID:     "plan-1",
		TaskID:     "ready-a",
		AgentID:    "agent-a",
		StaleAfter: time.Hour,
	})
	if err != nil {
		t.Fatalf("claim ready-a: %v", err)
	}
	if claimed.Owner != "agent-a" {
		t.Fatalf("unexpected owner: %#v", claimed)
	}

	// When: the owner cancels the task.
	cancelled, err := coord.CancelTask(ctx, TaskOwnerRequest{
		PlanID:  "plan-1",
		TaskID:  "ready-a",
		AgentID: "agent-a",
	})
	if err != nil {
		t.Fatalf("cancel task: %v", err)
	}

	// Then: cancellation is durable and releases ownership deterministically.
	if cancelled.Status != taskstate.TaskStatusCancelled || cancelled.Owner != "" || cancelled.ClaimedAt != nil {
		t.Fatalf("unexpected cancelled task: %#v", cancelled)
	}
}

func TestCoordinatorReleaseTask_returnsClaimedTaskToPending(t *testing.T) {
	// Given: a task claimed by agent-a.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	coord := NewCoordinator(tasks, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{})
	if _, err := coord.ClaimTask(ctx, ClaimTaskRequest{
		PlanID:     "plan-1",
		TaskID:     "ready-a",
		AgentID:    "agent-a",
		StaleAfter: time.Hour,
	}); err != nil {
		t.Fatalf("claim ready-a: %v", err)
	}

	// When: the owner releases the task.
	released, err := coord.ReleaseTask(ctx, TaskOwnerRequest{
		PlanID:  "plan-1",
		TaskID:  "ready-a",
		AgentID: "agent-a",
	})
	if err != nil {
		t.Fatalf("release task: %v", err)
	}

	// Then: the task can be claimed by another agent.
	if released.Status != taskstate.TaskStatusPending || released.Owner != "" {
		t.Fatalf("unexpected released task: %#v", released)
	}
	reclaimed, err := coord.ClaimTask(ctx, ClaimTaskRequest{
		PlanID:     "plan-1",
		TaskID:     "ready-a",
		AgentID:    "agent-b",
		StaleAfter: time.Hour,
	})
	if err != nil {
		t.Fatalf("reclaim task: %v", err)
	}
	if reclaimed.Owner != "agent-b" {
		t.Fatalf("unexpected reclaimed task: %#v", reclaimed)
	}
}

func TestCoordinatorApprovePlanExit_requiresLeader(t *testing.T) {
	// Given: a pending plan with a configured leader.
	ctx := context.Background()
	tasks := seededTaskStore(t)
	coord := NewCoordinator(tasks, NewFileGraphStore(filepath.Join(t.TempDir(), "team.json")), CoordinatorOptions{LeaderID: "leader"})

	// When: a non-leader tries to approve plan exit.
	_, err := coord.ApprovePlanExit(ctx, PlanExitApproval{
		PlanID:  "plan-1",
		ActorID: "agent-a",
		Approve: true,
		Reason:  "done",
	})

	// Then: approval is rejected until the leader records the decision.
	if !errors.Is(err, ErrLeaderApprovalRequired) {
		t.Fatalf("expected leader approval error, got %v", err)
	}
	approved, err := coord.ApprovePlanExit(ctx, PlanExitApproval{
		PlanID:  "plan-1",
		ActorID: "leader",
		Approve: true,
		Reason:  "done",
	})
	if err != nil {
		t.Fatalf("leader approve: %v", err)
	}
	if approved.Approval.State != taskstate.ApprovalApproved || approved.Approval.Actor != "leader" {
		t.Fatalf("unexpected approval: %#v", approved.Approval)
	}
}
