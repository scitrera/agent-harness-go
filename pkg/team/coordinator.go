package team

import (
	"context"
	"errors"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func NewCoordinator(tasks taskstate.Store, graph GraphStore, opts CoordinatorOptions) *Coordinator {
	return &Coordinator{
		tasks:   tasks,
		graph:   graph,
		leader:  opts.LeaderID,
		maxKids: opts.MaxActiveChildren,
	}
}

func (c *Coordinator) ClaimNext(ctx context.Context, req ClaimNextRequest) (taskstate.TaskNode, error) {
	if err := ctx.Err(); err != nil {
		return taskstate.TaskNode{}, err
	}
	if req.AgentID == "" {
		return taskstate.TaskNode{}, ErrInvalidAgent
	}
	claim := taskstate.ClaimRequest{
		PlanID:     req.PlanID,
		Owner:      string(req.AgentID),
		StaleAfter: req.StaleAfter,
	}
	task, err := c.tasks.ClaimNext(ctx, claim)
	if err != nil {
		return taskstate.TaskNode{}, fmt.Errorf("claim next task: %w", err)
	}
	return task, nil
}

func (c *Coordinator) ClaimTask(ctx context.Context, req ClaimTaskRequest) (taskstate.TaskNode, error) {
	if err := ctx.Err(); err != nil {
		return taskstate.TaskNode{}, err
	}
	if req.AgentID == "" {
		return taskstate.TaskNode{}, ErrInvalidAgent
	}
	claim := taskstate.ClaimRequest{
		PlanID:     req.PlanID,
		TaskID:     req.TaskID,
		Owner:      string(req.AgentID),
		StaleAfter: req.StaleAfter,
	}
	task, err := c.tasks.ClaimTask(ctx, claim)
	if err != nil {
		return taskstate.TaskNode{}, fmt.Errorf("claim task %s: %w", req.TaskID, err)
	}
	return task, nil
}

func (c *Coordinator) ReleaseTask(ctx context.Context, req TaskOwnerRequest) (taskstate.TaskNode, error) {
	if err := ctx.Err(); err != nil {
		return taskstate.TaskNode{}, err
	}
	if req.AgentID == "" {
		return taskstate.TaskNode{}, ErrInvalidAgent
	}
	released, err := c.tasks.UpdateTaskIfOwned(ctx, taskstate.GuardedTaskUpdate{
		Guard: taskstate.TaskOwnerGuard{
			PlanID: req.PlanID,
			Owner:  string(req.AgentID),
			Status: taskstate.TaskStatusInProgress,
		},
		Update: taskstate.TaskUpdate{
			ID:     req.TaskID,
			Status: taskstate.TaskStatusPending,
		},
	})
	if err != nil {
		return taskstate.TaskNode{}, wrapOwnedTaskUpdateError("release task", req.TaskID, err)
	}
	return released, nil
}

func (c *Coordinator) CancelTask(ctx context.Context, req TaskOwnerRequest) (taskstate.TaskNode, error) {
	if err := ctx.Err(); err != nil {
		return taskstate.TaskNode{}, err
	}
	if req.AgentID == "" {
		return taskstate.TaskNode{}, ErrInvalidAgent
	}
	cancelled, err := c.tasks.UpdateTaskIfOwned(ctx, taskstate.GuardedTaskUpdate{
		Guard: taskstate.TaskOwnerGuard{
			PlanID: req.PlanID,
			Owner:  string(req.AgentID),
			Status: taskstate.TaskStatusInProgress,
		},
		Update: taskstate.TaskUpdate{
			ID:     req.TaskID,
			Status: taskstate.TaskStatusCancelled,
		},
	})
	if err != nil {
		return taskstate.TaskNode{}, wrapOwnedTaskUpdateError("cancel task", req.TaskID, err)
	}
	return cancelled, nil
}

func (c *Coordinator) ApprovePlanExit(ctx context.Context, req PlanExitApproval) (taskstate.PlanRef, error) {
	if err := ctx.Err(); err != nil {
		return taskstate.PlanRef{}, err
	}
	if c.leader != "" && req.ActorID != c.leader {
		return taskstate.PlanRef{}, ErrLeaderApprovalRequired
	}
	state := taskstate.ApprovalRejected
	if req.Approve {
		state = taskstate.ApprovalApproved
	}
	plan, err := c.tasks.DecidePlan(ctx, req.PlanID, taskstate.ApprovalDecision{
		State:  state,
		Actor:  string(req.ActorID),
		Reason: req.Reason,
	})
	if err != nil {
		return taskstate.PlanRef{}, fmt.Errorf("approve plan exit %s: %w", req.PlanID, err)
	}
	return plan, nil
}

func (c *Coordinator) AddChild(ctx context.Context, parentID AgentID, child AgentNode) error {
	if c.graph == nil {
		return ErrAgentNotFound
	}
	if err := c.graph.AddChild(ctx, AddChildRequest{
		ParentID:          parentID,
		Child:             child,
		MaxActiveChildren: c.maxKids,
	}); err != nil {
		return fmt.Errorf("add child agent %s: %w", child.ID, err)
	}
	return nil
}

func (c *Coordinator) DescendantsBFS(ctx context.Context, parentID AgentID) ([]AgentNode, error) {
	if c.graph == nil {
		return nil, ErrAgentNotFound
	}
	nodes, err := c.graph.DescendantsBFS(ctx, parentID)
	if err != nil {
		return nil, fmt.Errorf("list descendants for %s: %w", parentID, err)
	}
	return nodes, nil
}

func wrapOwnedTaskUpdateError(op string, taskID string, err error) error {
	if errors.Is(err, taskstate.ErrTaskOwnerMismatch) {
		return fmt.Errorf("%s %s: %w", op, taskID, ErrTaskOwnerMismatch)
	}
	return fmt.Errorf("%s %s: %w", op, taskID, err)
}
