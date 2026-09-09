// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package taskstate

import (
	"context"
	"time"
)

type ApprovalState string

const (
	ApprovalPending  ApprovalState = "pending"
	ApprovalApproved ApprovalState = "approved"
	ApprovalRejected ApprovalState = "rejected"
)

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusCancelled  TaskStatus = "cancelled"
)

type PlanRef struct {
	ID          string         `json:"id"`
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Approval    ApprovalRecord `json:"approval"`
}

type ApprovalRecord struct {
	State     ApprovalState `json:"state"`
	Actor     string        `json:"actor,omitempty"`
	Reason    string        `json:"reason,omitempty"`
	DecidedAt *time.Time    `json:"decided_at,omitempty"`
}

type ApprovalDecision struct {
	State     ApprovalState `json:"state"`
	Actor     string        `json:"actor,omitempty"`
	Reason    string        `json:"reason,omitempty"`
	DecidedAt time.Time     `json:"decided_at,omitempty"`
}

type TaskNode struct {
	ID          string         `json:"id"`
	PlanID      string         `json:"plan_id"`
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	Status      TaskStatus     `json:"status"`
	BlockedBy   []string       `json:"blocked_by,omitempty"`
	Owner       string         `json:"owner,omitempty"`
	AgentHint   string         `json:"agent_hint,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	ClaimedAt   *time.Time     `json:"claimed_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	Approval    ApprovalRecord `json:"approval"`
	Evidence    []EvidenceLink `json:"evidence,omitempty"`
}

type EvidenceLink struct {
	Label string `json:"label"`
	Path  string `json:"path"`
	URL   string `json:"url,omitempty"`
}

type TaskEvent struct {
	TaskID    string     `json:"task_id"`
	PlanID    string     `json:"plan_id"`
	Type      string     `json:"type"`
	Actor     string     `json:"actor,omitempty"`
	Status    TaskStatus `json:"status,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type ClaimRequest struct {
	PlanID     string
	TaskID     string
	Owner      string
	StaleAfter time.Duration
}

type TaskUpdate struct {
	ID          string
	Title       string
	Description string
	Status      TaskStatus
	BlockedBy   []string
	Owner       string
	AgentHint   string
	Evidence    []EvidenceLink
}

type TaskOwnerGuard struct {
	PlanID string
	Owner  string
	Status TaskStatus
}

type GuardedTaskUpdate struct {
	Guard  TaskOwnerGuard
	Update TaskUpdate
}

type Store interface {
	ListPlans(ctx context.Context) ([]PlanRef, error)
	CreatePlan(ctx context.Context, plan PlanRef) (PlanRef, error)
	GetPlan(ctx context.Context, planID string) (PlanRef, error)
	DecidePlan(ctx context.Context, planID string, decision ApprovalDecision) (PlanRef, error)
	ListTasks(ctx context.Context, planID string) ([]TaskNode, error)
	CreateTask(ctx context.Context, task TaskNode) (TaskNode, error)
	UpdateTask(ctx context.Context, update TaskUpdate) (TaskNode, error)
	UpdateTaskIfOwned(ctx context.Context, update GuardedTaskUpdate) (TaskNode, error)
	DecideTask(ctx context.Context, taskID string, decision ApprovalDecision) (TaskNode, error)
	ClaimTask(ctx context.Context, claim ClaimRequest) (TaskNode, error)
	ClaimNext(ctx context.Context, claim ClaimRequest) (TaskNode, error)
	ListEvents(ctx context.Context, planID string) ([]TaskEvent, error)
}
