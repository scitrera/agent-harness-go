// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package taskstate

import (
	"sort"
	"time"
)

func applyUpdate(task TaskNode, update TaskUpdate, now time.Time) TaskNode {
	next := cloneTask(task)
	if update.Title != "" {
		next.Title = update.Title
	}
	if update.Description != "" {
		next.Description = update.Description
	}
	if update.Status != "" {
		next.Status = update.Status
	}
	if update.BlockedBy != nil {
		next.BlockedBy = append([]string(nil), update.BlockedBy...)
	}
	if update.Owner != "" {
		next.Owner = update.Owner
	}
	if update.AgentHint != "" {
		next.AgentHint = update.AgentHint
	}
	if update.Evidence != nil {
		next.Evidence = append([]EvidenceLink(nil), update.Evidence...)
	}
	if next.Status == TaskStatusCompleted && next.CompletedAt == nil {
		completedAt := now
		next.CompletedAt = &completedAt
	}
	if next.Status != TaskStatusInProgress {
		next.Owner = ""
		next.ClaimedAt = nil
	}
	next.UpdatedAt = now
	return next
}

func claimTask(state *fileState, task TaskNode, claim ClaimRequest, now time.Time) (TaskNode, error) {
	if err := validateClaimOwner(claim); err != nil {
		return TaskNode{}, err
	}
	if !claimable(state, task, claim, now) {
		if task.Owner != "" && !ownerStale(task, claim, now) {
			return TaskNode{}, ErrTaskOwned
		}
		return TaskNode{}, ErrTaskBlocked
	}
	claimedAt := now
	task.Status = TaskStatusInProgress
	task.Owner = claim.Owner
	task.ClaimedAt = &claimedAt
	task.UpdatedAt = now
	state.Tasks[task.ID] = cloneTask(task)
	state.Events = append(state.Events, newEvent(task, "task.claimed", claim.Owner, now))
	return cloneTask(task), nil
}

func validateClaimOwner(claim ClaimRequest) error {
	if claim.Owner == "" {
		return ErrInvalidOwner
	}
	return nil
}

func validateTaskOwnerGuard(task TaskNode, guard TaskOwnerGuard) error {
	if guard.Owner == "" {
		return ErrInvalidOwner
	}
	if guard.PlanID != "" && task.PlanID != guard.PlanID {
		return ErrTaskNotFound
	}
	if task.Owner != guard.Owner {
		return ErrTaskOwnerMismatch
	}
	if guard.Status != "" && task.Status != guard.Status {
		return ErrInvalidTransition
	}
	return nil
}

func claimable(state *fileState, task TaskNode, claim ClaimRequest, now time.Time) bool {
	if task.Status == TaskStatusCompleted || task.Status == TaskStatusCancelled {
		return false
	}
	if task.Status == TaskStatusInProgress && task.Owner != "" && !ownerStale(task, claim, now) {
		return false
	}
	return dependenciesComplete(state, task)
}

func ownerStale(task TaskNode, claim ClaimRequest, now time.Time) bool {
	if claim.StaleAfter <= 0 || task.ClaimedAt == nil {
		return false
	}
	return now.Sub(*task.ClaimedAt) > claim.StaleAfter
}

func defaultApproval(approval ApprovalRecord) ApprovalRecord {
	if approval.State == "" {
		approval.State = ApprovalPending
	}
	return approval
}

func approvalFromDecision(decision ApprovalDecision, now time.Time) (ApprovalRecord, error) {
	if !validApprovalState(decision.State) {
		return ApprovalRecord{}, ErrInvalidApproval
	}
	decidedAt := defaultTime(decision.DecidedAt, now)
	return ApprovalRecord{
		State:     decision.State,
		Actor:     decision.Actor,
		Reason:    decision.Reason,
		DecidedAt: &decidedAt,
	}, nil
}

func newEvent(task TaskNode, eventType, actor string, now time.Time) TaskEvent {
	return TaskEvent{
		TaskID:    task.ID,
		PlanID:    task.PlanID,
		Type:      eventType,
		Actor:     actor,
		Status:    task.Status,
		CreatedAt: now,
	}
}

func cloneTask(task TaskNode) TaskNode {
	task.BlockedBy = append([]string(nil), task.BlockedBy...)
	task.Evidence = append([]EvidenceLink(nil), task.Evidence...)
	if task.Approval.DecidedAt != nil {
		decidedAt := *task.Approval.DecidedAt
		task.Approval.DecidedAt = &decidedAt
	}
	return task
}

func sortTasks(tasks []TaskNode) {
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].ID < tasks[j].ID
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
	})
}

func defaultTime(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}
