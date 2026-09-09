// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

func (m model) handleTasks(fields []string) (tea.Model, tea.Cmd) {
	if m.taskStore == nil {
		m.showDrawer(drawerTasks, "task state is not wired")
		return m, nil
	}
	if len(fields) < 2 || fields[1] == "plans" || fields[1] == "list" || fields[1] == "ls" {
		planID := ""
		if len(fields) > 2 && fields[1] != "plans" {
			planID = fields[2]
		}
		return m, taskSummaryCmd(m.ctx, m.taskStore, planID)
	}
	switch fields[1] {
	case "plan":
		return m.handleTaskPlan(fields)
	case "create":
		return m.createTask(fields)
	case "claim":
		return m.claimTask(fields)
	case "next":
		return m.claimNextTask(fields)
	case "approve", "reject":
		return m.decideTask(fields)
	case "complete", "cancel", "reopen":
		return m.updateTaskStatus(fields)
	}
	m.addSystem("unknown /tasks command")
	return m, nil
}

func (m model) handleTaskPlan(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 || fields[2] == "list" || fields[2] == "ls" {
		return m, taskSummaryCmd(m.ctx, m.taskStore, "")
	}
	switch fields[2] {
	case "create":
		if len(fields) < 5 {
			m.addSystem("usage: /tasks plan create <plan-id> <title>")
			return m, nil
		}
		plan := taskstate.PlanRef{ID: fields[3], Title: strings.Join(fields[4:], " ")}
		return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "plan created", Run: func(ctx context.Context) (string, error) {
			created, err := m.taskStore.CreatePlan(ctx, plan)
			if err != nil {
				return "", err
			}
			return taskSummary(ctx, m.taskStore, created.ID)
		}})
	case "approve", "reject":
		if len(fields) < 4 {
			m.addSystem("usage: /tasks plan approve <plan-id> [reason]")
			return m, nil
		}
		state := taskstate.ApprovalApproved
		if fields[2] == "reject" {
			state = taskstate.ApprovalRejected
		}
		reason := strings.Join(fields[4:], " ")
		planID := fields[3]
		return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "plan approval recorded", Run: func(ctx context.Context) (string, error) {
			if _, err := m.taskStore.DecidePlan(ctx, planID, taskstate.ApprovalDecision{State: state, Actor: "tui", Reason: reason}); err != nil {
				return "", err
			}
			return taskSummary(ctx, m.taskStore, planID)
		}})
	}
	m.addSystem("unknown /tasks plan command")
	return m, nil
}

func (m model) createTask(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 5 {
		m.addSystem("usage: /tasks create <plan-id> <task-id> <title>")
		return m, nil
	}
	task := taskstate.TaskNode{PlanID: fields[2], ID: fields[3], Title: strings.Join(fields[4:], " ")}
	return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "task created", Run: func(ctx context.Context) (string, error) {
		created, err := m.taskStore.CreateTask(ctx, task)
		if err != nil {
			return "", err
		}
		return taskSummary(ctx, m.taskStore, created.PlanID)
	}})
}

func (m model) claimTask(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 5 {
		m.addSystem("usage: /tasks claim <plan-id> <task-id> <agent-id>")
		return m, nil
	}
	claim := taskstate.ClaimRequest{PlanID: fields[2], TaskID: fields[3], Owner: fields[4], StaleAfter: time.Hour}
	return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "task claimed", Run: func(ctx context.Context) (string, error) {
		claimed, err := m.taskStore.ClaimTask(ctx, claim)
		if err != nil {
			return "", err
		}
		return taskSummary(ctx, m.taskStore, claimed.PlanID)
	}})
}

func (m model) claimNextTask(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 4 {
		m.addSystem("usage: /tasks next <plan-id> <agent-id>")
		return m, nil
	}
	claim := taskstate.ClaimRequest{PlanID: fields[2], Owner: fields[3], StaleAfter: time.Hour}
	return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "next task claimed", Run: func(ctx context.Context) (string, error) {
		claimed, err := m.taskStore.ClaimNext(ctx, claim)
		if err != nil {
			return "", err
		}
		return taskSummary(ctx, m.taskStore, claimed.PlanID)
	}})
}

func (m model) decideTask(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 {
		m.addSystem("usage: /tasks approve <task-id> [reason]")
		return m, nil
	}
	state := taskstate.ApprovalApproved
	if fields[1] == "reject" {
		state = taskstate.ApprovalRejected
	}
	taskID := fields[2]
	reason := strings.Join(fields[3:], " ")
	return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "task approval recorded", Run: func(ctx context.Context) (string, error) {
		task, err := m.taskStore.DecideTask(ctx, taskID, taskstate.ApprovalDecision{State: state, Actor: "tui", Reason: reason})
		if err != nil {
			return "", err
		}
		return taskSummary(ctx, m.taskStore, task.PlanID)
	}})
}

func (m model) updateTaskStatus(fields []string) (tea.Model, tea.Cmd) {
	if len(fields) < 3 {
		m.addSystem("usage: /tasks " + fields[1] + " <task-id>")
		return m, nil
	}
	status := taskstate.TaskStatusPending
	if fields[1] == "complete" {
		status = taskstate.TaskStatusCompleted
	}
	if fields[1] == "cancel" {
		status = taskstate.TaskStatusCancelled
	}
	taskID := fields[2]
	return m, drawerLoadCmd(drawerLoadRequest{Context: m.ctx, Drawer: drawerTasks, Status: "task updated", Run: func(ctx context.Context) (string, error) {
		task, err := m.taskStore.UpdateTask(ctx, taskstate.TaskUpdate{ID: taskID, Status: status})
		if err != nil {
			return "", err
		}
		return taskSummary(ctx, m.taskStore, task.PlanID)
	}})
}

func taskSummaryCmd(ctx context.Context, store TaskStore, planID string) tea.Cmd {
	return drawerLoadCmd(drawerLoadRequest{
		Context: ctx,
		Drawer:  drawerTasks,
		Status:  "tasks loaded",
		Run: func(ctx context.Context) (string, error) {
			return taskSummary(ctx, store, planID)
		},
	})
}

type drawerLoadRequest struct {
	Context context.Context
	Drawer  drawerMode
	Status  string
	Run     func(context.Context) (string, error)
}

func drawerLoadCmd(req drawerLoadRequest) tea.Cmd {
	return func() tea.Msg {
		text, err := req.Run(req.Context)
		return drawerLoadedMsg{Drawer: req.Drawer, Text: text, Status: req.Status, Err: err}
	}
}

func taskSummary(ctx context.Context, store TaskStore, planID string) (string, error) {
	if strings.TrimSpace(planID) != "" {
		return taskPlanSummary(ctx, store, planID)
	}
	plans, err := store.ListPlans(ctx)
	if err != nil {
		return "", err
	}
	if len(plans) == 0 {
		return "task plans: none", nil
	}
	lines := []string{"task plans"}
	for _, plan := range plans {
		tasks, err := store.ListTasks(ctx, plan.ID)
		if err != nil {
			return "", err
		}
		lines = append(lines, fmt.Sprintf("%s %s approval=%s tasks=%d", plan.ID, plan.Title, plan.Approval.State, len(tasks)))
	}
	return strings.Join(lines, "\n"), nil
}

func taskPlanSummary(ctx context.Context, store TaskStore, planID string) (string, error) {
	plan, err := store.GetPlan(ctx, planID)
	if err != nil {
		return "", err
	}
	tasks, err := store.ListTasks(ctx, planID)
	if err != nil {
		return "", err
	}
	lines := []string{fmt.Sprintf("plan %s: %s approval=%s", plan.ID, plan.Title, plan.Approval.State)}
	if len(tasks) == 0 {
		return strings.Join(append(lines, "tasks: none"), "\n"), nil
	}
	for _, task := range tasks {
		owner := task.Owner
		if owner == "" {
			owner = "-"
		}
		lines = append(lines, fmt.Sprintf("%s [%s] owner=%s approval=%s %s", task.ID, task.Status, owner, task.Approval.State, task.Title))
	}
	return strings.Join(lines, "\n"), nil
}
