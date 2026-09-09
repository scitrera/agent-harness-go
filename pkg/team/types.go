// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package team

import (
	"context"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

type AgentID string

type AgentStatus string

const (
	AgentStatusRunning   AgentStatus = "running"
	AgentStatusCompleted AgentStatus = "completed"
	AgentStatusCancelled AgentStatus = "cancelled"
)

type AgentNode struct {
	ID        AgentID            `json:"id"`
	Name      subagent.AgentName `json:"name,omitempty"`
	Type      subagent.AgentType `json:"type"`
	Status    AgentStatus        `json:"status"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
	Metadata  map[string]string  `json:"metadata,omitempty"`
}

type AgentEdge struct {
	ParentID  AgentID   `json:"parent_id"`
	ChildID   AgentID   `json:"child_id"`
	CreatedAt time.Time `json:"created_at"`
}

type GraphStore interface {
	RegisterAgent(ctx context.Context, node AgentNode) error
	AddChild(ctx context.Context, req AddChildRequest) error
	UpdateAgentStatus(ctx context.Context, id AgentID, status AgentStatus) (AgentNode, error)
	DescendantsBFS(ctx context.Context, parentID AgentID) ([]AgentNode, error)
}

type AddChildRequest struct {
	ParentID          AgentID
	Child             AgentNode
	MaxActiveChildren int
}

type CoordinatorOptions struct {
	LeaderID          AgentID
	MaxActiveChildren int
}

type Coordinator struct {
	tasks   taskstate.Store
	graph   GraphStore
	leader  AgentID
	maxKids int
}

type ClaimNextRequest struct {
	PlanID     string
	AgentID    AgentID
	StaleAfter time.Duration
}

type ClaimTaskRequest struct {
	PlanID     string
	TaskID     string
	AgentID    AgentID
	StaleAfter time.Duration
}

type TaskOwnerRequest struct {
	PlanID  string
	TaskID  string
	AgentID AgentID
}

type PlanExitApproval struct {
	PlanID  string
	ActorID AgentID
	Approve bool
	Reason  string
}
