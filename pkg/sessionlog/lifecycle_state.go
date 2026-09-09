// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// SubagentStateSource supplies deterministic parent-scoped lifecycle records.
type SubagentStateSource interface {
	ListSubagents(ctx context.Context, workspaceID, parentSessionID string) ([]spec.SessionSubagentRecord, error)
}

// GoalStateSource supplies deterministic session-scoped goal records.
type GoalStateSource interface {
	ListGoals(ctx context.Context, workspaceID, sessionID string) ([]spec.SessionGoalRecord, error)
}

// CapabilityStateProvider is a snapshot state provider that explicitly names
// the optional protocol behavior it backs.
type CapabilityStateProvider interface {
	SnapshotStateProvider
	Capabilities() []spec.SessionCapability
}

// NegotiatedSnapshotStateProvider avoids loading optional namespaces a client
// did not negotiate. This keeps an unavailable optional backend from breaking a
// legacy snapshot request that cannot consume it.
type NegotiatedSnapshotStateProvider interface {
	CapabilityStateProvider
	SnapshotStateForCapabilities(
		ctx context.Context,
		workspaceID, sessionID string,
		cursor spec.SessionCursor,
		messages []protocol.ChatMessage,
		capabilities []spec.SessionCapability,
	) (map[string]json.RawMessage, error)
}

type LifecycleStateProviderConfig struct {
	Subagents SubagentStateSource
	Goals     GoalStateSource
}

// LifecycleStateProvider projects typed durable stores into the standard
// capability-owned SessionSnapshot.state namespaces.
type LifecycleStateProvider struct {
	subagents SubagentStateSource
	goals     GoalStateSource
}

func NewLifecycleStateProvider(config LifecycleStateProviderConfig) (*LifecycleStateProvider, error) {
	if config.Subagents == nil && config.Goals == nil {
		return nil, errors.New("sessionlog: lifecycle state provider requires at least one source")
	}
	return &LifecycleStateProvider{subagents: config.Subagents, goals: config.Goals}, nil
}

func (p *LifecycleStateProvider) Capabilities() []spec.SessionCapability {
	capabilities := make([]spec.SessionCapability, 0, 2)
	if p.goals != nil {
		capabilities = append(capabilities, spec.SessionCapabilityGoalsState)
	}
	if p.subagents != nil {
		capabilities = append(capabilities, spec.SessionCapabilitySubagentsState)
	}
	return spec.NormalizeSessionCapabilities(capabilities)
}

func (p *LifecycleStateProvider) SnapshotState(
	ctx context.Context,
	workspaceID, sessionID string,
	cursor spec.SessionCursor,
	messages []protocol.ChatMessage,
) (map[string]json.RawMessage, error) {
	return p.SnapshotStateForCapabilities(ctx, workspaceID, sessionID, cursor, messages, p.Capabilities())
}

func (p *LifecycleStateProvider) SnapshotStateForCapabilities(
	ctx context.Context,
	workspaceID, sessionID string,
	_ spec.SessionCursor,
	_ []protocol.ChatMessage,
	capabilities []spec.SessionCapability,
) (map[string]json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workspaceID == "" || sessionID == "" {
		return nil, errors.New("sessionlog: lifecycle snapshot requires workspace and session")
	}
	state := make(map[string]json.RawMessage, 2)
	if p.goals != nil && spec.SupportsSessionCapability(capabilities, spec.SessionCapabilityGoalsState) {
		records, err := p.goals.ListGoals(ctx, workspaceID, sessionID)
		if err != nil {
			return nil, fmt.Errorf("sessionlog: snapshot goals: %w", err)
		}
		projection := spec.SessionGoalsState{SchemaRevision: spec.SessionGoalsStateSchemaRevision, Records: records}
		if err := projection.Validate(); err != nil {
			return nil, fmt.Errorf("sessionlog: invalid goals projection: %w", err)
		}
		raw, err := json.Marshal(projection)
		if err != nil {
			return nil, fmt.Errorf("sessionlog: encode goals projection: %w", err)
		}
		state[spec.SessionStateGoals] = raw
	}
	if p.subagents != nil && spec.SupportsSessionCapability(capabilities, spec.SessionCapabilitySubagentsState) {
		records, err := p.subagents.ListSubagents(ctx, workspaceID, sessionID)
		if err != nil {
			return nil, fmt.Errorf("sessionlog: snapshot subagents: %w", err)
		}
		projection := spec.SessionSubagentsState{SchemaRevision: spec.SessionSubagentsStateSchemaRevision, Records: records}
		if err := projection.Validate(sessionID); err != nil {
			return nil, fmt.Errorf("sessionlog: invalid subagents projection: %w", err)
		}
		raw, err := json.Marshal(projection)
		if err != nil {
			return nil, fmt.Errorf("sessionlog: encode subagents projection: %w", err)
		}
		state[spec.SessionStateSubagents] = raw
	}
	return state, nil
}

var _ NegotiatedSnapshotStateProvider = (*LifecycleStateProvider)(nil)
