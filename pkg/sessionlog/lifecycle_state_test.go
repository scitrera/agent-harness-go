package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/harness"
)

type lifecycleSources struct {
	subagents []spec.SessionSubagentRecord
	goals     []spec.SessionGoalRecord
}

type unavailableLifecycleSources struct{}

func (*unavailableLifecycleSources) ListSubagents(context.Context, string, string) ([]spec.SessionSubagentRecord, error) {
	return nil, errors.New("subagent backend unavailable")
}

func (*unavailableLifecycleSources) ListGoals(context.Context, string, string) ([]spec.SessionGoalRecord, error) {
	return nil, errors.New("goal backend unavailable")
}

func (s *lifecycleSources) ListSubagents(_ context.Context, _, _ string) ([]spec.SessionSubagentRecord, error) {
	return append([]spec.SessionSubagentRecord(nil), s.subagents...), nil
}

func (s *lifecycleSources) ListGoals(_ context.Context, _, _ string) ([]spec.SessionGoalRecord, error) {
	return append([]spec.SessionGoalRecord(nil), s.goals...), nil
}

func TestLifecycleStateProviderProjectsNegotiatedNamespaces(t *testing.T) {
	ctx := context.Background()
	sources := &lifecycleSources{
		subagents: []spec.SessionSubagentRecord{{
			ID: "child-1", ParentSessionID: "session-1", ChildSessionID: "session-1::sub::1",
			Status: spec.SessionSubagentRunning, CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
		}},
		goals: []spec.SessionGoalRecord{{
			ID: "goal-1", Objective: "verify recovery", Status: spec.SessionGoalActive,
			CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
		}},
	}
	provider, err := NewLifecycleStateProvider(LifecycleStateProviderConfig{Subagents: sources, Goals: sources})
	if err != nil {
		t.Fatal(err)
	}
	state, err := provider.SnapshotState(ctx, "project-a", "session-1", spec.SessionCursor{Generation: "generation-1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 2 {
		t.Fatalf("state namespaces = %#v", state)
	}
	var subagents spec.SessionSubagentsState
	if err := json.Unmarshal(state[spec.SessionStateSubagents], &subagents); err != nil || len(subagents.Records) != 1 {
		t.Fatalf("subagents projection = %#v, %v", subagents, err)
	}

	resolver, err := NewStaticWorkspaceResolver("project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := append(spec.DefaultSessionCapabilities(), provider.Capabilities()...)
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Workspaces:   resolver,
		History:      harness.NewMemoryStore(),
		Events:       NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()}),
		State:        provider,
		Capabilities: capabilities,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest("session-1", "client-1")
	withoutState, err := coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(withoutState.Snapshot.State) != 0 {
		t.Fatalf("unrequested state leaked: %#v", withoutState.Snapshot.State)
	}

	request.Capabilities = append(request.Capabilities, spec.SessionCapabilityGoalsState, spec.SessionCapabilitySubagentsState)
	withState, err := coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(withState.Snapshot.State) != 2 || !spec.SupportsSessionCapability(withState.Capabilities, spec.SessionCapabilityGoalsState) || !spec.SupportsSessionCapability(withState.Capabilities, spec.SessionCapabilitySubagentsState) {
		t.Fatalf("negotiated lifecycle state = %#v caps=%#v", withState.Snapshot.State, withState.Capabilities)
	}
	if err := withState.ValidateState(); err != nil {
		t.Fatalf("state validation: %v", err)
	}
}

func TestLifecycleStateProviderEmitsExplicitEmptyState(t *testing.T) {
	sources := &lifecycleSources{}
	provider, err := NewLifecycleStateProvider(LifecycleStateProviderConfig{Subagents: sources, Goals: sources})
	if err != nil {
		t.Fatal(err)
	}
	state, err := provider.SnapshotState(context.Background(), "project-a", "session-1", spec.SessionCursor{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{spec.SessionStateGoals, spec.SessionStateSubagents} {
		var value struct {
			SchemaRevision uint32            `json:"schema_revision"`
			Records        []json.RawMessage `json:"records"`
		}
		if err := json.Unmarshal(state[key], &value); err != nil || value.SchemaRevision != 1 || value.Records == nil || len(value.Records) != 0 {
			t.Fatalf("empty state %q = %s, %v", key, state[key], err)
		}
	}
}

func TestCoordinatorDoesNotLoadUnnegotiatedLifecycleState(t *testing.T) {
	provider, err := NewLifecycleStateProvider(LifecycleStateProviderConfig{
		Subagents: &unavailableLifecycleSources{},
		Goals:     &unavailableLifecycleSources{},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStaticWorkspaceResolver("project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Workspaces:   resolver,
		History:      harness.NewMemoryStore(),
		Events:       NewMemoryEventLog(MemoryEventLogConfig{NewGeneration: sequenceGenerations()}),
		State:        provider,
		Capabilities: append(spec.DefaultSessionCapabilities(), provider.Capabilities()...),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest("session-1", "client-1")
	if _, err := coordinator.Attach(context.Background(), request); err != nil {
		t.Fatalf("unnegotiated optional state broke attach: %v", err)
	}
	request.Capabilities = append(request.Capabilities, spec.SessionCapabilitySubagentsState)
	if _, err := coordinator.Attach(context.Background(), request); err == nil {
		t.Fatal("negotiated unavailable subagent state unexpectedly succeeded")
	}
}
