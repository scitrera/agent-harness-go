// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"context"
	"reflect"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/goal"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

func TestParseVisibleWorkspacesNormalizesList(t *testing.T) {
	want := []string{"project-a", "project-b"}
	if got := parseVisibleWorkspaces(" project-b,project-a,,project-b "); !reflect.DeepEqual(got, want) {
		t.Fatalf("parseVisibleWorkspaces() = %#v, want %#v", got, want)
	}
}

func TestAdditionalVisibleWorkspaceIgnoresDefault(t *testing.T) {
	if hasAdditionalVisibleWorkspace("project-a", []string{"project-a"}) {
		t.Fatal("default-only list enabled multi-workspace capability")
	}
	if !hasAdditionalVisibleWorkspace("project-a", []string{"project-a", "project-b"}) {
		t.Fatal("additional workspace did not enable multi-workspace capability")
	}
}

func TestSessionTransportAdvertisesAndEnforcesVisibleWorkspaces(t *testing.T) {
	resolver, err := newSessionWorkspaceResolver("project-a", []string{"project-b"})
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	transport, err := newSessionTransport(stateDir, newTestSessionStores(t, stateDir), nil, resolver, "project-a", true, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest("shared", "client-1")
	request.WorkspaceID = "project-b"
	request.Capabilities = append(request.Capabilities, spec.SessionCapabilityMultiWorkspace)
	result, err := transport.Coordinator.Attach(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkspaceID != "project-b" || !spec.SupportsSessionCapability(result.Capabilities, spec.SessionCapabilityMultiWorkspace) {
		t.Fatalf("attach result = %+v", result)
	}
	request.WorkspaceID = "project-c"
	if _, err := transport.Coordinator.Attach(context.Background(), request); err == nil {
		t.Fatal("unlisted workspace was accepted")
	}
}

func TestSessionTransportResumesAfterRestart(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	resolver, err := newSessionWorkspaceResolver("project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	firstStores := newTestSessionStores(t, stateDir)
	firstTransport, err := newSessionTransport(stateDir, firstStores, nil, resolver, "project-a", false, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := spec.NewSessionAttachRequest("session-1", "client-1")
	firstAttach, err := firstTransport.Coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	message := spec.NewChatMessage("message-1", spec.RoleAssistant)
	message.Content = []spec.ContentPart{spec.NewTextPart("survives restart")}
	if err := firstTransport.Publisher.PublishEvent(ctx, channel.Event{
		Type:    channel.EventMessageFinal,
		Addr:    protocol.MessageAddress{ThreadID: request.SessionID},
		Message: &message,
	}); err != nil {
		t.Fatal(err)
	}
	if err := firstStores.subagents.ObserveSubagent(ctx, subagent.LifecycleEvent{
		WorkspaceID: "project-a",
		Record: spec.SessionSubagentRecord{
			ID: "child-1", ParentSessionID: request.SessionID, ChildSessionID: request.SessionID + "::sub::1",
			Status: spec.SessionSubagentRunning, CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := firstStores.goals.PutGoal(ctx, "project-a", request.SessionID, spec.SessionGoalRecord{
		ID: "goal-1", Objective: "survive restart", Status: spec.SessionGoalActive,
		CreatedAt: "2026-08-08T20:00:00Z", UpdatedAt: "2026-08-08T20:01:00Z",
	}); err != nil {
		t.Fatal(err)
	}

	restartedTransport, err := newSessionTransport(stateDir, newTestSessionStores(t, stateDir), nil, resolver, "project-a", false, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.ResumeAfter = &firstAttach.Snapshot.Cursor
	request.Capabilities = append(request.Capabilities, spec.SessionCapabilityGoalsState, spec.SessionCapabilitySubagentsState)
	restartedAttach, err := restartedTransport.Coordinator.Attach(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if restartedAttach.Snapshot.Cursor.Generation != firstAttach.Snapshot.Cursor.Generation || restartedAttach.Snapshot.Cursor.Sequence != 1 {
		t.Fatalf("restart cursor = %#v; initial = %#v", restartedAttach.Snapshot.Cursor, firstAttach.Snapshot.Cursor)
	}
	if restartedAttach.Replay == nil || restartedAttach.Replay.Status != spec.SessionReplayComplete || len(restartedAttach.Replay.Events) != 1 {
		t.Fatalf("restart replay = %#v", restartedAttach.Replay)
	}
	if len(restartedAttach.Snapshot.Messages) != 1 || restartedAttach.Snapshot.Messages[0].ID != message.ID {
		t.Fatalf("restart snapshot = %#v", restartedAttach.Snapshot.Messages)
	}
	goals, err := restartedAttach.Snapshot.DecodeGoalsState()
	if err != nil || goals == nil || len(goals.Records) != 1 || goals.Records[0].ID != "goal-1" {
		t.Fatalf("restart goals = %#v, %v", goals, err)
	}
	subagents, err := restartedAttach.Snapshot.DecodeSubagentsState()
	if err != nil || subagents == nil || len(subagents.Records) != 1 || subagents.Records[0].ID != "child-1" {
		t.Fatalf("restart subagents = %#v, %v", subagents, err)
	}
}

func newTestSessionStores(t *testing.T, stateDir string) stores {
	t.Helper()
	subagents, err := subagent.NewFileRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	goals, err := goal.NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	return stores{
		history:   &recordingScopedHistory{},
		subagents: subagents,
		goals:     goals,
	}
}
