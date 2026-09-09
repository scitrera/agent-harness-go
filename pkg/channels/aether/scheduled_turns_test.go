// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type recordingBoundTurnEnqueuer struct {
	inbound channel.Inbound
	binding spec.ExecutionBinding
	policy  ScheduledViewPolicy
	err     error
}

type fakeScheduleOperations struct {
	schedules            map[string]scheduledWorkflowDefinition
	upserts              []string
	deletes              []string
	listAuthorization    *pb.AuthorizationContext
	upsertOptions        []sdk.WorkflowScheduleOperationOptions
	deleteWorkspaces     []string
	deleteAuthorizations []*pb.AuthorizationContext
}

func (f *fakeScheduleOperations) ListSchedulesAuthorized(_ context.Context, _ string, authorization *pb.AuthorizationContext) (*sdk.WorkflowResponse, error) {
	f.listAuthorization = authorization
	definitions := make([]scheduledWorkflowDefinition, 0, len(f.schedules))
	for _, definition := range f.schedules {
		definitions = append(definitions, definition)
	}
	data, _ := json.Marshal(definitions)
	return &sdk.WorkflowResponse{Success: true, Data: data}, nil
}

func (f *fakeScheduleOperations) UpsertScheduleWithOptions(_ context.Context, data []byte, options sdk.WorkflowScheduleOperationOptions) (*sdk.WorkflowResponse, error) {
	var definition scheduledWorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		return nil, err
	}
	f.schedules[definition.ID] = definition
	f.upserts = append(f.upserts, definition.ID)
	f.upsertOptions = append(f.upsertOptions, options)
	return &sdk.WorkflowResponse{Success: true}, nil
}

func (f *fakeScheduleOperations) DeleteScheduleAuthorized(_ context.Context, workspace, id string, authorization *pb.AuthorizationContext) (*sdk.WorkflowResponse, error) {
	delete(f.schedules, id)
	f.deletes = append(f.deletes, id)
	f.deleteWorkspaces = append(f.deleteWorkspaces, workspace)
	f.deleteAuthorizations = append(f.deleteAuthorizations, authorization)
	return &sdk.WorkflowResponse{Success: true}, nil
}

type recordingScheduledTurnAuthorityProvider struct {
	requests            []ScheduledTurnAuthorityRequest
	authorization       *pb.AuthorizationContext
	scope               *pb.WorkflowScheduleAuthorityScope
	scopeEveryOperation bool
	err                 error
}

func (p *recordingScheduledTurnAuthorityProvider) AuthorityForScheduledTurn(_ context.Context, request ScheduledTurnAuthorityRequest) (ScheduledTurnAuthority, error) {
	p.requests = append(p.requests, request)
	if p.err != nil {
		return ScheduledTurnAuthority{}, p.err
	}
	authority := ScheduledTurnAuthority{Authorization: p.authorization}
	if request.Operation == ScheduledTurnAuthorityUpsert || p.scopeEveryOperation {
		authority.AuthorityScope = p.scope
	}
	return authority, nil
}

func (e *recordingBoundTurnEnqueuer) EnqueueBoundTurn(
	_ context.Context,
	inbound channel.Inbound,
	binding spec.ExecutionBinding,
	policy ScheduledViewPolicy,
) error {
	e.inbound = inbound
	e.binding = binding
	e.policy = policy
	return e.err
}

func scheduledRegistration() ScheduledTurnRegistration {
	binding := spec.NewExecutionBinding(
		"project-a", "view-a", "ag::routing::agent-harness::worker", spec.ExecutionSiteWorker,
	)
	binding.RootRef = "root:view-a"
	binding.Revision = "0123456789abcdef"
	return ScheduledTurnRegistration{
		ID: "daily-review", Name: "Daily review", ScheduleType: "cron",
		ScheduleExpression: "0 9 * * *", ThreadID: "scheduled-daily-review",
		Prompt: "Review this workspace", MissPolicy: ScheduledMissPolicyFireOnce, Enabled: true,
		TargetOfflinePolicy: "queue",
		Binding:             binding, ViewPolicy: ScheduledViewPolicy{
			WriteAccess: workspacepkg.ViewWriteAccessReadWrite, AllowDirtyView: true,
		},
	}
}

func TestScheduledTurnRegistrationAcceptsExplicitMissPolicies(t *testing.T) {
	for _, policy := range []string{
		ScheduledMissPolicySkip,
		ScheduledMissPolicyFireOnce,
		ScheduledMissPolicyFireAll,
	} {
		t.Run(policy, func(t *testing.T) {
			registration := scheduledRegistration()
			registration.MissPolicy = policy
			if err := registration.Validate(); err != nil {
				t.Fatalf("validate %q: %v", policy, err)
			}
		})
	}
	registration := scheduledRegistration()
	registration.MissPolicy = "future_policy"
	if err := registration.Validate(); err == nil {
		t.Fatal("unknown missed-fire policy was accepted")
	}
}

func TestScheduledViewPolicyRequiresDirtyOptInForWrites(t *testing.T) {
	registration := scheduledRegistration()
	registration.ViewPolicy = ScheduledViewPolicy{WriteAccess: "read_write"}
	if err := registration.Validate(); err == nil {
		t.Fatal("expected read-write schedule without dirty-view admission to fail")
	}
	registration.ViewPolicy.WriteAccess = "read_only"
	if err := registration.Validate(); err != nil {
		t.Fatalf("read-only schedule: %v", err)
	}
}

func TestScheduledTurnRegistrationValidatesAuthorityHopPolicy(t *testing.T) {
	registration := scheduledRegistration()
	registration.RequiredDownstreamAuthorityHops = 1
	if err := registration.Validate(); err == nil {
		t.Fatal("downstream authority without required task authority was accepted")
	}
	registration.RequireTaskAuthority = true
	if err := registration.Validate(); err != nil {
		t.Fatalf("valid authority policy: %v", err)
	}
	registration.RequiredDownstreamAuthorityHops = 2
	if err := registration.Validate(); err == nil {
		t.Fatal("unsupported downstream authority hop count was accepted")
	}
}

func TestScheduledWorkflowDataTargetsExactWorkerWithJSONEnvelope(t *testing.T) {
	registration := scheduledRegistration()
	data, err := scheduledWorkflowData("routing", registration.Binding.ToolHostID, registration)
	if err != nil {
		t.Fatal(err)
	}
	var definition scheduledWorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		t.Fatal(err)
	}
	if definition.Action.Metadata["scitrera.thread_id"] != registration.ThreadID {
		t.Fatalf("thread metadata = %q", definition.Action.Metadata["scitrera.thread_id"])
	}
	if definition.ID == "" || definition.MissPolicy != ScheduledMissPolicyFireOnce || definition.MaxConcurrent != 0 {
		t.Fatalf("schedule definition = %+v", definition)
	}
	if definition.Action.TargetAgentID != registration.Binding.ToolHostID ||
		definition.Action.TargetOfflinePolicy != "queue" ||
		definition.Action.PayloadEncoding != "json" ||
		definition.Action.TaskType != ScheduledTurnTaskType ||
		definition.Action.Payload.MissPolicy != ScheduledMissPolicyFireOnce ||
		!reflect.DeepEqual(definition.Action.Payload.Binding, registration.Binding) {
		t.Fatalf("schedule action = %+v", definition.Action)
	}
	if definition.Action.Metadata["scitrera.schedule_digest"] == "" ||
		definition.Action.Metadata["scitrera.schedule_miss_policy"] != ScheduledMissPolicyFireOnce ||
		definition.Action.Metadata["scitrera.view_revision"] != registration.Binding.Revision ||
		definition.Action.Metadata["turn_tool_host_id"] != registration.Binding.ToolHostID ||
		definition.Action.Metadata["turn_surface_kind"] != "worker" ||
		definition.Action.Metadata["turn_surface_instance_id"] != registration.Binding.ToolHostID {
		t.Fatalf("schedule metadata = %#v", definition.Action.Metadata)
	}
}

func TestScheduledTurnReconciliationDeletesOnlyOwnedStaleDefinitions(t *testing.T) {
	worker, _ := newTestChannel(t)
	registration := scheduledRegistration()
	registration.Binding.ToolHostID = worker.Topic()
	old := registration
	old.ID = "removed"
	old.Name = "Removed"
	oldData, err := scheduledWorkflowData(worker.workspace, worker.Topic(), old)
	if err != nil {
		t.Fatal(err)
	}
	var oldDefinition scheduledWorkflowDefinition
	if err := json.Unmarshal(oldData, &oldDefinition); err != nil {
		t.Fatal(err)
	}
	foreign := oldDefinition
	foreign.ID = "foreign-schedule"
	fake := &fakeScheduleOperations{schedules: map[string]scheduledWorkflowDefinition{
		oldDefinition.ID: oldDefinition,
		foreign.ID:       foreign,
	}}
	worker.scheduleOps = fake
	if err := worker.EnableScheduledTurns(context.Background(), []ScheduledTurnRegistration{registration}, nil, time.Second); err != nil {
		t.Fatal(err)
	}
	desiredID := scheduledWorkflowID(worker.workspace, worker.Topic(), registration.ID)
	if _, ok := fake.schedules[desiredID]; !ok {
		t.Fatalf("desired schedule %q was not upserted", desiredID)
	}
	if _, ok := fake.schedules[oldDefinition.ID]; ok {
		t.Fatalf("owned stale schedule %q was not deleted", oldDefinition.ID)
	}
	if _, ok := fake.schedules[foreign.ID]; !ok {
		t.Fatal("foreign schedule was deleted")
	}
	if err := worker.UpdateScheduledTurns(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.schedules[desiredID]; ok {
		t.Fatal("schedule removed from desired state was not deleted")
	}
	if _, ok := fake.schedules[foreign.ID]; !ok {
		t.Fatal("foreign schedule was deleted during empty reconciliation")
	}
}

func TestScheduledTurnReconciliationFailsClosedWithoutRequiredAuthorityProvider(t *testing.T) {
	worker, _ := newTestChannel(t)
	registration := scheduledRegistration()
	registration.Binding.ToolHostID = worker.Topic()
	registration.RequireTaskAuthority = true
	fake := &fakeScheduleOperations{schedules: map[string]scheduledWorkflowDefinition{}}
	worker.scheduleOps = fake
	if err := worker.EnableScheduledTurns(context.Background(), []ScheduledTurnRegistration{registration}, nil, time.Second); err == nil ||
		!strings.Contains(err.Error(), "no provider") {
		t.Fatalf("required-authority enable error = %v", err)
	}
	if len(fake.upserts) != 0 {
		t.Fatalf("required-authority schedule mutated Aether without provider: %+v", fake.upserts)
	}
	if worker.scheduledTurns != nil {
		t.Fatal("failed required-authority enable installed an executor")
	}
}

func TestScheduledTurnAuthorityProviderStaysOnTransportEnvelope(t *testing.T) {
	provider := &recordingScheduledTurnAuthorityProvider{}
	worker, err := New(Config{
		ServerAddr: "127.0.0.1:1", Workspace: "routing", Specifier: "authority-test",
		ScheduledTurnAuthority: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	registration := scheduledRegistration()
	registration.Binding.ToolHostID = worker.Topic()
	registration.RequireTaskAuthority = true
	registration.RequiredDownstreamAuthorityHops = 1
	authorization := &pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of", GrantId: "source-grant",
		Subject: &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"},
	}
	scope := &pb.WorkflowScheduleAuthorityScope{
		WorkspaceScope: []string{worker.workspace}, OperationScope: []string{"task_create"},
		MaxAccessLevel: 20, RequiredTaskAuthorityHops: 1,
		PolicyVersion: sdk.WorkflowScheduleAuthorityPolicyVersion,
	}
	provider.authorization = authorization
	provider.scope = scope
	fake := &fakeScheduleOperations{schedules: map[string]scheduledWorkflowDefinition{}}
	worker.scheduleOps = fake
	if err := worker.EnableScheduledTurns(context.Background(), []ScheduledTurnRegistration{registration}, nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if len(provider.requests) != 2 || provider.requests[0].Operation != ScheduledTurnAuthorityList ||
		provider.requests[1].Operation != ScheduledTurnAuthorityUpsert || provider.requests[1].Registration == nil {
		t.Fatalf("authority requests = %+v", provider.requests)
	}
	if len(fake.upsertOptions) != 1 || fake.upsertOptions[0].Authorization.GetGrantId() != "source-grant" ||
		fake.upsertOptions[0].AuthorityScope.GetRequiredTaskAuthorityHops() != 1 {
		t.Fatalf("upsert options = %+v", fake.upsertOptions)
	}
	definition := fake.schedules[scheduledWorkflowID(worker.workspace, worker.Topic(), registration.ID)]
	if !definition.Action.RequireTaskAuthority || definition.Action.RequiredDownstreamAuthorityHops != 1 {
		t.Fatalf("scheduled action authority policy = %+v", definition.Action)
	}
	encoded, err := json.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "source-grant") || strings.Contains(string(encoded), "alice") {
		t.Fatalf("transport authority leaked into schedule JSON: %s", encoded)
	}
	if err := worker.UpdateScheduledTurns(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(fake.deleteWorkspaces) != 1 || fake.deleteWorkspaces[0] != worker.workspace ||
		fake.deleteAuthorizations[0].GetGrantId() != "source-grant" {
		t.Fatalf("delete workspace/auth = %+v %+v", fake.deleteWorkspaces, fake.deleteAuthorizations)
	}
}

func TestScheduledTurnAuthorityProviderRejectsInvalidShapes(t *testing.T) {
	worker, _ := newTestChannel(t)
	registration := scheduledRegistration()
	registration.Binding.ToolHostID = worker.Topic()
	registration.RequireTaskAuthority = true
	registration.RequiredDownstreamAuthorityHops = 1
	authorization := &pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of", GrantId: "source-grant",
		Subject: &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"},
	}
	validScope := &pb.WorkflowScheduleAuthorityScope{
		PolicyVersion:             sdk.WorkflowScheduleAuthorityPolicyVersion,
		RequiredTaskAuthorityHops: 1,
	}
	for name, test := range map[string]struct {
		request  ScheduledTurnAuthorityRequest
		provider *recordingScheduledTurnAuthorityProvider
	}{
		"unknown operation": {
			request:  ScheduledTurnAuthorityRequest{Operation: "future"},
			provider: &recordingScheduledTurnAuthorityProvider{authorization: authorization},
		},
		"scope on list": {
			request:  ScheduledTurnAuthorityRequest{Operation: ScheduledTurnAuthorityList},
			provider: &recordingScheduledTurnAuthorityProvider{authorization: authorization, scope: validScope, scopeEveryOperation: true},
		},
		"missing authorization": {
			request:  ScheduledTurnAuthorityRequest{Operation: ScheduledTurnAuthorityUpsert, Registration: &registration},
			provider: &recordingScheduledTurnAuthorityProvider{scope: validScope},
		},
		"unknown policy version": {
			request:  ScheduledTurnAuthorityRequest{Operation: ScheduledTurnAuthorityUpsert, Registration: &registration},
			provider: &recordingScheduledTurnAuthorityProvider{authorization: authorization, scope: &pb.WorkflowScheduleAuthorityScope{RequiredTaskAuthorityHops: 1}},
		},
		"insufficient hops": {
			request:  ScheduledTurnAuthorityRequest{Operation: ScheduledTurnAuthorityUpsert, Registration: &registration},
			provider: &recordingScheduledTurnAuthorityProvider{authorization: authorization, scope: &pb.WorkflowScheduleAuthorityScope{PolicyVersion: sdk.WorkflowScheduleAuthorityPolicyVersion}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			worker.scheduledTurnAuthority = test.provider
			if _, err := worker.authorityForScheduledTurn(context.Background(), test.request); err == nil {
				t.Fatal("invalid scheduled authority shape was accepted")
			}
		})
	}
}

func TestScheduledTurnAuthorityProviderCannotMutateAuthorityPolicy(t *testing.T) {
	worker, _ := newTestChannel(t)
	registration := scheduledRegistration()
	registration.RequireTaskAuthority = true
	worker.scheduledTurnAuthority = ScheduledTurnAuthorityProviderFunc(func(
		_ context.Context,
		request ScheduledTurnAuthorityRequest,
	) (ScheduledTurnAuthority, error) {
		request.Registration.RequireTaskAuthority = false
		return ScheduledTurnAuthority{}, nil
	})
	_, err := worker.authorityForScheduledTurn(context.Background(), ScheduledTurnAuthorityRequest{
		Operation: ScheduledTurnAuthorityUpsert, Registration: &registration,
	})
	if err == nil || !strings.Contains(err.Error(), "required task authority") {
		t.Fatalf("mutated authority policy error = %v", err)
	}
	if !registration.RequireTaskAuthority {
		t.Fatal("provider mutated caller registration")
	}
}

func TestListScheduledTurnScheduleStatesProjectsSkippedBacklog(t *testing.T) {
	worker, _ := newTestChannel(t)
	registration := scheduledRegistration()
	registration.Binding.ToolHostID = worker.Topic()
	registration.MissPolicy = ScheduledMissPolicySkip
	data, err := scheduledWorkflowData(worker.workspace, worker.Topic(), registration)
	if err != nil {
		t.Fatal(err)
	}
	var definition scheduledWorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		t.Fatal(err)
	}
	lastFiredAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	scheduledFor := lastFiredAt.Add(24 * time.Hour)
	nextFireAt := scheduledFor.Add(24 * time.Hour)
	definition.LastFiredAt = &lastFiredAt
	definition.NextFireAt = &nextFireAt
	definition.LastOccurrence = &ScheduledTurnScheduleOccurrence{
		ScheduledFor: scheduledFor, Disposition: ScheduledDispositionSkipped,
		Reason: ScheduledSkipReasonMissPolicy, BacklogCount: scheduledTurnBacklogDetailLimit,
		BacklogTruncated: true,
	}
	worker.scheduleOps = &fakeScheduleOperations{schedules: map[string]scheduledWorkflowDefinition{
		definition.ID: definition,
	}}

	states, err := worker.ListScheduledTurnScheduleStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 {
		t.Fatalf("schedule states = %+v", states)
	}
	state := states[0]
	if state.WorkflowScheduleID != definition.ID || state.DeclarationID != registration.ID ||
		state.RoutingWorkspace != worker.workspace || state.AssignedTo != worker.Topic() ||
		state.LogicalWorkspace != registration.Binding.WorkspaceID || state.ThreadID != registration.ThreadID ||
		state.ViewID != registration.Binding.ViewID || state.ViewRevision != registration.Binding.Revision ||
		state.ScheduleType != registration.ScheduleType || state.ScheduleExpression != registration.ScheduleExpression ||
		state.LastFiredAt == nil || !state.LastFiredAt.Equal(lastFiredAt) ||
		state.NextFireAt == nil || !state.NextFireAt.Equal(nextFireAt) {
		t.Fatalf("schedule state = %+v", state)
	}
	if state.LastOccurrence == nil || state.LastOccurrence.Disposition != ScheduledDispositionSkipped ||
		state.LastOccurrence.Reason != ScheduledSkipReasonMissPolicy || state.LastOccurrence.DispatchedAt != nil ||
		state.LastOccurrence.BacklogCount != scheduledTurnBacklogDetailLimit ||
		!state.LastOccurrence.BacklogTruncated || state.LastOccurrence.BacklogIndex != 0 {
		t.Fatalf("schedule occurrence = %+v", state.LastOccurrence)
	}
}

func TestListScheduledTurnScheduleStatesRejectsMalformedOwnedOccurrence(t *testing.T) {
	worker, _ := newTestChannel(t)
	registration := scheduledRegistration()
	registration.Binding.ToolHostID = worker.Topic()
	data, err := scheduledWorkflowData(worker.workspace, worker.Topic(), registration)
	if err != nil {
		t.Fatal(err)
	}
	var definition scheduledWorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		t.Fatal(err)
	}
	definition.LastOccurrence = &ScheduledTurnScheduleOccurrence{
		ScheduledFor: time.Now().UTC(), Disposition: ScheduledDispositionCoalesced,
		BacklogCount: 2, BacklogIndex: 1,
	}
	worker.scheduleOps = &fakeScheduleOperations{schedules: map[string]scheduledWorkflowDefinition{
		definition.ID: definition,
	}}
	if _, err := worker.ListScheduledTurnScheduleStates(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "dispatch time") {
		t.Fatalf("malformed occurrence error = %v", err)
	}
}

func TestScheduledTurnExecutorEnqueuesPinnedWorkerTurn(t *testing.T) {
	registration := scheduledRegistration()
	envelope, err := scheduledTurnEnvelopeFor(registration)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	operations := &fakeTaskOperations{queryResponses: []*sdk.TaskQueryResponse{{
		Success: true, Task: &sdk.TaskInfo{TaskID: "task-1", Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()},
	}}}
	enqueuer := &recordingBoundTurnEnqueuer{}
	executor, err := NewScheduledTurnExecutor(
		operations, "routing", registration.Binding.ToolHostID,
		[]ScheduledTurnRegistration{registration}, enqueuer, nil, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	assignment := &sdk.TaskAssignment{
		TaskID: "task-1", TaskType: ScheduledTurnTaskType,
		AssignedTo: registration.Binding.ToolHostID, Workspace: "routing",
		Metadata: scheduledTurnMetadata(envelope), Payload: payload,
	}
	if err := executor.HandleAssignment(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(enqueuer.binding, registration.Binding) || enqueuer.inbound.Addr.TaskID != "task-1" ||
		enqueuer.inbound.Addr.ThreadID != registration.ThreadID || !IsScheduledTurnMessage(enqueuer.inbound.Message) {
		t.Fatalf("enqueued turn = %+v binding=%+v", enqueuer.inbound, enqueuer.binding)
	}
	if enqueuer.policy != registration.ViewPolicy {
		t.Fatalf("enqueued view policy = %+v", enqueuer.policy)
	}
	gotBinding, err := spec.GetExecutionBinding(enqueuer.inbound.Message)
	if err != nil || gotBinding == nil || !reflect.DeepEqual(*gotBinding, registration.Binding) {
		t.Fatalf("message binding = %+v err=%v", gotBinding, err)
	}
}

func TestScheduledTurnExecutorRecoversQueuedAssignmentAfterDeathBeforeClaim(t *testing.T) {
	registration := scheduledRegistration()
	envelope, err := scheduledTurnEnvelopeFor(registration)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	info := &sdk.TaskInfo{
		TaskID: "task-before-claim", TaskType: ScheduledTurnTaskType,
		Status: pb.TaskStatus_TASK_STATUS_QUEUED.String(), Workspace: "routing",
		AssignedTo: registration.Binding.ToolHostID, Metadata: scheduledTurnMetadata(envelope),
	}

	// The first process receives and enqueues the assignment, then disappears
	// before the runtime lifecycle decorator can claim it.
	firstOps := &fakeTaskOperations{queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: info}}}
	firstQueue := &recordingBoundTurnEnqueuer{}
	first, err := NewScheduledTurnExecutor(
		firstOps, "routing", registration.Binding.ToolHostID,
		[]ScheduledTurnRegistration{registration}, firstQueue, nil, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.HandleAssignment(context.Background(), &sdk.TaskAssignment{
		TaskID: info.TaskID, TaskType: info.TaskType, AssignedTo: info.AssignedTo,
		Workspace: info.Workspace, Metadata: info.Metadata, Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if firstQueue.inbound.Addr.TaskID != info.TaskID || firstOps.claimCalls != 0 {
		t.Fatalf("first delivery=%+v claim_calls=%d", firstQueue.inbound, firstOps.claimCalls)
	}

	// Aether's assigned state still projects as QUEUED. A fresh worker recovers
	// the exact digest-bound payload; claim remains the next, fail-closed runtime
	// boundary rather than an untracked side effect of assignment delivery.
	restartedOps := &fakeTaskOperations{
		listResponses:  []*sdk.TaskQueryResponse{{Success: true, Tasks: []*sdk.TaskInfo{info}, TotalCount: 1}},
		queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: info}},
	}
	restartedQueue := &recordingBoundTurnEnqueuer{}
	restarted, err := NewScheduledTurnExecutor(
		restartedOps, "routing", registration.Binding.ToolHostID,
		[]ScheduledTurnRegistration{registration}, restartedQueue, nil, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restartedQueue.inbound.Addr.TaskID != info.TaskID || restartedOps.claimCalls != 0 || len(restartedOps.listCalls) != 1 {
		t.Fatalf("recovered delivery=%+v ops=%+v", restartedQueue.inbound, restartedOps)
	}
}

func TestPrepareTurnRecoveryRestoresExactScheduledWorkerView(t *testing.T) {
	ctx := context.Background()
	worker, _ := newTestChannel(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "identity.txt"), []byte("recovered scheduled view"), 0o644); err != nil {
		t.Fatal(err)
	}
	host, err := NewWorkerToolHost(ctx, WorkerToolHostConfig{
		WorkspaceID: "project-a", WorkspaceRoot: root, StateDir: t.TempDir(), ToolHostID: worker.Topic(),
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.SetWorkerToolHost(host)
	binding, err := host.ScheduledExecutionBindingForDirectory(ctx, root, true, false)
	if err != nil {
		t.Fatal(err)
	}
	registration := scheduledRegistration()
	registration.Binding = binding
	registration.ViewPolicy = ScheduledViewPolicy{WriteAccess: workspacepkg.ViewWriteAccessReadOnly, AllowMutableView: true}
	executor, err := NewScheduledTurnExecutor(
		&fakeTaskOperations{}, worker.workspace, worker.Topic(),
		[]ScheduledTurnRegistration{registration}, &recordingBoundTurnEnqueuer{}, nil, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	worker.scheduledTurns = executor
	envelope, err := scheduledTurnEnvelopeFor(registration)
	if err != nil {
		t.Fatal(err)
	}
	info := &sdk.TaskInfo{
		TaskID: "running-schedule", TaskType: ScheduledTurnTaskType,
		Status: pb.TaskStatus_TASK_STATUS_RUNNING.String(), Workspace: worker.workspace,
		AssignedTo: worker.Topic(), Metadata: scheduledTurnMetadata(envelope),
	}
	cleanup, err := worker.PrepareTurnRecovery(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	addr := spec.MessageAddress{WorkspaceID: binding.WorkspaceID, ThreadID: registration.ThreadID, TaskID: info.TaskID}
	turnCtx := worker.TurnContext(ctx, addr)
	delegate := tools.ToolDelegateFrom(turnCtx)
	if delegate == nil {
		t.Fatal("scheduled recovery did not restore the exact-view delegate")
	}
	result, err := delegate.InvokeTool(turnCtx, tools.Request{
		CallID: "read-recovered", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
	})
	if err != nil || !strings.Contains(string(result.Payload), "recovered scheduled view") {
		t.Fatalf("recovered view result=%s err=%v", result.Payload, err)
	}
	cleanup()
	if tools.ToolDelegateFrom(worker.TurnContext(ctx, addr)) != nil {
		t.Fatal("scheduled recovery cleanup retained the process-local binding")
	}
}

func TestScheduledTurnExecutorFailsClosedOnDeclarationDrift(t *testing.T) {
	registration := scheduledRegistration()
	envelope, _ := scheduledTurnEnvelopeFor(registration)
	payload, _ := json.Marshal(envelope)
	registration.Prompt = "A changed prompt"
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{
			Success: true, Task: &sdk.TaskInfo{TaskID: "task-1", Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()},
		}},
		failResponse: &sdk.TaskOperationResponse{Success: true},
	}
	executor, err := NewScheduledTurnExecutor(
		operations, "routing", registration.Binding.ToolHostID,
		[]ScheduledTurnRegistration{registration}, &recordingBoundTurnEnqueuer{}, nil, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = executor.HandleAssignment(context.Background(), &sdk.TaskAssignment{
		TaskID: "task-1", TaskType: ScheduledTurnTaskType,
		AssignedTo: registration.Binding.ToolHostID, Workspace: "routing",
		Metadata: scheduledTurnMetadata(envelope), Payload: payload,
	})
	if err == nil || operations.failCalls != 1 {
		t.Fatalf("declaration drift err=%v failCalls=%d", err, operations.failCalls)
	}
}

func TestScheduledTurnFinalizationReleasesBindingPolicyAndDedupState(t *testing.T) {
	registration := scheduledRegistration()
	executor, err := NewScheduledTurnExecutor(
		&fakeTaskOperations{}, "default", registration.Binding.ToolHostID,
		[]ScheduledTurnRegistration{registration}, &recordingBoundTurnEnqueuer{}, nil, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !executor.markSeen("task-final") {
		t.Fatal("task was unexpectedly already seen")
	}
	worker, _ := newTestChannel(t)
	worker.scheduledTurns = executor
	worker.mu.Lock()
	worker.executionBindings["task-final"] = registration.Binding
	worker.executionPolicies["task-final"] = registration.ViewPolicy
	worker.mu.Unlock()
	if err := worker.PublishEvent(context.Background(), channel.Event{
		Type: channel.EventMessageFinal,
		Addr: spec.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread", TaskID: "task-final"},
	}); err != nil {
		t.Fatal(err)
	}
	worker.mu.Lock()
	_, bound := worker.executionBindings["task-final"]
	_, policy := worker.executionPolicies["task-final"]
	worker.mu.Unlock()
	if bound || policy || !executor.markSeen("task-final") {
		t.Fatalf("finalization retained task state: binding=%v policy=%v", bound, policy)
	}
}
