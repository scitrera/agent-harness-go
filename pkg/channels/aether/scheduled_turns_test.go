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
	schedules map[string]scheduledWorkflowDefinition
	upserts   []string
	deletes   []string
}

func (f *fakeScheduleOperations) ListSchedules(context.Context, string) (*sdk.WorkflowResponse, error) {
	definitions := make([]scheduledWorkflowDefinition, 0, len(f.schedules))
	for _, definition := range f.schedules {
		definitions = append(definitions, definition)
	}
	data, _ := json.Marshal(definitions)
	return &sdk.WorkflowResponse{Success: true, Data: data}, nil
}

func (f *fakeScheduleOperations) UpsertSchedule(_ context.Context, data []byte) (*sdk.WorkflowResponse, error) {
	var definition scheduledWorkflowDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		return nil, err
	}
	f.schedules[definition.ID] = definition
	f.upserts = append(f.upserts, definition.ID)
	return &sdk.WorkflowResponse{Success: true}, nil
}

func (f *fakeScheduleOperations) DeleteSchedule(_ context.Context, id string) (*sdk.WorkflowResponse, error) {
	delete(f.schedules, id)
	f.deletes = append(f.deletes, id)
	return &sdk.WorkflowResponse{Success: true}, nil
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
		definition.Action.Metadata["scitrera.view_revision"] != registration.Binding.Revision {
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
