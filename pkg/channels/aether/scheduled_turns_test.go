package aether

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type recordingBoundTurnEnqueuer struct {
	inbound channel.Inbound
	binding spec.ExecutionBinding
	policy  ScheduledViewPolicy
	err     error
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
		Prompt: "Review this workspace", MissPolicy: "fire_once", Enabled: true,
		Binding: binding, ViewPolicy: ScheduledViewPolicy{
			WriteAccess: workspacepkg.ViewWriteAccessReadWrite, AllowDirtyView: true,
		},
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
	if definition.ID == "" || definition.MissPolicy != "fire_once" || definition.MaxConcurrent != 0 {
		t.Fatalf("schedule definition = %+v", definition)
	}
	if definition.Action.TargetAgentID != registration.Binding.ToolHostID ||
		definition.Action.PayloadEncoding != "json" ||
		definition.Action.TaskType != ScheduledTurnTaskType ||
		!reflect.DeepEqual(definition.Action.Payload.Binding, registration.Binding) {
		t.Fatalf("schedule action = %+v", definition.Action)
	}
	if definition.Action.Metadata["scitrera.schedule_digest"] == "" ||
		definition.Action.Metadata["scitrera.view_revision"] != registration.Binding.Revision {
		t.Fatalf("schedule metadata = %#v", definition.Action.Metadata)
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
