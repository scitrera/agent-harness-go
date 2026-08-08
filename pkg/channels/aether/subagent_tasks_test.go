package aether

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

type fakeTaskOperations struct {
	createCalls      []sdk.CreateTaskOptions
	createErrors     []error
	createResponse   *sdk.CreateTaskResponse
	claimCalls       int
	claimResponse    *sdk.TaskOperationResponse
	claimError       error
	completeCalls    int
	completeResponse *sdk.TaskOperationResponse
	completeError    error
	failCalls        int
	failReason       string
	failResponse     *sdk.TaskOperationResponse
	failError        error
	cancelCalls      int
	cancelReason     string
	cancelResponse   *sdk.TaskOperationResponse
	cancelError      error
	queryCalls       int
	queryResponses   []*sdk.TaskQueryResponse
	queryErrors      []error
}

func (f *fakeTaskOperations) CreateTaskSync(_ context.Context, _, _ string, opts sdk.CreateTaskOptions, _ time.Duration) (*sdk.CreateTaskResponse, error) {
	f.createCalls = append(f.createCalls, opts)
	index := len(f.createCalls) - 1
	if index < len(f.createErrors) && f.createErrors[index] != nil {
		return nil, f.createErrors[index]
	}
	return f.createResponse, nil
}

func (f *fakeTaskOperations) ClaimTask(context.Context, string, time.Duration) (*sdk.TaskOperationResponse, error) {
	f.claimCalls++
	return f.claimResponse, f.claimError
}

func (f *fakeTaskOperations) CompleteTask(context.Context, string, time.Duration) (*sdk.TaskOperationResponse, error) {
	f.completeCalls++
	return f.completeResponse, f.completeError
}

func (f *fakeTaskOperations) FailTask(_ context.Context, _ string, reason string, _ time.Duration) (*sdk.TaskOperationResponse, error) {
	f.failCalls++
	f.failReason = reason
	return f.failResponse, f.failError
}

func (f *fakeTaskOperations) CancelTask(_ context.Context, _ string, reason string, _ time.Duration) (*sdk.TaskOperationResponse, error) {
	f.cancelCalls++
	f.cancelReason = reason
	return f.cancelResponse, f.cancelError
}

func (f *fakeTaskOperations) GetTask(context.Context, string, time.Duration) (*sdk.TaskQueryResponse, error) {
	index := f.queryCalls
	f.queryCalls++
	var response *sdk.TaskQueryResponse
	if index < len(f.queryResponses) {
		response = f.queryResponses[index]
	}
	var err error
	if index < len(f.queryErrors) {
		err = f.queryErrors[index]
	}
	return response, err
}

func TestSubagentTaskBackendAdmitsIdempotentOBOExecution(t *testing.T) {
	operations := &fakeTaskOperations{
		createErrors:   []error{errors.New("response lost")},
		createResponse: &sdk.CreateTaskResponse{Success: true, TaskID: "aether-child-1"},
	}
	backend, err := NewSubagentTaskBackend(operations, "routing-workspace", "agent-topic", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var boundParent, boundChild string
	backend.bindChildReply = func(parent, child string) {
		boundParent, boundChild = parent, child
	}
	admission := subagent.TaskAdmission{
		WorkspaceID: "project-a", ParentSessionID: "parent-session", ChildSessionID: "child-session",
		ParentTaskID: "parent-task", ParentMessageID: "parent-message", InvocationID: "tool-call-7",
		Name: "reviewer", Kind: "review", Model: "model-a", Depth: 2, Background: true,
		GrantID: "grant-1", SubjectType: "user", SubjectID: "alice",
	}
	taskID, err := backend.Admit(context.Background(), admission)
	if err != nil {
		t.Fatal(err)
	}
	if taskID != "aether-child-1" || boundParent != "parent-task" || boundChild != taskID {
		t.Fatalf("task=%q bound=(%q,%q)", taskID, boundParent, boundChild)
	}
	if len(operations.createCalls) != 2 {
		t.Fatalf("create calls = %d, want one retry", len(operations.createCalls))
	}
	first, second := operations.createCalls[0], operations.createCalls[1]
	if first.IdempotencyKey == "" || first.IdempotencyKey != second.IdempotencyKey {
		t.Fatalf("idempotency keys = %q / %q", first.IdempotencyKey, second.IdempotencyKey)
	}
	if first.TaskClass != pb.TaskClass_TASK_CLASS_BACKGROUND || first.ContextID == "" || first.RetryPolicy.GetMaxAttempts() != 1 {
		t.Fatalf("task policy = class:%s context:%q retry:%+v", first.TaskClass, first.ContextID, first.RetryPolicy)
	}
	if first.Authorization.GetAuthorityMode() != "on_behalf_of" || first.Authorization.GetGrantId() != "grant-1" || first.Authorization.GetSubject().GetPrincipalId() != "alice" {
		t.Fatalf("authorization = %+v", first.Authorization)
	}
	if first.Metadata["scitrera.logical_workspace"] != "project-a" || first.Metadata["scitrera.child_session_id"] != "child-session" || first.Metadata["scitrera.parent_task_id"] != "parent-task" {
		t.Fatalf("metadata = %#v", first.Metadata)
	}
	changed := admission
	changed.InvocationID = "tool-call-8"
	if taskAdmissionKey("agent-topic", "routing-workspace", admission) == taskAdmissionKey("agent-topic", "routing-workspace", changed) {
		t.Fatal("different spawn invocations must not collapse to one task")
	}
}

func TestSubagentTaskBackendRejectsPartialAuthority(t *testing.T) {
	backend, _ := NewSubagentTaskBackend(&fakeTaskOperations{}, "routing", "agent-topic", time.Second)
	_, err := backend.Admit(context.Background(), subagent.TaskAdmission{
		WorkspaceID: "project", ParentSessionID: "parent", ChildSessionID: "child", GrantID: "grant-only",
	})
	if err == nil || !strings.Contains(err.Error(), "requires grant, subject type, and subject id") {
		t.Fatalf("error = %v", err)
	}
}

func TestSubagentTaskBackendConfirmsAmbiguousLifecycleWithoutReplay(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		operations := &fakeTaskOperations{
			claimError:     errors.New("response lost"),
			queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_RUNNING.String()}}},
		}
		backend, _ := NewSubagentTaskBackend(operations, "routing", "agent-topic", time.Second)
		if err := backend.Start(context.Background(), "child-task"); err != nil {
			t.Fatal(err)
		}
		if operations.claimCalls != 1 || operations.queryCalls != 1 {
			t.Fatalf("claim=%d query=%d", operations.claimCalls, operations.queryCalls)
		}
	})

	t.Run("finish", func(t *testing.T) {
		operations := &fakeTaskOperations{
			completeError:  errors.New("response lost"),
			queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_COMPLETED.String()}}},
		}
		backend, _ := NewSubagentTaskBackend(operations, "routing", "agent-topic", time.Second)
		unbinds := 0
		backend.unbindReply = func(string) { unbinds++ }
		if err := backend.Finish(context.Background(), "child-task", subagent.TaskOutcomeCompleted, ""); err != nil {
			t.Fatal(err)
		}
		if operations.completeCalls != 1 || operations.queryCalls != 1 || unbinds != 1 {
			t.Fatalf("complete=%d query=%d unbind=%d", operations.completeCalls, operations.queryCalls, unbinds)
		}
	})

	t.Run("unconfirmed", func(t *testing.T) {
		operations := &fakeTaskOperations{
			claimError:     errors.New("response lost"),
			queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()}}},
		}
		backend, _ := NewSubagentTaskBackend(operations, "routing", "agent-topic", time.Second)
		if err := backend.Start(context.Background(), "child-task"); err == nil {
			t.Fatal("expected an unconfirmed start error")
		}
		if operations.claimCalls != 1 {
			t.Fatalf("claim mutation replayed %d times", operations.claimCalls)
		}
	})
}

func TestSubagentTaskBackendBoundsFailureAndRecoversOrphan(t *testing.T) {
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{
			{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_RUNNING.String()}},
		},
		failResponse: &sdk.TaskOperationResponse{Success: true},
	}
	backend, _ := NewSubagentTaskBackend(operations, "routing", "agent-topic", time.Second)
	recovery, err := backend.Recover(context.Background(), "child-task")
	if err != nil {
		t.Fatal(err)
	}
	if recovery != subagent.TaskRecoveryInterrupted || operations.failCalls != 1 {
		t.Fatalf("recovery=%q fail calls=%d", recovery, operations.failCalls)
	}
	if !strings.Contains(operations.failReason, "not resumable") {
		t.Fatalf("failure reason = %q", operations.failReason)
	}

	longReason := strings.Repeat("é", maxTaskFailureRunes+20)
	operations.failResponse = &sdk.TaskOperationResponse{Success: true}
	if err := backend.Finish(context.Background(), "another-task", subagent.TaskOutcomeFailed, longReason); err != nil {
		t.Fatal(err)
	}
	if len([]rune(operations.failReason)) != maxTaskFailureRunes {
		t.Fatalf("bounded reason runes = %d", len([]rune(operations.failReason)))
	}
}

func TestChannelBindsChildTaskToParentReplyLane(t *testing.T) {
	channel := &Channel{replyTo: map[string]string{"parent-task": "client-topic"}}
	channel.bindChildTaskReply("parent-task", "child-task")
	if got := channel.replyTopic(protocol.MessageAddress{TaskID: "child-task"}); got != "client-topic" {
		t.Fatalf("child reply topic = %q", got)
	}
	channel.unbindTaskReply("child-task")
	if got := channel.replyTopic(protocol.MessageAddress{TaskID: "child-task"}); got != "" {
		t.Fatalf("unbound child reply topic = %q", got)
	}
}
