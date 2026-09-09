// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/goal"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingContinuationEnqueuer struct {
	inbound []channel.Inbound
	err     error
}

func (e *recordingContinuationEnqueuer) Enqueue(_ context.Context, inbound channel.Inbound) error {
	if e.err != nil {
		return e.err
	}
	e.inbound = append(e.inbound, inbound)
	return nil
}

func aetherContinuationAdmission(t *testing.T) goal.ContinuationAdmission {
	t.Helper()
	addr := protocol.MessageAddress{
		WorkspaceID: "project-a", ThreadID: "session-1", RequestID: "request-1",
	}
	part, err := protocol.NewTextPart("continue the goal")
	if err != nil {
		t.Fatal(err)
	}
	return goal.ContinuationAdmission{
		WorkspaceID: "project-a", SessionID: "session-1", GoalID: "goal-1",
		ParentTaskID: "parent-task", ParentMessageID: "assistant-1", Attempt: 1,
		Inbound: channel.Inbound{Addr: addr, Message: protocol.ChatMessage{
			ID: "continuation-1", Role: protocol.RoleUser, Addr: addr,
			Content: []protocol.ContentPart{part},
		}},
		Authority: tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"},
	}
}

func TestContinuationTaskBackendAdmitsIdempotentCredentialFreeTask(t *testing.T) {
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{
			TaskID: "parent-task", Status: pb.TaskStatus_TASK_STATUS_RUNNING.String(), AssignedTo: "agent-topic",
		}}},
		createErrors:   []error{errors.New("response lost")},
		createResponse: &sdk.CreateTaskResponse{Success: true, TaskID: "continuation-task-1"},
	}
	backend, err := NewContinuationTaskBackend(operations, "routing", "agent-topic", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := backend.Admit(context.Background(), aetherContinuationAdmission(t))
	if err != nil {
		t.Fatal(err)
	}
	if receipt != (goal.ContinuationReceipt{Backend: "aether", TaskID: "continuation-task-1"}) {
		t.Fatalf("receipt = %#v", receipt)
	}
	if len(operations.createCalls) != 2 {
		t.Fatalf("create calls = %d", len(operations.createCalls))
	}
	first, second := operations.createCalls[0], operations.createCalls[1]
	if first.IdempotencyKey == "" || first.IdempotencyKey != second.IdempotencyKey ||
		first.TaskType != goalContinuationTaskType || first.AssignmentMode != sdk.TaskAssignmentTargeted ||
		first.TargetAgentID != "agent-topic" ||
		first.ParentTaskID != "parent-task" || first.RetryPolicy.GetMaxAttempts() != 1 {
		t.Fatalf("create options = %#v / %#v", first, second)
	}
	if first.Authorization.GetAuthorityMode() != "on_behalf_of" || first.Authorization.GetGrantId() != "grant-1" ||
		first.Authorization.GetSubject().GetPrincipalId() != "alice" {
		t.Fatalf("authorization = %#v", first.Authorization)
	}
	if bytes.Contains(first.Payload, []byte("grant-1")) || bytes.Contains(first.Payload, []byte("alice")) ||
		bytes.Contains(first.Payload, []byte("authority_handoff")) {
		t.Fatalf("authority leaked into payload: %s", first.Payload)
	}
	if first.Metadata["scitrera.logical_workspace"] != "project-a" ||
		first.Metadata["scitrera.goal_id"] != "goal-1" ||
		first.Metadata["scitrera.continuation_schema"] != goal.ContinuationEnvelopeSchema {
		t.Fatalf("metadata = %#v", first.Metadata)
	}
}

func TestAssignedContinuationExecutorEnqueuesOnceWithRealTaskAndTypedAuthority(t *testing.T) {
	creatorOps := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{Success: false}},
		createResponse: &sdk.CreateTaskResponse{Success: true, TaskID: "continuation-task-1"},
	}
	backend, _ := NewContinuationTaskBackend(creatorOps, "routing", "agent-topic", time.Second)
	admission := aetherContinuationAdmission(t)
	if _, err := backend.Admit(context.Background(), admission); err != nil {
		t.Fatal(err)
	}
	opts := creatorOps.createCalls[0]
	assignment := &sdk.TaskAssignment{
		TaskID: "continuation-task-1", TaskType: goalContinuationTaskType, AssignedTo: "agent-topic",
		Metadata: opts.Metadata, Payload: opts.Payload, Authorization: opts.Authorization,
	}
	operations := &fakeTaskOperations{queryResponses: []*sdk.TaskQueryResponse{{
		Success: true, Task: &sdk.TaskInfo{TaskID: assignment.TaskID, Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()},
	}}}
	enqueuer := &recordingContinuationEnqueuer{}
	handoff := authhandoff.New()
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: enqueuer, AuthHandoff: handoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.HandleAssignment(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if err := executor.HandleAssignment(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if len(enqueuer.inbound) != 1 || operations.queryCalls != 1 {
		t.Fatalf("enqueued=%d queries=%d", len(enqueuer.inbound), operations.queryCalls)
	}
	inbound := enqueuer.inbound[0]
	if inbound.Addr.TaskID != assignment.TaskID || inbound.Message.Addr.TaskID != assignment.TaskID {
		t.Fatalf("inbound address = %#v / %#v", inbound.Addr, inbound.Message.Addr)
	}
	authority, ok := handoff.ResolveMessage(inbound.Message)
	if !ok || authority != admission.Authority {
		t.Fatalf("authority = %#v ok=%v", authority, ok)
	}
}

func TestAssignedContinuationExecutorLeavesRunningTaskForTurnRecovery(t *testing.T) {
	operations := &fakeTaskOperations{queryResponses: []*sdk.TaskQueryResponse{{
		Success: true, Task: &sdk.TaskInfo{TaskID: "continuation-task-1", Status: pb.TaskStatus_TASK_STATUS_RUNNING.String()},
	}}}
	enqueuer := &recordingContinuationEnqueuer{}
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: enqueuer, AuthHandoff: authhandoff.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.HandleAssignment(context.Background(), &sdk.TaskAssignment{
		TaskID: "continuation-task-1", TaskType: goalContinuationTaskType, AssignedTo: "agent-topic",
	}); err != nil {
		t.Fatal(err)
	}
	if len(enqueuer.inbound) != 0 || operations.failCalls != 0 {
		t.Fatalf("running task enqueued=%d failed=%d", len(enqueuer.inbound), operations.failCalls)
	}
}

func TestAssignedContinuationExecutorRecoversQueuedTaskFromLedger(t *testing.T) {
	ctx := context.Background()
	admission := aetherContinuationAdmission(t)
	creatorOps := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{Success: false}},
		createResponse: &sdk.CreateTaskResponse{Success: true, TaskID: "continuation-task-1"},
	}
	backend, err := NewContinuationTaskBackend(creatorOps, "routing", "agent-topic", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Admit(ctx, admission); err != nil {
		t.Fatal(err)
	}
	created := creatorOps.createCalls[0]
	envelope, err := goal.NewContinuationEnvelope(admission)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := goal.NewFileLedger(t.TempDir(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, admitted, err := ledger.AppendFirstDecision(ctx, admission.WorkspaceID, admission.SessionID, goal.DecisionRecord{
		GoalID: admission.GoalID, TurnMessageID: admission.ParentMessageID,
		Action: goal.DecisionContinuationPlanned, Reason: goal.ReasonActiveGoal,
		ContinuationMessageID: admission.Inbound.Message.ID,
		ContinuationRequestID: admission.Inbound.Addr.RequestID,
		Continuation:          &envelope,
	}); err != nil || !admitted {
		t.Fatalf("append plan admitted=%v err=%v", admitted, err)
	}
	info := &sdk.TaskInfo{
		TaskID: "continuation-task-1", TaskType: goalContinuationTaskType,
		Status: pb.TaskStatus_TASK_STATUS_QUEUED.String(), Workspace: "routing", AssignedTo: "agent-topic",
		Metadata: created.Metadata, CreatedAt: time.Now().Unix(),
		AuthorityMode: "on_behalf_of", AuthorityGrantID: admission.Authority.GrantID,
		SubjectType: admission.Authority.SubjectType, SubjectID: admission.Authority.SubjectID,
	}
	operations := &fakeTaskOperations{
		listResponses:  []*sdk.TaskQueryResponse{{Success: true, Tasks: []*sdk.TaskInfo{info}, TotalCount: 1}},
		queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: info}},
	}
	enqueuer := &recordingContinuationEnqueuer{}
	handoff := authhandoff.New()
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: enqueuer, AuthHandoff: handoff, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.RecoverQueued(ctx); err != nil {
		t.Fatal(err)
	}
	if len(operations.listCalls) != 1 || operations.listCalls[0].GetWorkspace() != "routing" ||
		operations.listCalls[0].GetTaskType() != goalContinuationTaskType ||
		len(operations.listCalls[0].GetStatuses()) != 0 {
		t.Fatalf("recovery filter = %#v", operations.listCalls)
	}
	if len(enqueuer.inbound) != 1 || enqueuer.inbound[0].Addr.TaskID != info.TaskID {
		t.Fatalf("recovered inbound = %#v", enqueuer.inbound)
	}
	authority, ok := handoff.ResolveMessage(enqueuer.inbound[0].Message)
	if !ok || authority != admission.Authority {
		t.Fatalf("recovered authority = %#v ok=%v", authority, ok)
	}
}

func TestAssignedContinuationExecutorRecoveryPaginatesTaskTypeScan(t *testing.T) {
	firstPage := make([]*sdk.TaskInfo, goalContinuationRecoveryPage)
	for i := range firstPage {
		firstPage[i] = &sdk.TaskInfo{
			TaskID: "completed-task", TaskType: goalContinuationTaskType,
			Status: pb.TaskStatus_TASK_STATUS_COMPLETED.String(), AssignedTo: "agent-topic",
		}
	}
	operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{
		{Success: true, Tasks: firstPage, TotalCount: goalContinuationRecoveryPage, NextPageToken: "opaque-page-2"},
		{Success: true},
	}}
	ledger, err := goal.NewFileLedger(t.TempDir(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: &recordingContinuationEnqueuer{}, AuthHandoff: authhandoff.New(), Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.RecoverQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(operations.listCalls) != 2 || operations.listCalls[0].GetOffset() != 0 ||
		operations.listCalls[0].GetPageToken() != "" ||
		operations.listCalls[1].GetOffset() != goalContinuationRecoveryPage ||
		operations.listCalls[1].GetPageToken() != "opaque-page-2" {
		t.Fatalf("recovery pages = %#v", operations.listCalls)
	}
}

func TestAssignedContinuationExecutorRecoveryFallsBackToBoundedOffsets(t *testing.T) {
	firstPage := make([]*sdk.TaskInfo, goalContinuationRecoveryPage)
	for i := range firstPage {
		firstPage[i] = &sdk.TaskInfo{
			TaskID: "completed-task", TaskType: goalContinuationTaskType,
			Status: pb.TaskStatus_TASK_STATUS_COMPLETED.String(), AssignedTo: "agent-topic",
		}
	}
	operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{
		{Success: true, Tasks: firstPage, TotalCount: goalContinuationRecoveryPage},
		{Success: true},
	}}
	ledger, err := goal.NewFileLedger(t.TempDir(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: &recordingContinuationEnqueuer{}, AuthHandoff: authhandoff.New(), Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.RecoverQueued(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(operations.listCalls) != 2 || operations.listCalls[0].GetPageToken() != "" ||
		operations.listCalls[1].GetPageToken() != "" ||
		operations.listCalls[1].GetOffset() != goalContinuationRecoveryPage {
		t.Fatalf("cursorless recovery pages = %#v", operations.listCalls)
	}
}

func TestAssignedContinuationExecutorRecoveryBoundsCursorlessFullPages(t *testing.T) {
	fullPage := make([]*sdk.TaskInfo, goalContinuationRecoveryPage)
	for i := range fullPage {
		fullPage[i] = &sdk.TaskInfo{
			TaskID: "completed-task", TaskType: goalContinuationTaskType,
			Status: pb.TaskStatus_TASK_STATUS_COMPLETED.String(), AssignedTo: "agent-topic",
		}
	}
	fullResponse := &sdk.TaskQueryResponse{Success: true, Tasks: fullPage}
	responses := make([]*sdk.TaskQueryResponse, goalContinuationMaxOffsetPages)
	for i := range responses {
		responses[i] = fullResponse
	}
	operations := &fakeTaskOperations{listResponses: responses}
	ledger, err := goal.NewFileLedger(t.TempDir(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: &recordingContinuationEnqueuer{}, AuthHandoff: authhandoff.New(), Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = executor.RecoverQueued(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cursorless full pages") {
		t.Fatalf("RecoverQueued error = %v", err)
	}
	if len(operations.listCalls) != goalContinuationMaxOffsetPages {
		t.Fatalf("cursorless recovery calls = %d, want %d", len(operations.listCalls), goalContinuationMaxOffsetPages)
	}
}

func TestAssignedContinuationExecutorRecoveryRejectsNonAdvancingCursor(t *testing.T) {
	firstPage := make([]*sdk.TaskInfo, goalContinuationRecoveryPage)
	for i := range firstPage {
		firstPage[i] = &sdk.TaskInfo{
			TaskID: "completed-task", TaskType: goalContinuationTaskType,
			Status: pb.TaskStatus_TASK_STATUS_COMPLETED.String(), AssignedTo: "agent-topic",
		}
	}
	operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{
		{Success: true, Tasks: firstPage, NextPageToken: "repeated-cursor"},
		{Success: true, Tasks: firstPage, NextPageToken: "repeated-cursor"},
	}}
	ledger, err := goal.NewFileLedger(t.TempDir(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewAssignedContinuationExecutor(operations, "routing", "agent-topic", ContinuationExecutorConfig{
		Enqueuer: &recordingContinuationEnqueuer{}, AuthHandoff: authhandoff.New(), Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = executor.RecoverQueued(context.Background())
	if err == nil || !strings.Contains(err.Error(), "non-advancing page cursor") {
		t.Fatalf("RecoverQueued error = %v", err)
	}
}
