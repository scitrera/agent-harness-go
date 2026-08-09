package aether

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingAssignedRunner struct {
	calls    int
	taskID   string
	envelope subagent.ExecutionEnvelope
	req      subagent.Request
	err      error
}

func (r *recordingAssignedRunner) ExecuteAssignedSubagent(_ context.Context, taskID string, envelope subagent.ExecutionEnvelope, req subagent.Request) (subagent.Result, error) {
	r.calls++
	r.taskID = taskID
	r.envelope = envelope
	r.req = req
	return subagent.Result{Text: "done", ThreadID: envelope.ChildSessionID}, r.err
}

type staticCatalog struct {
	definition subagent.Definition
}

type recordingAssignedResolver struct {
	runners    map[string]subagent.AssignedRunner
	workspaces []string
	authority  []tools.MemoryAuthority
}

func (r *recordingAssignedResolver) ResolveAssignedExecution(ctx context.Context, workspaceID string) (subagent.AssignedExecutionResources, error) {
	r.workspaces = append(r.workspaces, workspaceID)
	auth, _ := tools.MemoryAuthorityFrom(ctx)
	r.authority = append(r.authority, auth)
	return subagent.AssignedExecutionResources{Runner: r.runners[workspaceID]}, nil
}

func (c staticCatalog) List(context.Context) ([]subagent.Definition, error) {
	return []subagent.Definition{c.definition}, nil
}

func (c staticCatalog) Get(_ context.Context, typ subagent.AgentType) (subagent.Definition, error) {
	if c.definition.Type != typ {
		return subagent.Definition{}, subagent.ErrUnknownAgent
	}
	return c.definition, nil
}

func externalAssignmentFixture(t *testing.T, req subagent.Request) *sdk.TaskAssignment {
	t.Helper()
	envelope, err := subagent.NewExecutionEnvelope(req, req.Parent.WorkspaceID, "child-session", req.Background)
	if err != nil {
		t.Fatal(err)
	}
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{Success: false, Error: "task not found or not authorized"}},
		createResponse: &sdk.CreateTaskResponse{Success: true, TaskID: "child-task"},
	}
	backend, err := NewSubagentTaskBackendWithConfig(operations, SubagentTaskBackendConfig{
		RoutingWorkspace: "routing", Namespace: "parent-agent", TargetAgentID: "ag::routing::agent-harness::worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	name := string(req.AgentName)
	if name == "" {
		name = string(req.AgentType)
	}
	if name == "" {
		name = "subagent"
	}
	_, err = backend.Admit(context.Background(), subagent.TaskAdmission{
		WorkspaceID: req.Parent.WorkspaceID, ParentSessionID: req.Parent.ThreadID, ChildSessionID: envelope.ChildSessionID,
		ParentTaskID: req.Parent.TaskID, ParentMessageID: req.ParentMessageID, InvocationID: req.InvocationID,
		Name: name, Kind: string(req.AgentType), Model: req.Model, Depth: req.Depth, Background: req.Background,
		GrantID: req.GrantID, SubjectType: req.SubjectType, SubjectID: req.SubjectID, Execution: envelope,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := operations.createCalls[0]
	return &sdk.TaskAssignment{
		TaskID: "child-task", TaskType: subagentTaskType,
		AssignedTo: "ag::routing::agent-harness::worker",
		Metadata:   opts.Metadata, Payload: opts.Payload, Authorization: opts.Authorization,
	}
}

func genericExternalRequest() subagent.Request {
	return subagent.Request{
		Depth: 2,
		Parent: protocol.MessageAddress{
			WorkspaceID: "project-a", ThreadID: "parent-session", TaskID: "parent-task",
		},
		Task: "inspect private code", InvocationID: "tool-call-7", ParentMessageID: "parent-message",
		Model: "model-a", GrantID: "task-grant", SubjectType: "user", SubjectID: "alice",
	}
}

func TestAssignedSubagentExecutorClaimsRunsAndCompletesWithTypedAuthority(t *testing.T) {
	assignment := externalAssignmentFixture(t, genericExternalRequest())
	// A server metadata mirror is not a trust source. The typed assignment must
	// remain authoritative even when an arbitrary key says otherwise.
	assignment.Metadata["scitrera.authority_grant_id"] = "forged-grant"
	operations := &fakeTaskOperations{
		queryResponses:   []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()}}},
		claimResponse:    &sdk.TaskOperationResponse{Success: true},
		completeResponse: &sdk.TaskOperationResponse{Success: true},
	}
	runner := &recordingAssignedRunner{}
	executor, err := NewAssignedSubagentExecutor(operations, assignment.AssignedTo, SubagentExecutorConfig{Runner: runner, MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.HandleAssignment(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if operations.claimCalls != 1 || operations.completeCalls != 1 || operations.failCalls != 0 {
		t.Fatalf("claim=%d complete=%d fail=%d", operations.claimCalls, operations.completeCalls, operations.failCalls)
	}
	if runner.calls != 1 || runner.taskID != assignment.TaskID || runner.req.Task != "" {
		t.Fatalf("runner calls=%d task=%q request task=%q", runner.calls, runner.taskID, runner.req.Task)
	}
	if runner.req.GrantID != "task-grant" || runner.req.SubjectType != "user" || runner.req.SubjectID != "alice" || runner.req.Depth != 2 {
		t.Fatalf("assigned authority/depth = %+v", runner.req)
	}
}

func TestAssignedSubagentExecutorResolvesRunnerByValidatedWorkspace(t *testing.T) {
	first := externalAssignmentFixture(t, genericExternalRequest())
	secondReq := genericExternalRequest()
	secondReq.Parent.WorkspaceID = "project-b"
	second := externalAssignmentFixture(t, secondReq)
	second.TaskID = "child-task-b"
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{
			{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()}},
			{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()}},
		},
		claimResponse:    &sdk.TaskOperationResponse{Success: true},
		completeResponse: &sdk.TaskOperationResponse{Success: true},
	}
	projectA := &recordingAssignedRunner{}
	projectB := &recordingAssignedRunner{}
	resolver := &recordingAssignedResolver{runners: map[string]subagent.AssignedRunner{
		"project-a": projectA,
		"project-b": projectB,
	}}
	executor, err := NewAssignedSubagentExecutor(operations, first.AssignedTo, SubagentExecutorConfig{Resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.HandleAssignment(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := executor.HandleAssignment(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if projectA.calls != 1 || projectB.calls != 1 {
		t.Fatalf("workspace runners called project-a=%d project-b=%d", projectA.calls, projectB.calls)
	}
	if len(resolver.workspaces) != 2 || resolver.workspaces[0] != "project-a" || resolver.workspaces[1] != "project-b" {
		t.Fatalf("resolved workspaces = %#v", resolver.workspaces)
	}
	for _, auth := range resolver.authority {
		if auth.GrantID != "task-grant" || auth.SubjectType != "user" || auth.SubjectID != "alice" {
			t.Fatalf("resolver authority = %+v", auth)
		}
	}
}

func TestAssignedSubagentExecutorRejectsAmbiguousStaticAndResolvedResources(t *testing.T) {
	_, err := NewAssignedSubagentExecutor(&fakeTaskOperations{}, "agent", SubagentExecutorConfig{
		Runner: &recordingAssignedRunner{}, Resolver: &recordingAssignedResolver{},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("error = %v", err)
	}
}

func TestAssignedSubagentExecutorFailsClosedOnCatalogDrift(t *testing.T) {
	req := genericExternalRequest()
	req.AgentName = "reviewer"
	req.AgentType = "review"
	req.Instructions = "original private prompt"
	assignment := externalAssignmentFixture(t, req)
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()}}},
		failResponse:   &sdk.TaskOperationResponse{Success: true},
	}
	runner := &recordingAssignedRunner{}
	executor, err := NewAssignedSubagentExecutor(operations, assignment.AssignedTo, SubagentExecutorConfig{
		Runner: runner,
		Catalog: staticCatalog{definition: subagent.Definition{
			Name: "reviewer", Type: "review", Description: "review", Prompt: "changed private prompt",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = executor.HandleAssignment(context.Background(), assignment)
	if err == nil || !strings.Contains(err.Error(), "policy snapshot mismatch") {
		t.Fatalf("error = %v", err)
	}
	if runner.calls != 0 || operations.claimCalls != 0 || operations.failCalls != 1 {
		t.Fatalf("runner=%d claim=%d fail=%d", runner.calls, operations.claimCalls, operations.failCalls)
	}
}

func TestAssignedSubagentExecutorNeverReplaysRunningDelivery(t *testing.T) {
	assignment := externalAssignmentFixture(t, genericExternalRequest())
	operations := &fakeTaskOperations{
		queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_RUNNING.String()}}},
		failResponse:   &sdk.TaskOperationResponse{Success: true},
	}
	runner := &recordingAssignedRunner{}
	executor, _ := NewAssignedSubagentExecutor(operations, assignment.AssignedTo, SubagentExecutorConfig{Runner: runner})
	err := executor.HandleAssignment(context.Background(), assignment)
	if err == nil || !strings.Contains(err.Error(), "refusing uncertain replay") {
		t.Fatalf("error = %v", err)
	}
	if runner.calls != 0 || operations.claimCalls != 0 || operations.failCalls != 1 {
		t.Fatalf("runner=%d claim=%d fail=%d", runner.calls, operations.claimCalls, operations.failCalls)
	}
}

func TestAssignedSubagentExecutorRejectsMetadataAndAuthorityMismatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*sdk.TaskAssignment)
		want   string
	}{
		{name: "workspace", mutate: func(a *sdk.TaskAssignment) { a.Metadata["scitrera.logical_workspace"] = "project-b" }, want: "logical_workspace"},
		{name: "partial authority", mutate: func(a *sdk.TaskAssignment) {
			a.Authorization = &pb.AuthorizationContext{AuthorityMode: "on_behalf_of", GrantId: "grant-only"}
		}, want: "requires grant"},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment := externalAssignmentFixture(t, genericExternalRequest())
			test.mutate(assignment)
			operations := &fakeTaskOperations{
				queryResponses: []*sdk.TaskQueryResponse{{Success: true, Task: &sdk.TaskInfo{Status: pb.TaskStatus_TASK_STATUS_QUEUED.String()}}},
				failResponse:   &sdk.TaskOperationResponse{Success: true},
			}
			runner := &recordingAssignedRunner{}
			executor, _ := NewAssignedSubagentExecutor(operations, assignment.AssignedTo, SubagentExecutorConfig{Runner: runner})
			err := executor.HandleAssignment(context.Background(), assignment)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v", err)
			}
			if runner.calls != 0 || operations.claimCalls != 0 || operations.failCalls != 1 {
				t.Fatalf("runner=%d claim=%d fail=%d", runner.calls, operations.claimCalls, operations.failCalls)
			}
		})
	}
}

func TestAssignedSubagentExecutorIgnoresOtherTaskTypes(t *testing.T) {
	operations := &fakeTaskOperations{}
	runner := &recordingAssignedRunner{err: errors.New("must not run")}
	executor, _ := NewAssignedSubagentExecutor(operations, "agent", SubagentExecutorConfig{Runner: runner})
	if err := executor.HandleAssignment(context.Background(), &sdk.TaskAssignment{TaskType: "other"}); err != nil {
		t.Fatal(err)
	}
	if runner.calls != 0 || operations.queryCalls != 0 {
		t.Fatalf("runner=%d query=%d", runner.calls, operations.queryCalls)
	}
}

func TestAssignedSubagentExecutorDoesNotMutateMisroutedTask(t *testing.T) {
	assignment := externalAssignmentFixture(t, genericExternalRequest())
	operations := &fakeTaskOperations{}
	runner := &recordingAssignedRunner{}
	executor, _ := NewAssignedSubagentExecutor(operations, "ag::routing::agent-harness::other", SubagentExecutorConfig{Runner: runner})
	err := executor.HandleAssignment(context.Background(), assignment)
	if err == nil || !strings.Contains(err.Error(), "assigned to") {
		t.Fatalf("error = %v", err)
	}
	if runner.calls != 0 || operations.queryCalls != 0 || operations.failCalls != 0 {
		t.Fatalf("runner=%d query=%d fail=%d", runner.calls, operations.queryCalls, operations.failCalls)
	}
}

var _ subagent.AssignedRunner = (*recordingAssignedRunner)(nil)
