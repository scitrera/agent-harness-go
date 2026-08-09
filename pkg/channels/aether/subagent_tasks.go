package aether

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

const (
	subagentTaskType       = "agent-harness.subagent.v1"
	defaultTaskTimeout     = 10 * time.Second
	maxTaskFailureRunes    = 2048
	taskAdmissionAttempts  = 2
	taskMetadataComponent  = "agent-harness"
	taskMetadataKind       = "subagent"
	taskContextHashPrefix  = "ah-session-v1-"
	taskAdmissionKeyPrefix = "ah-subagent-v1-"
)

// TaskOperations is the narrow Aether SDK surface required by the neutral
// subagent task adapter. It is exported so distributions already wrapping an
// AgentClient can reuse the same lifecycle semantics without forking them.
type TaskOperations interface {
	CreateTaskSync(ctx context.Context, taskType, workspace string, opts sdk.CreateTaskOptions, timeout time.Duration) (*sdk.CreateTaskResponse, error)
	ClaimTask(ctx context.Context, taskID string, timeout time.Duration) (*sdk.TaskOperationResponse, error)
	CompleteTask(ctx context.Context, taskID string, timeout time.Duration) (*sdk.TaskOperationResponse, error)
	FailTask(ctx context.Context, taskID, reason string, timeout time.Duration) (*sdk.TaskOperationResponse, error)
	CancelTask(ctx context.Context, taskID, reason string, timeout time.Duration) (*sdk.TaskOperationResponse, error)
	GetTask(ctx context.Context, taskID string, timeout time.Duration) (*sdk.TaskQueryResponse, error)
}

// SubagentTaskBackend maps the neutral child execution contract to real Aether
// tasks. The task is self-assigned because this harness still executes the
// child in-process; task identity nevertheless owns admission and lifecycle.
// When the triggering turn is an active Aether task, ParentTaskID asks Aether
// to validate the caller's ownership and persist native task hierarchy.
type SubagentTaskBackend struct {
	tasks          TaskOperations
	workspace      string
	namespace      string
	timeout        time.Duration
	bindChildReply func(parentTaskID, childTaskID string)
	unbindReply    func(taskID string)
}

func NewSubagentTaskBackend(tasks TaskOperations, workspace, namespace string, timeout time.Duration) (*SubagentTaskBackend, error) {
	if tasks == nil {
		return nil, errors.New("aether: subagent task operations are required")
	}
	if strings.TrimSpace(workspace) == "" {
		return nil, errors.New("aether: subagent task routing workspace is required")
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("aether: subagent task admission namespace is required")
	}
	if timeout <= 0 {
		timeout = defaultTaskTimeout
	}
	return &SubagentTaskBackend{tasks: tasks, workspace: workspace, namespace: namespace, timeout: timeout}, nil
}

// SubagentTaskBackend returns a task backend bound to this channel's Aether
// routing workspace. Logical workspaces remain explicit in task metadata and
// the parent-session projection.
func (c *Channel) SubagentTaskBackend(timeout time.Duration) (*SubagentTaskBackend, error) {
	backend, err := NewSubagentTaskBackend(c.client, c.workspace, c.client.Topic(), timeout)
	if err != nil {
		return nil, err
	}
	backend.bindChildReply = c.bindChildTaskReply
	backend.unbindReply = c.unbindTaskReply
	return backend, nil
}

func (b *SubagentTaskBackend) Admit(ctx context.Context, admission subagent.TaskAdmission) (string, error) {
	if err := validateTaskAdmission(admission); err != nil {
		return "", err
	}
	executionPayload, err := subagent.MarshalExecutionEnvelope(admission.Execution)
	if err != nil {
		return "", fmt.Errorf("aether: encode subagent execution payload: %w", err)
	}
	authorization, err := taskAuthorization(admission)
	if err != nil {
		return "", err
	}
	metadata := map[string]string{
		"scitrera.component":         taskMetadataComponent,
		"scitrera.kind":              taskMetadataKind,
		"scitrera.logical_workspace": admission.WorkspaceID,
		"scitrera.parent_session_id": admission.ParentSessionID,
		"scitrera.child_session_id":  admission.ChildSessionID,
		"scitrera.depth":             strconv.Itoa(max(admission.Depth, 0)),
		"scitrera.background":        strconv.FormatBool(admission.Background),
		"scitrera.execution_schema":  admission.Execution.Schema,
		"scitrera.execution_id":      admission.Execution.ExecutionID,
		"scitrera.policy_digest":     admission.Execution.Policy.SnapshotDigest,
	}
	addTaskMetadata(metadata, "scitrera.parent_task_id", admission.ParentTaskID)
	addTaskMetadata(metadata, "scitrera.parent_message_id", admission.ParentMessageID)
	addTaskMetadata(metadata, "scitrera.invocation_id", admission.InvocationID)
	addTaskMetadata(metadata, "scitrera.agent_name", admission.Name)
	addTaskMetadata(metadata, "scitrera.agent_kind", admission.Kind)
	addTaskMetadata(metadata, "scitrera.model", admission.Model)

	taskClass := pb.TaskClass_TASK_CLASS_INTERACTIVE
	if admission.Background {
		taskClass = pb.TaskClass_TASK_CLASS_BACKGROUND
	}
	opts := sdk.CreateTaskOptions{
		TaskType:       subagentTaskType,
		Workspace:      b.workspace,
		AssignmentMode: sdk.TaskAssignmentSelfAssign,
		ParentTaskID:   admission.ParentTaskID,
		Metadata:       metadata,
		Payload:        executionPayload,
		Authorization:  authorization,
		RetryPolicy:    &pb.RetryPolicy{MaxAttempts: 1},
		TaskClass:      taskClass,
		ContextID:      taskContextID(b.namespace, admission),
		IdempotencyKey: taskAdmissionKey(b.namespace, b.workspace, admission),
	}

	var response *sdk.CreateTaskResponse
	for attempt := 0; attempt < taskAdmissionAttempts; attempt++ {
		response, err = b.tasks.CreateTaskSync(ctx, subagentTaskType, b.workspace, opts, b.timeout)
		if err == nil || ctx.Err() != nil {
			break
		}
		// Admission is the only mutation retried: the stable Aether idempotency
		// key makes the second request resolve to the original task if the first
		// response was lost. Start/Finish never blindly repeat a mutation.
	}
	if err != nil {
		return "", fmt.Errorf("aether: create subagent task: %w", err)
	}
	if response == nil {
		return "", errors.New("aether: create subagent task returned no response")
	}
	if !response.Success {
		return "", fmt.Errorf("aether: create subagent task rejected (%s): %s", response.ErrorCode, response.ErrorMessage)
	}
	if strings.TrimSpace(response.TaskID) == "" {
		return "", errors.New("aether: create subagent task returned an empty task id")
	}
	if b.bindChildReply != nil {
		b.bindChildReply(admission.ParentTaskID, response.TaskID)
	}
	return response.TaskID, nil
}

func (b *SubagentTaskBackend) Start(ctx context.Context, taskID string) error {
	return b.confirmOperation(ctx, taskID, pb.TaskStatus_TASK_STATUS_RUNNING.String(), func() (*sdk.TaskOperationResponse, error) {
		return b.tasks.ClaimTask(ctx, taskID, b.timeout)
	})
}

func (b *SubagentTaskBackend) Finish(ctx context.Context, taskID string, outcome subagent.TaskOutcome, reason string) error {
	if b.unbindReply != nil {
		defer b.unbindReply(taskID)
	}
	var desired string
	var operation func() (*sdk.TaskOperationResponse, error)
	switch outcome {
	case subagent.TaskOutcomeCompleted:
		desired = pb.TaskStatus_TASK_STATUS_COMPLETED.String()
		operation = func() (*sdk.TaskOperationResponse, error) {
			return b.tasks.CompleteTask(ctx, taskID, b.timeout)
		}
	case subagent.TaskOutcomeFailed:
		desired = pb.TaskStatus_TASK_STATUS_FAILED.String()
		reason = boundedTaskReason(reason)
		operation = func() (*sdk.TaskOperationResponse, error) {
			return b.tasks.FailTask(ctx, taskID, reason, b.timeout)
		}
	case subagent.TaskOutcomeCancelled:
		desired = pb.TaskStatus_TASK_STATUS_CANCELLED.String()
		reason = boundedTaskReason(reason)
		operation = func() (*sdk.TaskOperationResponse, error) {
			return b.tasks.CancelTask(ctx, taskID, reason, b.timeout)
		}
	default:
		return fmt.Errorf("aether: unsupported subagent task outcome %q", outcome)
	}
	return b.confirmOperation(ctx, taskID, desired, operation)
}

func (b *SubagentTaskBackend) Recover(ctx context.Context, taskID string) (subagent.TaskRecovery, error) {
	query, err := b.tasks.GetTask(ctx, taskID, b.timeout)
	if err != nil {
		return "", fmt.Errorf("aether: get subagent task: %w", err)
	}
	if query == nil || !query.Success || query.Task == nil {
		message := "task query returned no task"
		if query != nil && query.Error != "" {
			message = query.Error
		}
		return "", fmt.Errorf("aether: get subagent task %q: %s", taskID, message)
	}
	switch query.Task.Status {
	case pb.TaskStatus_TASK_STATUS_COMPLETED.String():
		return subagent.TaskRecoveryCompleted, nil
	case pb.TaskStatus_TASK_STATUS_FAILED.String(), pb.TaskStatus_TASK_STATUS_REJECTED.String():
		return subagent.TaskRecoveryFailed, nil
	case pb.TaskStatus_TASK_STATUS_CANCELLED.String():
		return subagent.TaskRecoveryCancelled, nil
	case pb.TaskStatus_TASK_STATUS_QUEUED.String(),
		pb.TaskStatus_TASK_STATUS_RUNNING.String(),
		pb.TaskStatus_TASK_STATUS_WAITING_INPUT.String(),
		pb.TaskStatus_TASK_STATUS_WAITING_AUTHORITY.String(),
		pb.TaskStatus_TASK_STATUS_WAITING_DEPENDENCY.String(),
		pb.TaskStatus_TASK_STATUS_HIBERNATED.String():
		if err := b.Finish(ctx, taskID, subagent.TaskOutcomeFailed, "agent-harness owner restarted; in-process subagent execution is not resumable"); err != nil {
			return "", fmt.Errorf("aether: terminate orphaned subagent task: %w", err)
		}
		return subagent.TaskRecoveryInterrupted, nil
	default:
		return "", fmt.Errorf("aether: subagent task %q has unsupported status %q", taskID, query.Task.Status)
	}
}

// confirmOperation never replays a lifecycle mutation after an ambiguous
// response. It performs one authoritative GET and accepts only the requested
// state, leaving the caller to project interruption when state is still unknown.
func (b *SubagentTaskBackend) confirmOperation(ctx context.Context, taskID, desired string, operation func() (*sdk.TaskOperationResponse, error)) error {
	if strings.TrimSpace(taskID) == "" {
		return errors.New("aether: subagent task id is required")
	}
	response, opErr := operation()
	if opErr == nil && response != nil && response.Success {
		return nil
	}
	query, queryErr := b.tasks.GetTask(ctx, taskID, b.timeout)
	if queryErr == nil && query != nil && query.Success && query.Task != nil && query.Task.Status == desired {
		return nil
	}
	if opErr != nil {
		if queryErr != nil {
			return errors.Join(fmt.Errorf("aether: task operation: %w", opErr), fmt.Errorf("aether: confirm task state: %w", queryErr))
		}
		return fmt.Errorf("aether: task operation: %w (confirmed status %q, wanted %q)", opErr, taskStatus(query), desired)
	}
	operationError := "task operation returned no response"
	if response != nil {
		operationError = response.Error
		if operationError == "" {
			operationError = response.Message
		}
	}
	if queryErr != nil {
		return errors.Join(errors.New("aether: "+operationError), fmt.Errorf("aether: confirm task state: %w", queryErr))
	}
	return fmt.Errorf("aether: %s (confirmed status %q, wanted %q)", operationError, taskStatus(query), desired)
}

func validateTaskAdmission(admission subagent.TaskAdmission) error {
	switch {
	case strings.TrimSpace(admission.WorkspaceID) == "":
		return errors.New("aether: subagent logical workspace is required")
	case strings.TrimSpace(admission.ParentSessionID) == "":
		return errors.New("aether: subagent parent session is required")
	case strings.TrimSpace(admission.ChildSessionID) == "":
		return errors.New("aether: subagent child session is required")
	}
	if err := admission.Execution.Validate(); err != nil {
		return fmt.Errorf("aether: invalid subagent execution envelope: %w", err)
	}
	if admission.Execution.WorkspaceID != admission.WorkspaceID ||
		admission.Execution.ParentSessionID != admission.ParentSessionID ||
		admission.Execution.ChildSessionID != admission.ChildSessionID ||
		admission.Execution.ParentTaskID != admission.ParentTaskID ||
		admission.Execution.ParentMessageID != admission.ParentMessageID ||
		admission.Execution.InvocationID != admission.InvocationID ||
		admission.Execution.Background != admission.Background {
		return errors.New("aether: subagent execution envelope identity does not match admission")
	}
	executionName := admission.Execution.Policy.AgentName
	if executionName == "" {
		executionName = admission.Execution.Policy.AgentType
	}
	if executionName == "" {
		executionName = "subagent"
	}
	if executionName != admission.Name ||
		admission.Execution.Policy.AgentType != admission.Kind ||
		admission.Execution.Policy.Model != strings.TrimSpace(admission.Model) {
		return errors.New("aether: subagent execution policy does not match admission")
	}
	return nil
}

func taskAuthorization(admission subagent.TaskAdmission) (*pb.AuthorizationContext, error) {
	set := 0
	for _, value := range []string{admission.GrantID, admission.SubjectType, admission.SubjectID} {
		if strings.TrimSpace(value) != "" {
			set++
		}
	}
	if set == 0 {
		return nil, nil
	}
	if set != 3 {
		return nil, errors.New("aether: subagent task authority requires grant, subject type, and subject id together")
	}
	return &pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of",
		GrantId:       admission.GrantID,
		Subject: &pb.PrincipalRef{
			PrincipalType: admission.SubjectType,
			PrincipalId:   admission.SubjectID,
		},
	}, nil
}

func taskContextID(namespace string, admission subagent.TaskAdmission) string {
	return taskContextHashPrefix + hashTaskIdentity(namespace, admission.WorkspaceID, admission.ParentSessionID)
}

func taskAdmissionKey(namespace, routingWorkspace string, admission subagent.TaskAdmission) string {
	if admission.Execution.ExecutionID != "" {
		return taskAdmissionKeyPrefix + hashTaskIdentity(namespace, routingWorkspace, admission.Execution.ExecutionID)
	}
	return taskAdmissionKeyPrefix + hashTaskIdentity(
		namespace,
		routingWorkspace,
		admission.WorkspaceID,
		admission.ParentSessionID,
		admission.ChildSessionID,
		admission.ParentTaskID,
		admission.ParentMessageID,
		admission.InvocationID,
	)
}

func hashTaskIdentity(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func addTaskMetadata(metadata map[string]string, key, value string) {
	if value != "" {
		metadata[key] = value
	}
}

func boundedTaskReason(reason string) string {
	runes := []rune(reason)
	if len(runes) <= maxTaskFailureRunes {
		return reason
	}
	return string(runes[:maxTaskFailureRunes])
}

func taskStatus(query *sdk.TaskQueryResponse) string {
	if query == nil || query.Task == nil {
		return ""
	}
	return query.Task.Status
}

func (c *Channel) bindChildTaskReply(parentTaskID, childTaskID string) {
	if parentTaskID == "" || childTaskID == "" {
		return
	}
	c.mu.Lock()
	if topic := c.replyTo[parentTaskID]; topic != "" {
		c.replyTo[childTaskID] = topic
	}
	c.mu.Unlock()
}

func (c *Channel) unbindTaskReply(taskID string) {
	if taskID == "" {
		return
	}
	c.mu.Lock()
	delete(c.replyTo, taskID)
	c.mu.Unlock()
}

var _ subagent.TaskBackend = (*SubagentTaskBackend)(nil)
