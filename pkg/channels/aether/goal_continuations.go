// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/goal"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	goalContinuationTaskType       = "agent-harness.goal-continuation.v1"
	goalContinuationBackendName    = "aether"
	goalContinuationKeyPrefix      = "ah-goal-continuation-v1-"
	goalContinuationContextPrefix  = "ah-goal-session-v1-"
	goalContinuationMetadataKind   = "goal_continuation"
	goalContinuationRecoveryPage   = 100
	goalContinuationMaxOffsetPages = 1000
)

type continuationTaskQueries interface {
	QueryTasks(ctx context.Context, filter *pb.TaskFilter, timeout time.Duration) (*sdk.TaskQueryResponse, error)
}

// ContinuationTaskBackend maps the neutral goal admission contract to a
// Aether task targeted to the admitting worker's stable identity. The task
// payload is credential-free; delegated
// authority is carried only in Aether's typed AuthorizationContext.
type ContinuationTaskBackend struct {
	tasks     TaskOperations
	workspace string
	namespace string
	timeout   time.Duration
}

func NewContinuationTaskBackend(tasks TaskOperations, workspace, namespace string, timeout time.Duration) (*ContinuationTaskBackend, error) {
	if tasks == nil {
		return nil, errors.New("aether: continuation task operations are required")
	}
	if strings.TrimSpace(workspace) == "" {
		return nil, errors.New("aether: continuation routing workspace is required")
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("aether: continuation admission namespace is required")
	}
	if timeout <= 0 {
		timeout = defaultTaskTimeout
	}
	return &ContinuationTaskBackend{
		tasks: tasks, workspace: workspace, namespace: namespace, timeout: timeout,
	}, nil
}

func (b *ContinuationTaskBackend) Admit(ctx context.Context, admission goal.ContinuationAdmission) (goal.ContinuationReceipt, error) {
	if err := admission.Validate(); err != nil {
		return goal.ContinuationReceipt{}, fmt.Errorf("aether: invalid continuation admission: %w", err)
	}
	if admission.Inbound.Addr.TaskID != "" || admission.Inbound.Message.Addr.TaskID != "" {
		return goal.ContinuationReceipt{}, errors.New("aether: durable continuation payload must not assert a task id")
	}
	payload, err := goal.MarshalContinuationEnvelope(admission)
	if err != nil {
		return goal.ContinuationReceipt{}, err
	}
	authorization, err := continuationAuthorization(admission.Authority)
	if err != nil {
		return goal.ContinuationReceipt{}, err
	}
	nativeParentTaskID, err := resolveNativeParentTaskID(ctx, b.tasks, b.namespace, b.timeout, admission.ParentTaskID)
	if err != nil {
		return goal.ContinuationReceipt{}, err
	}
	metadata := continuationMetadata(admission)
	opts := sdk.CreateTaskOptions{
		TaskType:       goalContinuationTaskType,
		Workspace:      b.workspace,
		AssignmentMode: sdk.TaskAssignmentTargeted,
		TargetAgentID:  b.namespace,
		ParentTaskID:   nativeParentTaskID,
		Metadata:       metadata,
		Payload:        payload,
		Authorization:  authorization,
		RetryPolicy:    &pb.RetryPolicy{MaxAttempts: 1},
		TaskClass:      pb.TaskClass_TASK_CLASS_BACKGROUND,
		ContextID: goalContinuationContextPrefix + hashTaskIdentity(
			b.namespace, admission.WorkspaceID, admission.SessionID,
		),
		IdempotencyKey: goalContinuationKeyPrefix + hashTaskIdentity(
			b.namespace, b.workspace, admission.WorkspaceID, admission.SessionID,
			admission.GoalID, admission.ParentMessageID, admission.Inbound.Message.ID,
			admission.Inbound.Addr.RequestID, strconv.FormatUint(uint64(admission.Attempt), 10),
		),
	}
	var response *sdk.CreateTaskResponse
	for attempt := 0; attempt < taskAdmissionAttempts; attempt++ {
		response, err = b.tasks.CreateTaskSync(ctx, goalContinuationTaskType, b.workspace, opts, b.timeout)
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return goal.ContinuationReceipt{}, fmt.Errorf("aether: create continuation task: %w", err)
	}
	if response == nil {
		return goal.ContinuationReceipt{}, errors.New("aether: create continuation task returned no response")
	}
	if !response.Success {
		return goal.ContinuationReceipt{}, fmt.Errorf("aether: create continuation task rejected (%s): %s", response.ErrorCode, response.ErrorMessage)
	}
	if strings.TrimSpace(response.TaskID) == "" {
		return goal.ContinuationReceipt{}, errors.New("aether: create continuation task returned an empty task id")
	}
	return goal.ContinuationReceipt{Backend: goalContinuationBackendName, TaskID: response.TaskID}, nil
}

func (b *ContinuationTaskBackend) Inspect(ctx context.Context, receipt goal.ContinuationReceipt) (goal.ContinuationState, error) {
	if err := receipt.Validate(); err != nil {
		return "", err
	}
	if receipt.Backend != goalContinuationBackendName {
		return "", fmt.Errorf("aether: continuation receipt backend %q is not supported", receipt.Backend)
	}
	return b.inspectTask(ctx, receipt.TaskID)
}

func (b *ContinuationTaskBackend) inspectTask(ctx context.Context, taskID string) (goal.ContinuationState, error) {
	query, err := b.tasks.GetTask(ctx, taskID, b.timeout)
	if err != nil {
		return "", fmt.Errorf("aether: get continuation task: %w", err)
	}
	if query == nil || !query.Success || query.Task == nil {
		message := "task query returned no task"
		if query != nil && query.Error != "" {
			message = query.Error
		}
		return "", fmt.Errorf("aether: get continuation task %q: %s", taskID, message)
	}
	switch query.Task.Status {
	case pb.TaskStatus_TASK_STATUS_COMPLETED.String():
		return goal.ContinuationCompleted, nil
	case pb.TaskStatus_TASK_STATUS_FAILED.String(), pb.TaskStatus_TASK_STATUS_REJECTED.String():
		return goal.ContinuationFailed, nil
	case pb.TaskStatus_TASK_STATUS_CANCELLED.String():
		return goal.ContinuationCancelled, nil
	case pb.TaskStatus_TASK_STATUS_RUNNING.String():
		return goal.ContinuationRunning, nil
	case pb.TaskStatus_TASK_STATUS_QUEUED.String(),
		pb.TaskStatus_TASK_STATUS_WAITING_INPUT.String(),
		pb.TaskStatus_TASK_STATUS_WAITING_AUTHORITY.String(),
		pb.TaskStatus_TASK_STATUS_WAITING_DEPENDENCY.String(),
		pb.TaskStatus_TASK_STATUS_HIBERNATED.String():
		return goal.ContinuationAdmitted, nil
	default:
		return "", fmt.Errorf("aether: continuation task %q has unsupported status %q", taskID, query.Task.Status)
	}
}

func (b *ContinuationTaskBackend) fail(ctx context.Context, taskID, reason string) error {
	return (&SubagentTaskBackend{tasks: b.tasks, timeout: b.timeout}).confirmOperation(
		ctx, taskID, pb.TaskStatus_TASK_STATUS_FAILED.String(), func() (*sdk.TaskOperationResponse, error) {
			return b.tasks.FailTask(ctx, taskID, boundedTaskReason(reason), b.timeout)
		},
	)
}

func continuationMetadata(admission goal.ContinuationAdmission) map[string]string {
	metadata := map[string]string{
		"scitrera.component":               taskMetadataComponent,
		"scitrera.kind":                    goalContinuationMetadataKind,
		"scitrera.logical_workspace":       admission.WorkspaceID,
		"scitrera.session_id":              admission.SessionID,
		"scitrera.goal_id":                 admission.GoalID,
		"scitrera.parent_message_id":       admission.ParentMessageID,
		"scitrera.continuation_message_id": admission.Inbound.Message.ID,
		"scitrera.continuation_request_id": admission.Inbound.Addr.RequestID,
		"scitrera.continuation_attempt":    strconv.FormatUint(uint64(admission.Attempt), 10),
		"scitrera.continuation_schema":     goal.ContinuationEnvelopeSchema,
	}
	addTaskMetadata(metadata, "scitrera.parent_task_id", admission.ParentTaskID)
	return metadata
}

func continuationAuthorization(authority tools.MemoryAuthority) (*pb.AuthorizationContext, error) {
	set := 0
	for _, value := range []string{authority.GrantID, authority.SubjectType, authority.SubjectID} {
		if strings.TrimSpace(value) != "" {
			set++
		}
	}
	if set == 0 {
		return nil, nil
	}
	if set != 3 {
		return nil, errors.New("aether: continuation task authority requires grant, subject type, and subject id together")
	}
	return &pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of",
		GrantId:       authority.GrantID,
		Subject: &pb.PrincipalRef{
			PrincipalType: authority.SubjectType,
			PrincipalId:   authority.SubjectID,
		},
	}, nil
}

// ContinuationExecutorConfig configures assignment-to-ingress delivery. The
// handoff store must be the same one the turn runner consumes.
type ContinuationExecutorConfig struct {
	Enqueuer    channel.Enqueuer
	AuthHandoff *authhandoff.Store
	Ledger      goal.ContinuationLedger
	Timeout     time.Duration
}

// AssignedContinuationExecutor reconstructs queued Aether continuation tasks
// into normal inbound turns. It never claims or finishes the task; the normal
// turn lifecycle and journal own those transitions.
type AssignedContinuationExecutor struct {
	backend    *ContinuationTaskBackend
	assignedTo string
	enqueuer   channel.Enqueuer
	handoff    *authhandoff.Store
	ledger     goal.ContinuationLedger

	mu   sync.Mutex
	seen map[string]struct{}
}

func NewAssignedContinuationExecutor(tasks TaskOperations, routingWorkspace, assignedTo string, cfg ContinuationExecutorConfig) (*AssignedContinuationExecutor, error) {
	if cfg.Enqueuer == nil {
		return nil, errors.New("aether: assigned continuation enqueuer is required")
	}
	if cfg.AuthHandoff == nil {
		return nil, errors.New("aether: assigned continuation authority handoff is required")
	}
	backend, err := NewContinuationTaskBackend(tasks, routingWorkspace, assignedTo, cfg.Timeout)
	if err != nil {
		return nil, err
	}
	return &AssignedContinuationExecutor{
		backend: backend, assignedTo: assignedTo, enqueuer: cfg.Enqueuer,
		handoff: cfg.AuthHandoff, ledger: cfg.Ledger, seen: map[string]struct{}{},
	}, nil
}

// RecoverQueued reconstructs goal tasks that were assigned before this worker
// disconnected. Aether's live assignment carries Payload, but its TaskInfo
// query projection intentionally does not; the credential-free envelope in the
// private continuation ledger is the durable source for restart recovery.
func (e *AssignedContinuationExecutor) RecoverQueued(ctx context.Context) error {
	if e == nil || e.backend == nil {
		return errors.New("aether: assigned continuation executor is not configured")
	}
	if e.ledger == nil {
		return errors.New("aether: continuation recovery ledger is required")
	}
	queries, ok := e.backend.tasks.(continuationTaskQueries)
	if !ok {
		return errors.New("aether: continuation task backend does not support task queries")
	}
	var (
		offset          int32
		pageToken       string
		offsetPageCount int
		seenPageTokens  = make(map[string]struct{})
	)
	for {
		response, err := queries.QueryTasks(ctx, &pb.TaskFilter{
			Workspace: e.backend.workspace,
			TaskType:  goalContinuationTaskType,
			Limit:     goalContinuationRecoveryPage,
			Offset:    offset,
			PageToken: pageToken,
		}, e.backend.timeout)
		if err != nil {
			return fmt.Errorf("aether: query queued continuation tasks: %w", err)
		}
		if response == nil {
			return errors.New("aether: queued continuation query returned no response")
		}
		if !response.Success {
			return fmt.Errorf("aether: queued continuation query rejected: %s", strings.TrimSpace(response.Error))
		}
		for _, info := range response.Tasks {
			// Aether projects pending/assigned/starting as QUEUED. Query the
			// private task type and filter the projection here so recovery also
			// remains compatible with servers that predate grouped filters.
			if info == nil || info.TaskType != goalContinuationTaskType ||
				info.Status != pb.TaskStatus_TASK_STATUS_QUEUED.String() ||
				strings.TrimSpace(info.AssignedTo) != e.assignedTo {
				continue
			}
			assignment, err := e.recoveryAssignment(ctx, info)
			if err != nil {
				return err
			}
			if err := e.HandleAssignment(ctx, assignment); err != nil {
				return fmt.Errorf("aether: recover continuation task %q: %w", info.TaskID, err)
			}
		}
		pageCount := int32(len(response.Tasks))
		offset += pageCount
		nextPageToken := strings.TrimSpace(response.NextPageToken)
		if nextPageToken != "" {
			if _, seen := seenPageTokens[nextPageToken]; seen {
				return fmt.Errorf("aether: queued continuation query returned non-advancing page cursor %q", nextPageToken)
			}
			seenPageTokens[nextPageToken] = struct{}{}
			pageToken = nextPageToken
			continue
		}
		if pageCount == 0 || pageCount < goalContinuationRecoveryPage {
			return nil
		}

		// Compatibility with servers that predate task-query cursors. A full
		// page without a cursor falls back to the accumulated offset, but the
		// path is bounded so a non-conforming server cannot spin forever.
		pageToken = ""
		offsetPageCount++
		if offsetPageCount >= goalContinuationMaxOffsetPages {
			return fmt.Errorf("aether: queued continuation query reached the limit of %d cursorless full pages", goalContinuationMaxOffsetPages)
		}
	}
}

func (e *AssignedContinuationExecutor) recoveryAssignment(ctx context.Context, info *sdk.TaskInfo) (*sdk.TaskAssignment, error) {
	envelope, err := e.ledgeredEnvelope(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("aether: recover continuation task %q: %w", info.TaskID, err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("aether: encode recovered continuation task %q: %w", info.TaskID, err)
	}
	authority, err := TaskInfoAuthority(info)
	if err != nil {
		return nil, err
	}
	authorization, err := continuationAuthorization(authority)
	if err != nil {
		return nil, err
	}
	return &sdk.TaskAssignment{
		TaskID: info.TaskID, TaskType: info.TaskType, AssignedTo: info.AssignedTo,
		Metadata: info.Metadata, Payload: payload, Authorization: authorization,
		AssignedAt: time.Unix(info.CreatedAt, 0), Workspace: info.Workspace,
	}, nil
}

func (e *AssignedContinuationExecutor) ledgeredEnvelope(ctx context.Context, info *sdk.TaskInfo) (goal.ContinuationEnvelope, error) {
	metadata := info.Metadata
	workspaceID := strings.TrimSpace(metadata["scitrera.logical_workspace"])
	sessionID := strings.TrimSpace(metadata["scitrera.session_id"])
	goalID := strings.TrimSpace(metadata["scitrera.goal_id"])
	messageID := strings.TrimSpace(metadata["scitrera.continuation_message_id"])
	requestID := strings.TrimSpace(metadata["scitrera.continuation_request_id"])
	if workspaceID == "" || sessionID == "" || goalID == "" || messageID == "" || requestID == "" {
		return goal.ContinuationEnvelope{}, errors.New("continuation task metadata lacks its ledger identity")
	}
	records, err := e.ledger.List(ctx, workspaceID, sessionID)
	if err != nil {
		return goal.ContinuationEnvelope{}, fmt.Errorf("read continuation ledger: %w", err)
	}
	for _, record := range records {
		if record.Action != goal.DecisionContinuationPlanned || record.Continuation == nil ||
			record.GoalID != goalID || record.ContinuationMessageID != messageID ||
			record.ContinuationRequestID != requestID {
			continue
		}
		envelope := record.Continuation.Clone()
		if err := validateContinuationMetadata(metadata, envelope); err != nil {
			return goal.ContinuationEnvelope{}, err
		}
		return envelope, nil
	}
	return goal.ContinuationEnvelope{}, errors.New("matching planned continuation was not found in the ledger")
}

func (e *AssignedContinuationExecutor) HandleAssignment(ctx context.Context, assignment *sdk.TaskAssignment) error {
	if assignment == nil || assignment.TaskType != goalContinuationTaskType {
		return nil
	}
	taskID := strings.TrimSpace(assignment.TaskID)
	if taskID == "" {
		return errors.New("aether: assigned continuation task id is required")
	}
	if strings.TrimSpace(assignment.AssignedTo) != e.assignedTo {
		return fmt.Errorf("aether: continuation task assigned to %q, executor is %q", assignment.AssignedTo, e.assignedTo)
	}
	if !e.markSeen(taskID) {
		return nil
	}
	state, err := e.backend.inspectTask(ctx, taskID)
	if err != nil {
		e.clearSeen(taskID)
		return err
	}
	switch state {
	case goal.ContinuationCompleted, goal.ContinuationFailed, goal.ContinuationCancelled, goal.ContinuationRunning:
		return nil
	case goal.ContinuationAdmitted:
	default:
		e.clearSeen(taskID)
		return fmt.Errorf("aether: unsupported assigned continuation state %q", state)
	}
	envelope, err := goal.ParseContinuationEnvelope(assignment.Payload)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	if err := validateContinuationMetadata(assignment.Metadata, envelope); err != nil {
		return e.reject(ctx, taskID, err)
	}
	grantID, subjectType, subjectID, err := assignedAuthority(assignment.Authorization)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	inbound := envelope.Inbound
	inbound.Addr.TaskID = taskID
	inbound.Message.Addr = inbound.Addr
	handoffToken := ""
	if grantID != "" {
		handoffToken = e.handoff.Put(tools.MemoryAuthority{
			GrantID: grantID, SubjectType: subjectType, SubjectID: subjectID,
		})
		if handoffToken == "" {
			return e.reject(ctx, taskID, errors.New("aether: could not create continuation authority handoff"))
		}
		inbound.Message = authhandoff.StampMessage(inbound.Message, handoffToken)
	}
	if err := e.enqueuer.Enqueue(ctx, inbound); err != nil {
		_, _ = e.handoff.Resolve(handoffToken)
		return e.reject(ctx, taskID, fmt.Errorf("aether: enqueue assigned continuation: %w", err))
	}
	return nil
}

func (e *AssignedContinuationExecutor) markSeen(taskID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.seen[taskID]; ok {
		return false
	}
	e.seen[taskID] = struct{}{}
	return true
}

func (e *AssignedContinuationExecutor) clearSeen(taskID string) {
	e.mu.Lock()
	delete(e.seen, taskID)
	e.mu.Unlock()
}

func (e *AssignedContinuationExecutor) reject(ctx context.Context, taskID string, cause error) error {
	finishErr := e.backend.fail(context.WithoutCancel(ctx), taskID, cause.Error())
	if finishErr != nil {
		finishErr = fmt.Errorf("aether: fail assigned continuation task: %w", finishErr)
	}
	return errors.Join(cause, finishErr)
}

func validateContinuationMetadata(metadata map[string]string, envelope goal.ContinuationEnvelope) error {
	want := map[string]string{
		"scitrera.component":               taskMetadataComponent,
		"scitrera.kind":                    goalContinuationMetadataKind,
		"scitrera.logical_workspace":       envelope.WorkspaceID,
		"scitrera.session_id":              envelope.SessionID,
		"scitrera.goal_id":                 envelope.GoalID,
		"scitrera.parent_message_id":       envelope.ParentMessageID,
		"scitrera.continuation_message_id": envelope.Inbound.Message.ID,
		"scitrera.continuation_request_id": envelope.Inbound.Addr.RequestID,
		"scitrera.continuation_attempt":    strconv.FormatUint(uint64(envelope.Attempt), 10),
		"scitrera.continuation_schema":     envelope.Schema,
	}
	if envelope.ParentTaskID != "" {
		want["scitrera.parent_task_id"] = envelope.ParentTaskID
	}
	for key, expected := range want {
		if metadata[key] != expected {
			return fmt.Errorf("aether: assigned continuation metadata %q mismatch", key)
		}
	}
	return nil
}

// EnableGoalContinuations installs durable task admission and assignment on an
// OSS Channel. Call it before Start so live assignments cannot race handler
// registration; call ReconcileGoalContinuations after Start to recover assigned
// tasks whose original callback was lost with an earlier process.
func (c *Channel) EnableGoalContinuations(ledger goal.ContinuationLedger, handoff *authhandoff.Store, timeout time.Duration) (*ContinuationTaskBackend, error) {
	if ledger == nil {
		return nil, errors.New("aether: goal continuation ledger is required")
	}
	backend, err := NewContinuationTaskBackend(c.client, c.workspace, c.client.Topic(), timeout)
	if err != nil {
		return nil, err
	}
	executor, err := NewAssignedContinuationExecutor(c.client, c.workspace, c.client.Topic(), ContinuationExecutorConfig{
		Enqueuer: c, AuthHandoff: handoff, Ledger: ledger, Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	c.assignmentRouter.Register(executor.HandleAssignment)
	c.goalContinuations = executor
	return backend, nil
}

// ReconcileGoalContinuations recovers queued assignments after the SDK receive
// loop is live, so synchronous task queries can receive their responses.
func (c *Channel) ReconcileGoalContinuations(ctx context.Context) error {
	if c.goalContinuations == nil {
		return nil
	}
	return c.goalContinuations.RecoverQueued(ctx)
}

var _ goal.ContinuationBackend = (*ContinuationTaskBackend)(nil)
