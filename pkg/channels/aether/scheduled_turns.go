package aether

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	ScheduledTurnTaskType          = "agent-harness.scheduled-turn.v1"
	ScheduledTurnEnvelopeSchema    = "agent-harness.scheduled-turn.v1"
	ScheduledMissPolicySkip        = "skip"
	ScheduledMissPolicyFireOnce    = "fire_once"
	ScheduledMissPolicyFireAll     = "fire_all"
	scheduledTurnMetadataKind      = "scheduled_turn"
	scheduledTurnMessageMetaKey    = "scitrera.scheduled_turn"
	scheduledTurnRecoveryPage      = 100
	scheduledTurnMaxOffsetPages    = 1000
	scheduledTurnScheduleIDPrefix  = "ah-schedule-v1-"
	scheduledTurnDeclarationDigest = "sha256:"
)

// ScheduledTurnRegistration is the validated, host-bound form of one config
// declaration. A concrete worker binding is part of the declaration digest so
// delayed tasks cannot drift to a replacement checkout or a new Git revision.
type ScheduledTurnRegistration struct {
	ID                  string                `json:"id"`
	Name                string                `json:"name"`
	ScheduleType        string                `json:"schedule_type"`
	ScheduleExpression  string                `json:"schedule_expression"`
	ThreadID            string                `json:"thread_id"`
	Prompt              string                `json:"prompt"`
	MissPolicy          string                `json:"miss_policy"`
	TargetOfflinePolicy string                `json:"target_offline_policy"`
	Enabled             bool                  `json:"enabled"`
	Binding             spec.ExecutionBinding `json:"execution_binding"`
	ViewPolicy          ScheduledViewPolicy   `json:"view_policy"`
}

func (r ScheduledTurnRegistration) Validate() error {
	if strings.TrimSpace(r.ID) == "" || strings.TrimSpace(r.Name) == "" {
		return errors.New("aether: scheduled turn id and name are required")
	}
	if !r.Enabled {
		return nil
	}
	switch r.ScheduleType {
	case "cron", "interval", "once":
	default:
		return fmt.Errorf("aether: scheduled turn %q has unsupported schedule type %q", r.ID, r.ScheduleType)
	}
	if strings.TrimSpace(r.ScheduleExpression) == "" || strings.TrimSpace(r.ThreadID) == "" || strings.TrimSpace(r.Prompt) == "" {
		return fmt.Errorf("aether: scheduled turn %q requires expression, thread, and prompt", r.ID)
	}
	if r.MissPolicy != ScheduledMissPolicySkip &&
		r.MissPolicy != ScheduledMissPolicyFireOnce &&
		r.MissPolicy != ScheduledMissPolicyFireAll {
		return fmt.Errorf("aether: scheduled turn %q has unsupported miss policy %q", r.ID, r.MissPolicy)
	}
	if r.TargetOfflinePolicy != "queue" && r.TargetOfflinePolicy != "reject" && r.TargetOfflinePolicy != "orchestrate" {
		return fmt.Errorf("aether: scheduled turn %q has unsupported target offline policy %q", r.ID, r.TargetOfflinePolicy)
	}
	if err := r.Binding.Validate(); err != nil {
		return fmt.Errorf("aether: scheduled turn %q binding: %w", r.ID, err)
	}
	if r.Binding.ExecutionSite != spec.ExecutionSiteWorker {
		return fmt.Errorf("aether: scheduled turn %q must target a worker execution site", r.ID)
	}
	if err := r.ViewPolicy.Validate(); err != nil {
		return fmt.Errorf("aether: scheduled turn %q view policy: %w", r.ID, err)
	}
	return nil
}

type scheduledTurnEnvelope struct {
	Schema            string                `json:"schema"`
	ScheduleID        string                `json:"schedule_id"`
	DeclarationDigest string                `json:"declaration_digest"`
	ThreadID          string                `json:"thread_id"`
	Prompt            string                `json:"prompt"`
	MissPolicy        string                `json:"miss_policy"`
	Binding           spec.ExecutionBinding `json:"execution_binding"`
	ViewPolicy        ScheduledViewPolicy   `json:"view_policy"`
}

func (e scheduledTurnEnvelope) validate() error {
	if e.Schema != ScheduledTurnEnvelopeSchema {
		return fmt.Errorf("aether: unsupported scheduled turn schema %q", e.Schema)
	}
	if e.ScheduleID == "" || e.DeclarationDigest == "" || e.ThreadID == "" || strings.TrimSpace(e.Prompt) == "" ||
		(e.MissPolicy != ScheduledMissPolicySkip && e.MissPolicy != ScheduledMissPolicyFireOnce && e.MissPolicy != ScheduledMissPolicyFireAll) {
		return errors.New("aether: scheduled turn envelope is incomplete")
	}
	if err := e.Binding.Validate(); err != nil {
		return fmt.Errorf("aether: scheduled turn execution binding: %w", err)
	}
	if e.Binding.ExecutionSite != spec.ExecutionSiteWorker {
		return errors.New("aether: scheduled turn execution site is not worker")
	}
	if err := e.ViewPolicy.Validate(); err != nil {
		return fmt.Errorf("aether: scheduled turn view policy: %w", err)
	}
	return nil
}

func parseScheduledTurnEnvelope(payload []byte) (scheduledTurnEnvelope, error) {
	var envelope scheduledTurnEnvelope
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return scheduledTurnEnvelope{}, fmt.Errorf("aether: decode scheduled turn envelope: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return scheduledTurnEnvelope{}, errors.New("aether: scheduled turn envelope has trailing JSON")
		}
		return scheduledTurnEnvelope{}, fmt.Errorf("aether: decode scheduled turn envelope trailer: %w", err)
	}
	if err := envelope.validate(); err != nil {
		return scheduledTurnEnvelope{}, err
	}
	return envelope, nil
}

func scheduledTurnDigest(registration ScheduledTurnRegistration) (string, error) {
	canonical, err := json.Marshal(struct {
		Schema       string                    `json:"schema"`
		Registration ScheduledTurnRegistration `json:"registration"`
	}{
		Schema: ScheduledTurnEnvelopeSchema, Registration: registration,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return scheduledTurnDeclarationDigest + hex.EncodeToString(digest[:]), nil
}

func scheduledTurnEnvelopeFor(registration ScheduledTurnRegistration) (scheduledTurnEnvelope, error) {
	digest, err := scheduledTurnDigest(registration)
	if err != nil {
		return scheduledTurnEnvelope{}, err
	}
	return scheduledTurnEnvelope{
		Schema: ScheduledTurnEnvelopeSchema, ScheduleID: registration.ID,
		DeclarationDigest: digest, ThreadID: registration.ThreadID,
		Prompt: registration.Prompt, MissPolicy: registration.MissPolicy, Binding: registration.Binding,
		ViewPolicy: registration.ViewPolicy,
	}, nil
}

func scheduledTurnMetadata(envelope scheduledTurnEnvelope) map[string]string {
	return map[string]string{
		"scitrera.component":            taskMetadataComponent,
		"scitrera.kind":                 scheduledTurnMetadataKind,
		"scitrera.schedule_id":          envelope.ScheduleID,
		"scitrera.schedule_schema":      envelope.Schema,
		"scitrera.schedule_digest":      envelope.DeclarationDigest,
		"scitrera.schedule_miss_policy": envelope.MissPolicy,
		"scitrera.logical_workspace":    envelope.Binding.WorkspaceID,
		"scitrera.view_id":              envelope.Binding.ViewID,
		"scitrera.view_revision":        envelope.Binding.Revision,
		"scitrera.execution_tool_host":  envelope.Binding.ToolHostID,
	}
}

func validateScheduledTurnMetadata(metadata map[string]string, envelope scheduledTurnEnvelope) error {
	for key, expected := range scheduledTurnMetadata(envelope) {
		if metadata[key] != expected {
			return fmt.Errorf("aether: assigned scheduled turn metadata %q mismatch", key)
		}
	}
	return nil
}

type scheduledWorkflowAction struct {
	Type                string                `json:"type"`
	TaskType            string                `json:"task_type"`
	TargetAgentID       string                `json:"target_agent_id"`
	TargetOfflinePolicy string                `json:"target_offline_policy"`
	PayloadEncoding     string                `json:"payload_encoding"`
	Payload             scheduledTurnEnvelope `json:"payload"`
	Workspace           string                `json:"workspace"`
	Metadata            map[string]string     `json:"metadata"`
	Retry               map[string]any        `json:"retry"`
}

type scheduledWorkflowDefinition struct {
	ID            string                  `json:"id"`
	Name          string                  `json:"name"`
	Workspace     string                  `json:"workspace"`
	ScheduleType  string                  `json:"schedule_type"`
	ScheduleExpr  string                  `json:"schedule_expr"`
	Action        scheduledWorkflowAction `json:"action"`
	Enabled       bool                    `json:"enabled"`
	MissPolicy    string                  `json:"miss_policy"`
	MaxConcurrent int                     `json:"max_concurrent"`
}

func scheduledWorkflowID(routingWorkspace, assignedTo, declarationID string) string {
	return scheduledTurnScheduleIDPrefix + hashTaskIdentity(routingWorkspace, assignedTo, declarationID)
}

func scheduledWorkflowData(routingWorkspace, assignedTo string, registration ScheduledTurnRegistration) ([]byte, error) {
	if err := registration.Validate(); err != nil {
		return nil, err
	}
	if registration.Binding.ToolHostID != assignedTo {
		return nil, fmt.Errorf("aether: scheduled turn %q targets a different worker", registration.ID)
	}
	envelope, err := scheduledTurnEnvelopeFor(registration)
	if err != nil {
		return nil, err
	}
	definition := scheduledWorkflowDefinition{
		ID:   scheduledWorkflowID(routingWorkspace, assignedTo, registration.ID),
		Name: registration.Name, Workspace: routingWorkspace,
		ScheduleType: registration.ScheduleType, ScheduleExpr: registration.ScheduleExpression,
		Enabled: true, MissPolicy: registration.MissPolicy,
		// Aether's task lifecycle is authoritative for concurrency. Keep this at
		// zero until schedule active-task markers are task IDs rather than time
		// sentinels for every schedule type.
		MaxConcurrent: 0,
		Action: scheduledWorkflowAction{
			Type: "create_task", TaskType: ScheduledTurnTaskType,
			TargetAgentID: assignedTo, TargetOfflinePolicy: registration.TargetOfflinePolicy,
			PayloadEncoding: "json", Payload: envelope,
			Workspace: routingWorkspace, Metadata: scheduledTurnMetadata(envelope),
			Retry: map[string]any{"max_attempts": 1},
		},
	}
	return json.Marshal(definition)
}

type boundTurnEnqueuer interface {
	EnqueueBoundTurn(
		ctx context.Context,
		inbound channel.Inbound,
		binding spec.ExecutionBinding,
		policy ScheduledViewPolicy,
	) error
}

// ScheduledTurnExecutor converts exact-target Aether assignments into ordinary
// harness turns. It does not claim or finish tasks; the normal turn lifecycle
// owns those transitions and restart journal recovery.
type ScheduledTurnExecutor struct {
	tasks            TaskOperations
	routingWorkspace string
	assignedTo       string
	enqueuer         boundTurnEnqueuer
	handoff          *authhandoff.Store
	registrationsMu  sync.RWMutex
	registrations    map[string]ScheduledTurnRegistration
	timeout          time.Duration

	mu   sync.Mutex
	seen map[string]struct{}
}

func NewScheduledTurnExecutor(
	tasks TaskOperations,
	routingWorkspace string,
	assignedTo string,
	registrations []ScheduledTurnRegistration,
	enqueuer boundTurnEnqueuer,
	handoff *authhandoff.Store,
	timeout time.Duration,
) (*ScheduledTurnExecutor, error) {
	if tasks == nil || enqueuer == nil {
		return nil, errors.New("aether: scheduled turn task operations and enqueuer are required")
	}
	if strings.TrimSpace(routingWorkspace) == "" || strings.TrimSpace(assignedTo) == "" {
		return nil, errors.New("aether: scheduled turn routing workspace and worker are required")
	}
	if timeout <= 0 {
		timeout = defaultTaskTimeout
	}
	indexed, err := indexScheduledTurnRegistrations(registrations, assignedTo)
	if err != nil {
		return nil, err
	}
	return &ScheduledTurnExecutor{
		tasks: tasks, routingWorkspace: routingWorkspace, assignedTo: assignedTo,
		enqueuer: enqueuer, handoff: handoff, registrations: indexed,
		timeout: timeout, seen: map[string]struct{}{},
	}, nil
}

func indexScheduledTurnRegistrations(registrations []ScheduledTurnRegistration, assignedTo string) (map[string]ScheduledTurnRegistration, error) {
	indexed := make(map[string]ScheduledTurnRegistration, len(registrations))
	for _, registration := range registrations {
		if err := registration.Validate(); err != nil {
			return nil, err
		}
		if registration.Enabled && registration.Binding.ToolHostID != assignedTo {
			return nil, fmt.Errorf("aether: scheduled turn %q targets a different worker", registration.ID)
		}
		if _, exists := indexed[registration.ID]; exists {
			return nil, fmt.Errorf("aether: duplicate scheduled turn id %q", registration.ID)
		}
		indexed[registration.ID] = registration
	}
	return indexed, nil
}

// ReplaceRegistrations atomically installs a fully validated desired set. A
// task created from a removed or changed declaration is rejected immediately,
// even while the corresponding remote schedule reconciliation is in flight.
func (e *ScheduledTurnExecutor) ReplaceRegistrations(registrations []ScheduledTurnRegistration) error {
	indexed, err := indexScheduledTurnRegistrations(registrations, e.assignedTo)
	if err != nil {
		return err
	}
	e.registrationsMu.Lock()
	e.registrations = indexed
	e.registrationsMu.Unlock()
	return nil
}

func (e *ScheduledTurnExecutor) registration(id string) (ScheduledTurnRegistration, bool) {
	e.registrationsMu.RLock()
	registration, ok := e.registrations[id]
	e.registrationsMu.RUnlock()
	return registration, ok
}

func (e *ScheduledTurnExecutor) HandleAssignment(ctx context.Context, assignment *sdk.TaskAssignment) error {
	if assignment == nil || assignment.TaskType != ScheduledTurnTaskType {
		return nil
	}
	taskID := strings.TrimSpace(assignment.TaskID)
	if taskID == "" {
		return errors.New("aether: assigned scheduled turn task id is required")
	}
	if strings.TrimSpace(assignment.AssignedTo) != e.assignedTo {
		return fmt.Errorf("aether: scheduled turn assigned to %q, executor is %q", assignment.AssignedTo, e.assignedTo)
	}
	if assignment.Workspace != "" && assignment.Workspace != e.routingWorkspace {
		return fmt.Errorf("aether: scheduled turn routing workspace changed")
	}
	if !e.markSeen(taskID) {
		return nil
	}
	state, err := e.taskState(ctx, taskID)
	if err != nil {
		e.clearSeen(taskID)
		return err
	}
	switch state {
	case pb.TaskStatus_TASK_STATUS_COMPLETED.String(), pb.TaskStatus_TASK_STATUS_FAILED.String(),
		pb.TaskStatus_TASK_STATUS_REJECTED.String(), pb.TaskStatus_TASK_STATUS_CANCELLED.String(),
		pb.TaskStatus_TASK_STATUS_RUNNING.String():
		return nil
	case pb.TaskStatus_TASK_STATUS_QUEUED.String():
	default:
		return e.reject(ctx, taskID, fmt.Errorf("aether: unsupported scheduled turn task state %q", state))
	}
	envelope, err := parseScheduledTurnEnvelope(assignment.Payload)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	if envelope.Binding.ToolHostID != e.assignedTo {
		return e.reject(ctx, taskID, errors.New("aether: scheduled turn binding targets a different worker"))
	}
	if err := validateScheduledTurnMetadata(assignment.Metadata, envelope); err != nil {
		return e.reject(ctx, taskID, err)
	}
	registration, ok := e.registration(envelope.ScheduleID)
	if !ok {
		return e.reject(ctx, taskID, fmt.Errorf("aether: scheduled turn declaration %q is not configured", envelope.ScheduleID))
	}
	digest, err := scheduledTurnDigest(registration)
	if err != nil || digest != envelope.DeclarationDigest {
		if err == nil {
			err = errors.New("aether: scheduled turn declaration changed after task creation")
		}
		return e.reject(ctx, taskID, err)
	}
	part, _ := protocol.NewTextPart(envelope.Prompt)
	message := spec.NewChatMessage("scheduled-"+hashTaskIdentity(taskID), spec.RoleUser)
	message.Content = []spec.ContentPart{part}
	message.Addr = spec.MessageAddress{
		WorkspaceID: envelope.Binding.WorkspaceID, ThreadID: envelope.ThreadID,
		TaskID: taskID, RequestID: "schedule-" + envelope.ScheduleID,
	}
	message.Meta[scheduledTurnMessageMetaKey] = json.RawMessage("true")
	if err := spec.PutExecutionBinding(&message, envelope.Binding); err != nil {
		return e.reject(ctx, taskID, err)
	}
	grantID, subjectType, subjectID, err := assignedAuthority(assignment.Authorization)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	handoffToken := ""
	if grantID != "" {
		if e.handoff == nil {
			return e.reject(ctx, taskID, errors.New("aether: scheduled turn OBO authority handoff is not configured"))
		}
		handoffToken = e.handoff.Put(tools.MemoryAuthority{
			GrantID: grantID, SubjectType: subjectType, SubjectID: subjectID,
		})
		if handoffToken == "" {
			return e.reject(ctx, taskID, errors.New("aether: could not create scheduled turn authority handoff"))
		}
		message = authhandoff.StampMessage(message, handoffToken)
	}
	inbound := channel.Inbound{Addr: message.Addr, Message: message}
	if err := e.enqueuer.EnqueueBoundTurn(ctx, inbound, envelope.Binding, envelope.ViewPolicy); err != nil {
		_, _ = e.handoff.Resolve(handoffToken)
		return e.reject(ctx, taskID, fmt.Errorf("aether: enqueue scheduled turn: %w", err))
	}
	return nil
}

func (e *ScheduledTurnExecutor) taskState(ctx context.Context, taskID string) (string, error) {
	response, err := e.tasks.GetTask(ctx, taskID, e.timeout)
	if err != nil {
		return "", fmt.Errorf("aether: inspect scheduled turn task: %w", err)
	}
	if response == nil || !response.Success || response.Task == nil {
		return "", errors.New("aether: scheduled turn task query returned no authoritative task")
	}
	return response.Task.Status, nil
}

func (e *ScheduledTurnExecutor) markSeen(taskID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.seen[taskID]; ok {
		return false
	}
	e.seen[taskID] = struct{}{}
	return true
}

func (e *ScheduledTurnExecutor) clearSeen(taskID string) {
	e.mu.Lock()
	delete(e.seen, taskID)
	e.mu.Unlock()
}

func (e *ScheduledTurnExecutor) forget(taskID string) { e.clearSeen(taskID) }

func (e *ScheduledTurnExecutor) reject(ctx context.Context, taskID string, cause error) error {
	response, finishErr := e.tasks.FailTask(context.WithoutCancel(ctx), taskID, boundedTaskReason(cause.Error()), e.timeout)
	if finishErr == nil && (response == nil || !response.Success) {
		finishErr = errors.New("Aether rejected scheduled turn task failure")
	}
	if finishErr != nil {
		finishErr = fmt.Errorf("aether: fail assigned scheduled turn task: %w", finishErr)
	}
	return errors.Join(cause, finishErr)
}

// RecoverQueued recreates payloads from the current declaration only when its
// digest exactly matches the task metadata. This avoids replaying a queued task
// with a newly edited prompt or a replacement workspace revision.
func (e *ScheduledTurnExecutor) RecoverQueued(ctx context.Context) error {
	queries, ok := e.tasks.(continuationTaskQueries)
	if !ok {
		return errors.New("aether: scheduled turn backend does not support task queries")
	}
	var offset int32
	var pageToken string
	seenTokens := map[string]struct{}{}
	for page := 0; ; page++ {
		response, err := queries.QueryTasks(ctx, &pb.TaskFilter{
			Workspace: e.routingWorkspace, TaskType: ScheduledTurnTaskType,
			Limit: scheduledTurnRecoveryPage, Offset: offset, PageToken: pageToken,
		}, e.timeout)
		if err != nil {
			return fmt.Errorf("aether: query queued scheduled turns: %w", err)
		}
		if response == nil || !response.Success {
			return errors.New("aether: queued scheduled turn query was rejected")
		}
		for _, info := range response.Tasks {
			if info == nil || info.TaskType != ScheduledTurnTaskType ||
				info.Status != pb.TaskStatus_TASK_STATUS_QUEUED.String() || info.AssignedTo != e.assignedTo {
				continue
			}
			assignment, err := e.recoveryAssignment(info)
			if err != nil {
				return e.reject(ctx, info.TaskID, err)
			}
			if err := e.HandleAssignment(ctx, assignment); err != nil {
				return fmt.Errorf("aether: recover scheduled turn %q: %w", info.TaskID, err)
			}
		}
		count := int32(len(response.Tasks))
		offset += count
		next := strings.TrimSpace(response.NextPageToken)
		if next != "" {
			if _, duplicate := seenTokens[next]; duplicate {
				return fmt.Errorf("aether: scheduled turn query returned non-advancing page cursor %q", next)
			}
			seenTokens[next] = struct{}{}
			pageToken = next
			continue
		}
		if count < scheduledTurnRecoveryPage {
			return nil
		}
		if page+1 >= scheduledTurnMaxOffsetPages {
			return fmt.Errorf("aether: scheduled turn query reached the limit of %d cursorless full pages", scheduledTurnMaxOffsetPages)
		}
		pageToken = ""
	}
}

func (e *ScheduledTurnExecutor) recoveryAssignment(info *sdk.TaskInfo) (*sdk.TaskAssignment, error) {
	registration, ok := e.registration(info.Metadata["scitrera.schedule_id"])
	if !ok {
		return nil, fmt.Errorf("aether: scheduled turn declaration %q is not configured", info.Metadata["scitrera.schedule_id"])
	}
	envelope, err := scheduledTurnEnvelopeFor(registration)
	if err != nil {
		return nil, err
	}
	if info.Metadata["scitrera.schedule_digest"] != envelope.DeclarationDigest {
		return nil, errors.New("aether: queued scheduled turn declaration digest no longer matches")
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
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

// IsScheduledTurnMessage selects tasks whose claim/complete/fail lifecycle is
// owned by Aether rather than by a task-less interactive transport.
func IsScheduledTurnMessage(message protocol.ChatMessage) bool {
	return bytes.Equal(bytes.TrimSpace(message.Meta[scheduledTurnMessageMetaKey]), []byte("true"))
}

type scheduleOperations interface {
	ListSchedules(context.Context, string) (*sdk.WorkflowResponse, error)
	UpsertSchedule(context.Context, []byte) (*sdk.WorkflowResponse, error)
	DeleteSchedule(context.Context, string) (*sdk.WorkflowResponse, error)
}

// EnableScheduledTurns registers one mutable assignment handler, then applies
// the initial desired schedule set.
func (c *Channel) EnableScheduledTurns(
	ctx context.Context,
	registrations []ScheduledTurnRegistration,
	handoff *authhandoff.Store,
	timeout time.Duration,
) error {
	c.scheduleReconcileMu.Lock()
	defer c.scheduleReconcileMu.Unlock()
	executor, err := NewScheduledTurnExecutor(
		c.client, c.workspace, c.Topic(), registrations, c, handoff, timeout,
	)
	if err != nil {
		return err
	}
	c.scheduledTurnsMu.Lock()
	if c.scheduledTurns != nil {
		c.scheduledTurnsMu.Unlock()
		return errors.New("aether: scheduled turns are already enabled")
	}
	c.scheduledTurns = executor
	c.scheduledTurnsMu.Unlock()
	c.assignmentRouter.Register(executor.HandleAssignment)
	return c.reconcileScheduledTurnDefinitions(ctx, registrations)
}

// UpdateScheduledTurns atomically changes the executor's accepted declaration
// set and reconciles Aether to that same desired state. Removed declarations
// are discovered from Aether and deleted, so restart-time edits converge too.
func (c *Channel) UpdateScheduledTurns(ctx context.Context, registrations []ScheduledTurnRegistration) error {
	c.scheduleReconcileMu.Lock()
	defer c.scheduleReconcileMu.Unlock()
	c.scheduledTurnsMu.RLock()
	executor := c.scheduledTurns
	c.scheduledTurnsMu.RUnlock()
	if executor == nil {
		return errors.New("aether: scheduled turns are not enabled")
	}
	if err := executor.ReplaceRegistrations(registrations); err != nil {
		return err
	}
	return c.reconcileScheduledTurnDefinitions(ctx, registrations)
}

func (c *Channel) reconcileScheduledTurnDefinitions(ctx context.Context, registrations []ScheduledTurnRegistration) error {
	if c.scheduleOps == nil {
		return errors.New("aether: scheduled workflow operations are not configured")
	}
	response, err := c.scheduleOps.ListSchedules(ctx, c.workspace)
	if err != nil {
		return fmt.Errorf("aether: list schedules for reconciliation: %w", err)
	}
	if response == nil || !response.Success {
		return fmt.Errorf("aether: list schedules for reconciliation was rejected: %s", workflowResponseError(response))
	}
	var current []scheduledWorkflowDefinition
	if len(bytes.TrimSpace(response.Data)) > 0 {
		if err := json.Unmarshal(response.Data, &current); err != nil {
			return fmt.Errorf("aether: decode schedules for reconciliation: %w", err)
		}
	}
	stale := map[string]struct{}{}
	for _, definition := range current {
		if ownedScheduledWorkflow(definition, c.workspace, c.Topic()) {
			stale[definition.ID] = struct{}{}
		}
	}
	ordered := append([]ScheduledTurnRegistration(nil), registrations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	for _, registration := range ordered {
		scheduleID := scheduledWorkflowID(c.workspace, c.Topic(), registration.ID)
		if !registration.Enabled {
			stale[scheduleID] = struct{}{}
			continue
		}
		data, err := scheduledWorkflowData(c.workspace, c.Topic(), registration)
		if err != nil {
			return err
		}
		response, err := c.scheduleOps.UpsertSchedule(ctx, data)
		if err != nil {
			return fmt.Errorf("aether: upsert schedule %q: %w", registration.ID, err)
		}
		if response == nil || !response.Success {
			return fmt.Errorf("aether: upsert schedule %q was rejected: %s", registration.ID, workflowResponseError(response))
		}
		delete(stale, scheduleID)
	}
	deleteIDs := make([]string, 0, len(stale))
	for id := range stale {
		deleteIDs = append(deleteIDs, id)
	}
	sort.Strings(deleteIDs)
	for _, id := range deleteIDs {
		response, err := c.scheduleOps.DeleteSchedule(ctx, id)
		if err != nil {
			return fmt.Errorf("aether: delete stale schedule %q: %w", id, err)
		}
		if response == nil || !response.Success {
			return fmt.Errorf("aether: delete stale schedule %q was rejected: %s", id, workflowResponseError(response))
		}
	}
	return nil
}

func workflowResponseError(response *sdk.WorkflowResponse) string {
	if response == nil {
		return "no response"
	}
	if message := strings.TrimSpace(response.Error); message != "" {
		return message
	}
	return "no error detail"
}

func ownedScheduledWorkflow(definition scheduledWorkflowDefinition, routingWorkspace, assignedTo string) bool {
	declarationID := strings.TrimSpace(definition.Action.Metadata["scitrera.schedule_id"])
	return declarationID != "" &&
		definition.Workspace == routingWorkspace &&
		definition.Action.Type == "create_task" &&
		definition.Action.TaskType == ScheduledTurnTaskType &&
		definition.Action.TargetAgentID == assignedTo &&
		definition.Action.Metadata["scitrera.component"] == taskMetadataComponent &&
		definition.Action.Metadata["scitrera.kind"] == scheduledTurnMetadataKind &&
		definition.Action.Metadata["scitrera.execution_tool_host"] == assignedTo &&
		definition.ID == scheduledWorkflowID(routingWorkspace, assignedTo, declarationID)
}

func (c *Channel) ReconcileScheduledTurns(ctx context.Context) error {
	c.scheduledTurnsMu.RLock()
	executor := c.scheduledTurns
	c.scheduledTurnsMu.RUnlock()
	if executor == nil {
		return nil
	}
	return executor.RecoverQueued(ctx)
}

// PrepareTurnRecovery reconstructs process-local exact-view authority for a
// RUNNING scheduled turn before the durable turn journal resumes it. Aether task
// state and the current declaration digest are authoritative; no workspace path
// or policy is recovered from untrusted chat history. Non-scheduled tasks need
// no process-local binding and return a no-op cleanup.
func (c *Channel) PrepareTurnRecovery(ctx context.Context, info *sdk.TaskInfo) (func(), error) {
	cleanup := func() {}
	if info == nil || info.TaskType != ScheduledTurnTaskType {
		return cleanup, nil
	}
	if info.TaskID == "" || info.AssignedTo != c.Topic() || (info.Workspace != "" && info.Workspace != c.workspace) {
		return nil, errors.New("aether: scheduled turn recovery task identity mismatch")
	}
	c.scheduledTurnsMu.RLock()
	executor := c.scheduledTurns
	c.scheduledTurnsMu.RUnlock()
	if executor == nil {
		return nil, errors.New("aether: scheduled turns are not enabled for turn recovery")
	}
	assignment, err := executor.recoveryAssignment(info)
	if err != nil {
		return nil, fmt.Errorf("aether: reconstruct scheduled turn recovery envelope: %w", err)
	}
	envelope, err := parseScheduledTurnEnvelope(assignment.Payload)
	if err != nil {
		return nil, err
	}
	if err := validateScheduledTurnMetadata(info.Metadata, envelope); err != nil {
		return nil, err
	}
	registration, ok := executor.registration(envelope.ScheduleID)
	if !ok {
		return nil, errors.New("aether: scheduled turn recovery declaration is unavailable")
	}
	if err := registration.ViewPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("aether: scheduled turn recovery view policy: %w", err)
	}
	c.sessionMu.Lock()
	host := c.workerToolHost
	c.sessionMu.Unlock()
	if host == nil || host.local == nil {
		return nil, errors.New("aether: worker workspace tool host is not configured")
	}
	if err := host.validateScheduledBinding(ctx, envelope.Binding, registration.ViewPolicy); err != nil {
		return nil, fmt.Errorf("aether: scheduled turn recovery view is unavailable: %w", err)
	}
	c.mu.Lock()
	if _, exists := c.executionBindings[info.TaskID]; exists {
		c.mu.Unlock()
		return nil, fmt.Errorf("aether: task %q already has an execution binding", info.TaskID)
	}
	c.executionBindings[info.TaskID] = envelope.Binding
	c.executionPolicies[info.TaskID] = registration.ViewPolicy
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.executionBindings, info.TaskID)
		delete(c.executionPolicies, info.TaskID)
		c.mu.Unlock()
	}, nil
}
