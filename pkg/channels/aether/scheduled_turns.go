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
	"google.golang.org/protobuf/proto"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const (
	ScheduledTurnTaskType           = "agent-harness.scheduled-turn.v1"
	ScheduledTurnEnvelopeSchema     = "agent-harness.scheduled-turn.v1"
	ScheduledMissPolicySkip         = "skip"
	ScheduledMissPolicyFireOnce     = "fire_once"
	ScheduledMissPolicyFireAll      = "fire_all"
	ScheduledDispositionOrdinary    = "ordinary"
	ScheduledDispositionSkipped     = "skipped"
	ScheduledDispositionCoalesced   = "coalesced"
	ScheduledDispositionCatchUp     = "catch_up"
	ScheduledSkipReasonMissPolicy   = "miss_policy"
	ScheduledSkipReasonConcurrency  = "max_concurrent"
	scheduledTurnMetadataKind       = "scheduled_turn"
	scheduledTurnMessageMetaKey     = "scitrera.scheduled_turn"
	scheduledTurnRecoveryPage       = 100
	scheduledTurnMaxOffsetPages     = 1000
	scheduledTurnScheduleIDPrefix   = "ah-schedule-v1-"
	scheduledTurnDeclarationDigest  = "sha256:"
	scheduledTurnBacklogDetailLimit = 101
)

// ScheduledTurnRegistration is the validated, host-bound form of one config
// declaration. A concrete worker binding is part of the declaration digest so
// delayed tasks cannot drift to a replacement checkout or a new Git revision.
type ScheduledTurnRegistration struct {
	ID                              string                `json:"id"`
	Name                            string                `json:"name"`
	ScheduleType                    string                `json:"schedule_type"`
	ScheduleExpression              string                `json:"schedule_expression"`
	ThreadID                        string                `json:"thread_id"`
	Prompt                          string                `json:"prompt"`
	MissPolicy                      string                `json:"miss_policy"`
	TargetOfflinePolicy             string                `json:"target_offline_policy"`
	Enabled                         bool                  `json:"enabled"`
	RequireTaskAuthority            bool                  `json:"require_task_authority,omitempty"`
	RequiredDownstreamAuthorityHops uint32                `json:"required_downstream_authority_hops,omitempty"`
	Binding                         spec.ExecutionBinding `json:"execution_binding"`
	ViewPolicy                      ScheduledViewPolicy   `json:"view_policy"`
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
	if r.RequiredDownstreamAuthorityHops > 1 {
		return fmt.Errorf("aether: scheduled turn %q supports at most one downstream authority hop", r.ID)
	}
	if r.RequiredDownstreamAuthorityHops > 0 && !r.RequireTaskAuthority {
		return fmt.Errorf("aether: scheduled turn %q reserves downstream authority without requiring task authority", r.ID)
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
		"scitrera.thread_id":            envelope.ThreadID,
		"scitrera.view_id":              envelope.Binding.ViewID,
		"scitrera.view_revision":        envelope.Binding.Revision,
		"scitrera.execution_tool_host":  envelope.Binding.ToolHostID,
		"turn_tool_host_id":             envelope.Binding.ToolHostID,
		"turn_surface_kind":             "worker",
		"turn_surface_instance_id":      envelope.Binding.ToolHostID,
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
	Type                            string                `json:"type"`
	TaskType                        string                `json:"task_type"`
	TargetAgentID                   string                `json:"target_agent_id"`
	TargetOfflinePolicy             string                `json:"target_offline_policy"`
	PayloadEncoding                 string                `json:"payload_encoding"`
	Payload                         scheduledTurnEnvelope `json:"payload"`
	Workspace                       string                `json:"workspace"`
	Metadata                        map[string]string     `json:"metadata"`
	Retry                           map[string]any        `json:"retry"`
	RequireTaskAuthority            bool                  `json:"require_task_authority,omitempty"`
	RequiredDownstreamAuthorityHops uint32                `json:"required_downstream_authority_hops,omitempty"`
}

type scheduledWorkflowDefinition struct {
	ID             string                           `json:"id"`
	Name           string                           `json:"name"`
	Workspace      string                           `json:"workspace"`
	ScheduleType   string                           `json:"schedule_type"`
	ScheduleExpr   string                           `json:"schedule_expr"`
	Action         scheduledWorkflowAction          `json:"action"`
	Enabled        bool                             `json:"enabled"`
	MissPolicy     string                           `json:"miss_policy"`
	MaxConcurrent  int                              `json:"max_concurrent"`
	NextFireAt     *time.Time                       `json:"next_fire_at,omitempty"`
	LastFiredAt    *time.Time                       `json:"last_fired_at,omitempty"`
	LastOccurrence *ScheduledTurnScheduleOccurrence `json:"last_occurrence,omitempty"`
}

// ScheduledTurnScheduleOccurrence is Aether's bounded authoritative summary
// of the latest decision for one schedule. DispatchedAt is nil for a no-task
// skip, which remains observable here even though it cannot appear in task
// history.
type ScheduledTurnScheduleOccurrence struct {
	ScheduledFor     time.Time  `json:"scheduled_for"`
	DispatchedAt     *time.Time `json:"dispatched_at,omitempty"`
	Disposition      string     `json:"disposition"`
	Reason           string     `json:"reason,omitempty"`
	BacklogCount     int        `json:"backlog_count"`
	BacklogTruncated bool       `json:"backlog_truncated"`
	BacklogIndex     int        `json:"backlog_index"`
}

// ScheduledTurnScheduleState is the operations projection of one Sahara-owned
// Aether schedule. It is read-only; declarations and reconciliation remain
// worker-owned and Aether remains authoritative for scheduling.
type ScheduledTurnScheduleState struct {
	WorkflowScheduleID string                           `json:"workflow_schedule_id"`
	DeclarationID      string                           `json:"declaration_id"`
	DeclarationDigest  string                           `json:"declaration_digest"`
	Name               string                           `json:"name"`
	ScheduleType       string                           `json:"schedule_type"`
	ScheduleExpression string                           `json:"schedule_expression"`
	RoutingWorkspace   string                           `json:"routing_workspace"`
	AssignedTo         string                           `json:"assigned_to"`
	LogicalWorkspace   string                           `json:"logical_workspace"`
	ThreadID           string                           `json:"thread_id"`
	ViewID             string                           `json:"view_id"`
	ViewRevision       string                           `json:"view_revision,omitempty"`
	MissPolicy         string                           `json:"miss_policy"`
	MaxConcurrent      int                              `json:"max_concurrent"`
	Enabled            bool                             `json:"enabled"`
	NextFireAt         *time.Time                       `json:"next_fire_at,omitempty"`
	LastFiredAt        *time.Time                       `json:"last_fired_at,omitempty"`
	LastOccurrence     *ScheduledTurnScheduleOccurrence `json:"last_occurrence,omitempty"`
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
			Retry:                           map[string]any{"max_attempts": 1},
			RequireTaskAuthority:            registration.RequireTaskAuthority,
			RequiredDownstreamAuthorityHops: registration.RequiredDownstreamAuthorityHops,
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

// Registrations returns a source-independent copy of the executor's current
// declaration set. It is used by embedding distributions to restore the
// previous accepted set when a remote reconciliation fails.
func (e *ScheduledTurnExecutor) Registrations() []ScheduledTurnRegistration {
	e.registrationsMu.RLock()
	registrations := make([]ScheduledTurnRegistration, 0, len(e.registrations))
	for _, registration := range e.registrations {
		registrations = append(registrations, registration)
	}
	e.registrationsMu.RUnlock()
	sort.Slice(registrations, func(i, j int) bool { return registrations[i].ID < registrations[j].ID })
	return registrations
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

// ScheduleOperations is the bounded WorkflowEngine SDK surface required by
// scheduled-turn reconciliation. It is exported so distributions that already
// own an Aether connection can reuse the exact OSS reconciler.
type ScheduleOperations interface {
	ListSchedulesAuthorized(context.Context, string, *pb.AuthorizationContext) (*sdk.WorkflowResponse, error)
	UpsertScheduleWithOptions(context.Context, []byte, sdk.WorkflowScheduleOperationOptions) (*sdk.WorkflowResponse, error)
	DeleteScheduleAuthorized(context.Context, string, string, *pb.AuthorizationContext) (*sdk.WorkflowResponse, error)
}

// ReconcileScheduledTurns converges Aether to one worker's desired declaration
// set without constructing a second transport. Embedding distributions should
// install their assignment executor before connecting, or reconcile before
// connection, so a newly created schedule cannot race handler registration.
func ReconcileScheduledTurns(
	ctx context.Context,
	operations ScheduleOperations,
	routingWorkspace string,
	assignedTo string,
	registrations []ScheduledTurnRegistration,
	provider ScheduledTurnAuthorityProvider,
) error {
	if operations == nil {
		return errors.New("aether: scheduled workflow operations are not configured")
	}
	channel := &Channel{
		workspace: routingWorkspace, scheduleOps: operations,
		scheduledTurnAuthority: provider,
	}
	channel.scheduleReconcileMu.Lock()
	defer channel.scheduleReconcileMu.Unlock()
	if err := channel.validateScheduledTurnAuthorityRequirements(registrations); err != nil {
		return err
	}
	return channel.reconcileScheduledTurnDefinitionsFor(ctx, assignedTo, registrations)
}

type ScheduledTurnAuthorityOperation string

const (
	ScheduledTurnAuthorityList   ScheduledTurnAuthorityOperation = "list"
	ScheduledTurnAuthorityUpsert ScheduledTurnAuthorityOperation = "upsert"
	ScheduledTurnAuthorityDelete ScheduledTurnAuthorityOperation = "delete"
)

// ScheduledTurnAuthorityRequest is credential-free context for one schedule
// management operation. Registration is present only for upsert and is a copy;
// providers must derive credentials from their own authority source.
type ScheduledTurnAuthorityRequest struct {
	Operation          ScheduledTurnAuthorityOperation
	RoutingWorkspace   string
	WorkflowScheduleID string
	AssignedTo         string
	Registration       *ScheduledTurnRegistration
}

// ScheduledTurnAuthority remains on the Aether transport envelope. It is never
// serialized into scheduledWorkflowDefinition, the task payload, or metadata.
type ScheduledTurnAuthority struct {
	Authorization  *pb.AuthorizationContext
	AuthorityScope *pb.WorkflowScheduleAuthorityScope
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
	if err := c.validateScheduledTurnAuthorityRequirements(registrations); err != nil {
		return err
	}
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
	if err := c.validateScheduledTurnAuthorityRequirements(registrations); err != nil {
		return err
	}
	if err := executor.ReplaceRegistrations(registrations); err != nil {
		return err
	}
	return c.reconcileScheduledTurnDefinitions(ctx, registrations)
}

func (c *Channel) reconcileScheduledTurnDefinitions(ctx context.Context, registrations []ScheduledTurnRegistration) error {
	return c.reconcileScheduledTurnDefinitionsFor(ctx, c.Topic(), registrations)
}

func (c *Channel) reconcileScheduledTurnDefinitionsFor(ctx context.Context, assignedTo string, registrations []ScheduledTurnRegistration) error {
	current, err := c.listScheduledWorkflowDefinitionsFor(ctx, assignedTo)
	if err != nil {
		return fmt.Errorf("aether: load schedules for reconciliation: %w", err)
	}
	stale := map[string]struct{}{}
	for _, definition := range current {
		if ownedScheduledWorkflow(definition, c.workspace, assignedTo) {
			stale[definition.ID] = struct{}{}
		}
	}
	ordered := append([]ScheduledTurnRegistration(nil), registrations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	for _, registration := range ordered {
		scheduleID := scheduledWorkflowID(c.workspace, assignedTo, registration.ID)
		if !registration.Enabled {
			stale[scheduleID] = struct{}{}
			continue
		}
		data, err := scheduledWorkflowData(c.workspace, assignedTo, registration)
		if err != nil {
			return err
		}
		registrationCopy := registration
		authority, err := c.authorityForScheduledTurn(ctx, ScheduledTurnAuthorityRequest{
			Operation: ScheduledTurnAuthorityUpsert, RoutingWorkspace: c.workspace,
			WorkflowScheduleID: scheduleID, AssignedTo: assignedTo, Registration: &registrationCopy,
		})
		if err != nil {
			return fmt.Errorf("aether: authorize schedule %q: %w", registration.ID, err)
		}
		response, err := c.scheduleOps.UpsertScheduleWithOptions(ctx, data, sdk.WorkflowScheduleOperationOptions{
			Authorization: authority.Authorization, AuthorityScope: authority.AuthorityScope,
		})
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
		authority, err := c.authorityForScheduledTurn(ctx, ScheduledTurnAuthorityRequest{
			Operation: ScheduledTurnAuthorityDelete, RoutingWorkspace: c.workspace,
			WorkflowScheduleID: id, AssignedTo: assignedTo,
		})
		if err != nil {
			return fmt.Errorf("aether: authorize stale schedule %q deletion: %w", id, err)
		}
		response, err := c.scheduleOps.DeleteScheduleAuthorized(ctx, c.workspace, id, authority.Authorization)
		if err != nil {
			return fmt.Errorf("aether: delete stale schedule %q: %w", id, err)
		}
		if response == nil || !response.Success {
			return fmt.Errorf("aether: delete stale schedule %q was rejected: %s", id, workflowResponseError(response))
		}
	}
	return nil
}

func (c *Channel) validateScheduledTurnAuthorityRequirements(registrations []ScheduledTurnRegistration) error {
	if c.scheduledTurnAuthority != nil {
		return nil
	}
	for _, registration := range registrations {
		if registration.Enabled && registration.RequireTaskAuthority {
			return fmt.Errorf("aether: scheduled turn %q requires task authority but no provider is configured", registration.ID)
		}
	}
	return nil
}

func (c *Channel) listScheduledWorkflowDefinitions(ctx context.Context) ([]scheduledWorkflowDefinition, error) {
	return c.listScheduledWorkflowDefinitionsFor(ctx, c.Topic())
}

func (c *Channel) listScheduledWorkflowDefinitionsFor(ctx context.Context, assignedTo string) ([]scheduledWorkflowDefinition, error) {
	if c == nil || c.scheduleOps == nil {
		return nil, errors.New("scheduled workflow operations are not configured")
	}
	authority, err := c.authorityForScheduledTurn(ctx, ScheduledTurnAuthorityRequest{
		Operation: ScheduledTurnAuthorityList, RoutingWorkspace: c.workspace, AssignedTo: assignedTo,
	})
	if err != nil {
		return nil, fmt.Errorf("authorize schedule list: %w", err)
	}
	response, err := c.scheduleOps.ListSchedulesAuthorized(ctx, c.workspace, authority.Authorization)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	if response == nil || !response.Success {
		return nil, fmt.Errorf("list schedules was rejected: %s", workflowResponseError(response))
	}
	var definitions []scheduledWorkflowDefinition
	if len(bytes.TrimSpace(response.Data)) == 0 {
		return definitions, nil
	}
	if err := json.Unmarshal(response.Data, &definitions); err != nil {
		return nil, fmt.Errorf("decode schedules: %w", err)
	}
	return definitions, nil
}

func (c *Channel) authorityForScheduledTurn(ctx context.Context, request ScheduledTurnAuthorityRequest) (ScheduledTurnAuthority, error) {
	switch request.Operation {
	case ScheduledTurnAuthorityList, ScheduledTurnAuthorityUpsert, ScheduledTurnAuthorityDelete:
	default:
		return ScheduledTurnAuthority{}, fmt.Errorf("unsupported schedule authority operation %q", request.Operation)
	}
	provider := c.scheduledTurnAuthority
	if provider == nil {
		if request.Registration != nil && request.Registration.RequireTaskAuthority {
			return ScheduledTurnAuthority{}, errors.New("required task authority provider is not configured")
		}
		return ScheduledTurnAuthority{}, nil
	}
	providerRequest := request
	if request.Registration != nil {
		registrationCopy := *request.Registration
		providerRequest.Registration = &registrationCopy
	}
	authority, err := provider.AuthorityForScheduledTurn(ctx, providerRequest)
	if err != nil {
		return ScheduledTurnAuthority{}, err
	}
	if request.Operation != ScheduledTurnAuthorityUpsert {
		if authority.AuthorityScope != nil {
			return ScheduledTurnAuthority{}, errors.New("schedule authority scope is valid only for upsert")
		}
		return cloneScheduledTurnAuthority(authority), nil
	}
	if request.Registration == nil {
		return ScheduledTurnAuthority{}, errors.New("schedule upsert authority requires a registration")
	}
	if request.Registration.RequireTaskAuthority {
		if authority.Authorization == nil || authority.AuthorityScope == nil {
			return ScheduledTurnAuthority{}, errors.New("required task authority needs authorization and a bounded scope")
		}
		if authority.AuthorityScope.GetPolicyVersion() != sdk.WorkflowScheduleAuthorityPolicyVersion {
			return ScheduledTurnAuthority{}, fmt.Errorf("schedule authority policy version %d is unsupported", authority.AuthorityScope.GetPolicyVersion())
		}
		if authority.AuthorityScope.GetRequiredTaskAuthorityHops() < request.Registration.RequiredDownstreamAuthorityHops {
			return ScheduledTurnAuthority{}, errors.New("schedule authority scope has insufficient downstream hops")
		}
	} else if authority.AuthorityScope != nil {
		return ScheduledTurnAuthority{}, errors.New("provider returned task authority for a direct schedule")
	}
	return cloneScheduledTurnAuthority(authority), nil
}

func cloneScheduledTurnAuthority(authority ScheduledTurnAuthority) ScheduledTurnAuthority {
	cloned := ScheduledTurnAuthority{}
	if authority.Authorization != nil {
		cloned.Authorization = proto.Clone(authority.Authorization).(*pb.AuthorizationContext)
	}
	if authority.AuthorityScope != nil {
		cloned.AuthorityScope = proto.Clone(authority.AuthorityScope).(*pb.WorkflowScheduleAuthorityScope)
	}
	return cloned
}

// ListScheduledTurnScheduleStates returns Aether's current state for the
// scheduled-turn declarations owned by this concrete worker. Other workflow
// schedules and other workers' declarations are deliberately omitted.
func (c *Channel) ListScheduledTurnScheduleStates(ctx context.Context) ([]ScheduledTurnScheduleState, error) {
	definitions, err := c.listScheduledWorkflowDefinitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("aether: list scheduled-turn state: %w", err)
	}
	states := make([]ScheduledTurnScheduleState, 0, len(definitions))
	for _, definition := range definitions {
		if !ownedScheduledWorkflow(definition, c.workspace, c.Topic()) {
			continue
		}
		state, err := projectScheduledTurnScheduleState(definition)
		if err != nil {
			return nil, fmt.Errorf("aether: project schedule %q: %w", definition.ID, err)
		}
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].DeclarationID < states[j].DeclarationID
	})
	return states, nil
}

func projectScheduledTurnScheduleState(definition scheduledWorkflowDefinition) (ScheduledTurnScheduleState, error) {
	envelope := definition.Action.Payload
	if err := envelope.validate(); err != nil {
		return ScheduledTurnScheduleState{}, fmt.Errorf("invalid action envelope: %w", err)
	}
	if err := validateScheduledTurnMetadata(definition.Action.Metadata, envelope); err != nil {
		return ScheduledTurnScheduleState{}, err
	}
	registration := ScheduledTurnRegistration{
		ID: envelope.ScheduleID, Name: definition.Name,
		ScheduleType: definition.ScheduleType, ScheduleExpression: definition.ScheduleExpr,
		ThreadID: envelope.ThreadID, Prompt: envelope.Prompt, MissPolicy: envelope.MissPolicy,
		TargetOfflinePolicy: definition.Action.TargetOfflinePolicy, Enabled: true,
		Binding: envelope.Binding, ViewPolicy: envelope.ViewPolicy,
	}
	if err := registration.Validate(); err != nil {
		return ScheduledTurnScheduleState{}, fmt.Errorf("invalid declaration: %w", err)
	}
	digest, err := scheduledTurnDigest(registration)
	if err != nil {
		return ScheduledTurnScheduleState{}, fmt.Errorf("digest declaration: %w", err)
	}
	if digest != envelope.DeclarationDigest {
		return ScheduledTurnScheduleState{}, errors.New("declaration digest does not match schedule content")
	}
	if definition.MissPolicy != envelope.MissPolicy {
		return ScheduledTurnScheduleState{}, errors.New("schedule and declaration miss policies differ")
	}
	if definition.Action.Workspace != definition.Workspace || definition.Action.PayloadEncoding != "json" {
		return ScheduledTurnScheduleState{}, errors.New("scheduled action routing or encoding is invalid")
	}
	if definition.Action.TargetAgentID != envelope.Binding.ToolHostID {
		return ScheduledTurnScheduleState{}, errors.New("scheduled action target does not match its execution binding")
	}

	occurrence, err := validateScheduledTurnScheduleOccurrence(
		definition.LastOccurrence, definition.LastFiredAt, definition.MissPolicy,
	)
	if err != nil {
		return ScheduledTurnScheduleState{}, err
	}
	return ScheduledTurnScheduleState{
		WorkflowScheduleID: definition.ID, DeclarationID: envelope.ScheduleID,
		DeclarationDigest: envelope.DeclarationDigest, Name: definition.Name,
		ScheduleType: definition.ScheduleType, ScheduleExpression: definition.ScheduleExpr,
		RoutingWorkspace: definition.Workspace, AssignedTo: definition.Action.TargetAgentID,
		LogicalWorkspace: envelope.Binding.WorkspaceID, ThreadID: envelope.ThreadID,
		ViewID: envelope.Binding.ViewID, ViewRevision: envelope.Binding.Revision,
		MissPolicy: definition.MissPolicy, MaxConcurrent: definition.MaxConcurrent, Enabled: definition.Enabled,
		NextFireAt: cloneUTC(definition.NextFireAt), LastFiredAt: cloneUTC(definition.LastFiredAt),
		LastOccurrence: occurrence,
	}, nil
}

func validateScheduledTurnScheduleOccurrence(
	occurrence *ScheduledTurnScheduleOccurrence,
	lastFiredAt *time.Time,
	missPolicy string,
) (*ScheduledTurnScheduleOccurrence, error) {
	if occurrence == nil {
		return nil, nil
	}
	if occurrence.ScheduledFor.IsZero() || occurrence.BacklogCount < 1 || occurrence.BacklogCount > scheduledTurnBacklogDetailLimit {
		return nil, errors.New("schedule has invalid occurrence time or backlog count")
	}
	if occurrence.BacklogTruncated && occurrence.BacklogCount != scheduledTurnBacklogDetailLimit {
		return nil, errors.New("schedule has invalid occurrence backlog truncation")
	}
	fired := occurrence.Disposition != ScheduledDispositionSkipped
	if fired {
		if occurrence.DispatchedAt == nil || occurrence.DispatchedAt.Before(occurrence.ScheduledFor) {
			return nil, errors.New("schedule has invalid occurrence dispatch time")
		}
		if lastFiredAt == nil || !lastFiredAt.Equal(*occurrence.DispatchedAt) {
			return nil, errors.New("schedule occurrence dispatch does not match last_fired_at")
		}
		if occurrence.Reason != "" {
			return nil, errors.New("dispatched schedule occurrence has a skip reason")
		}
	} else if occurrence.DispatchedAt != nil {
		return nil, errors.New("skipped schedule occurrence has a dispatch time")
	}

	switch occurrence.Disposition {
	case ScheduledDispositionOrdinary:
		if occurrence.BacklogCount != 1 || occurrence.BacklogTruncated || occurrence.BacklogIndex != 1 {
			return nil, errors.New("schedule has inconsistent ordinary occurrence state")
		}
	case ScheduledDispositionSkipped:
		if occurrence.BacklogIndex != 0 {
			return nil, errors.New("schedule has inconsistent skipped occurrence index")
		}
		switch occurrence.Reason {
		case ScheduledSkipReasonMissPolicy:
			if missPolicy != ScheduledMissPolicySkip || occurrence.BacklogCount < 2 {
				return nil, errors.New("schedule has inconsistent missed-policy skip state")
			}
		case ScheduledSkipReasonConcurrency:
		default:
			return nil, errors.New("schedule has an unknown skip reason")
		}
	case ScheduledDispositionCoalesced:
		if missPolicy != ScheduledMissPolicyFireOnce || occurrence.BacklogCount < 2 || occurrence.BacklogIndex != 1 {
			return nil, errors.New("schedule has inconsistent coalesced occurrence state")
		}
	case ScheduledDispositionCatchUp:
		if missPolicy != ScheduledMissPolicyFireAll || occurrence.BacklogCount < 2 ||
			occurrence.BacklogIndex < 1 || occurrence.BacklogIndex > occurrence.BacklogCount {
			return nil, errors.New("schedule has inconsistent catch-up occurrence state")
		}
	default:
		return nil, fmt.Errorf("schedule has unknown occurrence disposition %q", occurrence.Disposition)
	}

	copy := *occurrence
	copy.ScheduledFor = occurrence.ScheduledFor.UTC()
	copy.DispatchedAt = cloneUTC(occurrence.DispatchedAt)
	return &copy, nil
}

func cloneUTC(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
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
