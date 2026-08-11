package aether

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

const (
	defaultScheduledRunPageSize int32 = 50
	maxScheduledRunPageSize     int32 = 100
)

// ScheduledRunTaskQueries is the narrow Aether task-list surface required by
// ScheduledRunReader. Aether remains authoritative for task lifecycle and owns
// the opaque page cursor.
type ScheduledRunTaskQueries interface {
	QueryTasks(ctx context.Context, filter *pb.TaskFilter, timeout time.Duration) (*sdk.TaskQueryResponse, error)
}

// ScheduledRunJournal is the MemoryLayer-backed execution lookup required by
// ScheduledRunReader. Missing records are represented by turnjournal.ErrNotFound.
type ScheduledRunJournal interface {
	Get(ctx context.Context, workspaceID, taskID string) (turnjournal.Record, error)
}

// ScheduledRunQuery selects one cursor page. Statuses are Aether protobuf enum
// values; UNSPECIFIED is rejected because it is not a lifecycle state.
type ScheduledRunQuery struct {
	Limit     int32
	PageToken string
	Statuses  []pb.TaskStatus
}

// ScheduledRunPage preserves Aether's ordering, total, and opaque next cursor.
// No client should parse or synthesize NextPageToken.
type ScheduledRunPage struct {
	Runs          []ScheduledRun `json:"runs"`
	TotalCount    int32          `json:"total_count"`
	NextPageToken string         `json:"next_page_token,omitempty"`
}

// ScheduledRunOccurrence describes the exact WorkflowEngine occurrence that
// created a task. ScheduleID is Aether's deterministic workflow-schedule ID;
// DeclarationID is the operator-facing Sahara declaration ID.
type ScheduledRunOccurrence struct {
	ScheduleID                string    `json:"schedule_id"`
	DeclarationID             string    `json:"declaration_id"`
	MissPolicy                string    `json:"miss_policy"`
	ScheduledFor              time.Time `json:"scheduled_for"`
	DispatchedAt              time.Time `json:"dispatched_at"`
	DispatchDelayMilliseconds int64     `json:"dispatch_delay_ms"`
}

// ScheduledRunExecution is the exact durable turn-journal checkpoint. Phase is
// intentionally not collapsed: completing/failing/interrupting identify the
// MemoryLayer-to-Aether terminal outbox boundary.
type ScheduledRunExecution struct {
	Revision      uint64            `json:"revision"`
	OwnerIdentity string            `json:"owner_identity"`
	Phase         turnjournal.Phase `json:"phase"`
	FailureReason string            `json:"failure_reason,omitempty"`
	UpdatedAt     time.Time         `json:"updated_at"`
	CompletedAt   *time.Time        `json:"completed_at,omitempty"`
}

// ScheduledRun is a read-only join. TaskStatus/Error/timestamps come from
// Aether; Execution comes from the MemoryLayer-backed journal; Thread is
// MemoryLayer thread metadata. A nil Execution or Thread is expected before the
// worker creates either record and is not filled from another source.
type ScheduledRun struct {
	TaskID            string                 `json:"task_id"`
	TaskStatus        string                 `json:"task_status"`
	TaskError         string                 `json:"task_error,omitempty"`
	RoutingWorkspace  string                 `json:"routing_workspace"`
	AssignedTo        string                 `json:"assigned_to"`
	LogicalWorkspace  string                 `json:"logical_workspace"`
	ThreadID          string                 `json:"thread_id"`
	ViewID            string                 `json:"view_id"`
	ViewRevision      string                 `json:"view_revision"`
	ToolHostID        string                 `json:"tool_host_id"`
	EnvelopeSchema    string                 `json:"envelope_schema"`
	DeclarationDigest string                 `json:"declaration_digest"`
	Occurrence        ScheduledRunOccurrence `json:"occurrence"`
	State             string                 `json:"state"`
	Attempt           int32                  `json:"attempt"`
	MaxAttempts       int32                  `json:"max_attempts"`
	CreatedAt         time.Time              `json:"created_at"`
	StartedAt         *time.Time             `json:"started_at,omitempty"`
	CompletedAt       *time.Time             `json:"completed_at,omitempty"`
	Execution         *ScheduledRunExecution `json:"execution,omitempty"`
	Thread            *threadindex.Session   `json:"thread,omitempty"`
}

// ScheduledRunReader joins the two existing authorities without persisting a
// third projection. It is safe to construct without enabling scheduled-turn
// execution on the local Channel, which makes it reusable by operations hosts.
type ScheduledRunReader struct {
	tasks            ScheduledRunTaskQueries
	journal          ScheduledRunJournal
	threads          threadindex.WorkspaceLookup
	routingWorkspace string
	timeout          time.Duration
}

func NewScheduledRunReader(
	tasks ScheduledRunTaskQueries,
	journal ScheduledRunJournal,
	threads threadindex.WorkspaceLookup,
	routingWorkspace string,
	timeout time.Duration,
) (*ScheduledRunReader, error) {
	if tasks == nil {
		return nil, errors.New("aether: scheduled run task queries are required")
	}
	if journal == nil {
		return nil, errors.New("aether: scheduled run journal is required")
	}
	if threads == nil {
		return nil, errors.New("aether: scheduled run thread lookup is required")
	}
	routingWorkspace = strings.TrimSpace(routingWorkspace)
	if routingWorkspace == "" {
		return nil, errors.New("aether: scheduled run routing workspace is required")
	}
	if timeout <= 0 {
		timeout = defaultTaskTimeout
	}
	return &ScheduledRunReader{
		tasks: tasks, journal: journal, threads: threads,
		routingWorkspace: routingWorkspace, timeout: timeout,
	}, nil
}

// ScheduledRunReader returns a joined reader for this Channel's routing
// workspace. This is additive: standalone/Aether-free operation does not build
// or call the reader.
func (c *Channel) ScheduledRunReader(journal ScheduledRunJournal, threads threadindex.WorkspaceLookup, timeout time.Duration) (*ScheduledRunReader, error) {
	if c == nil {
		return nil, errors.New("aether: channel is required")
	}
	return NewScheduledRunReader(c.client, journal, threads, c.workspace, timeout)
}

// Query returns one joined page. Missing journal/thread rows remain visible as
// task-only results; malformed identity metadata or a backend failure fails the
// page instead of silently returning a partial or cross-workspace join.
func (r *ScheduledRunReader) Query(ctx context.Context, query ScheduledRunQuery) (ScheduledRunPage, error) {
	limit, statuses, err := validateScheduledRunQuery(query)
	if err != nil {
		return ScheduledRunPage{}, err
	}
	response, err := r.tasks.QueryTasks(ctx, &pb.TaskFilter{
		Workspace: r.routingWorkspace,
		TaskType:  ScheduledTurnTaskType,
		Limit:     limit,
		PageToken: query.PageToken,
		Statuses:  statuses,
	}, r.timeout)
	if err != nil {
		return ScheduledRunPage{}, fmt.Errorf("aether: query scheduled runs: %w", err)
	}
	if response == nil {
		return ScheduledRunPage{}, errors.New("aether: scheduled run query returned no response")
	}
	if !response.Success {
		return ScheduledRunPage{}, fmt.Errorf("aether: scheduled run query rejected: %s", strings.TrimSpace(response.Error))
	}

	page := ScheduledRunPage{
		Runs:       make([]ScheduledRun, 0, len(response.Tasks)),
		TotalCount: response.TotalCount, NextPageToken: response.NextPageToken,
	}
	threadCache := make(map[string]*threadindex.Session)
	missingThreads := make(map[string]bool)
	for index, info := range response.Tasks {
		run, err := r.project(ctx, info, threadCache, missingThreads)
		if err != nil {
			return ScheduledRunPage{}, fmt.Errorf("aether: project scheduled run at index %d: %w", index, err)
		}
		page.Runs = append(page.Runs, run)
	}
	return page, nil
}

func validateScheduledRunQuery(query ScheduledRunQuery) (int32, []pb.TaskStatus, error) {
	limit := query.Limit
	if limit == 0 {
		limit = defaultScheduledRunPageSize
	}
	if limit < 0 || limit > maxScheduledRunPageSize {
		return 0, nil, fmt.Errorf("aether: scheduled run page limit must be between 1 and %d", maxScheduledRunPageSize)
	}
	statuses := append([]pb.TaskStatus(nil), query.Statuses...)
	for _, status := range statuses {
		if status == pb.TaskStatus_TASK_STATUS_UNSPECIFIED {
			return 0, nil, errors.New("aether: scheduled run status must not be unspecified")
		}
		if _, known := pb.TaskStatus_name[int32(status)]; !known {
			return 0, nil, fmt.Errorf("aether: unknown scheduled run status %d", status)
		}
	}
	return limit, statuses, nil
}

func (r *ScheduledRunReader) project(
	ctx context.Context,
	info *sdk.TaskInfo,
	threadCache map[string]*threadindex.Session,
	missingThreads map[string]bool,
) (ScheduledRun, error) {
	if info == nil {
		return ScheduledRun{}, errors.New("task projection is nil")
	}
	if info.TaskType != ScheduledTurnTaskType || info.Workspace != r.routingWorkspace {
		return ScheduledRun{}, errors.New("task projection escaped the scheduled-run query scope")
	}
	if strings.TrimSpace(info.TaskID) == "" || strings.TrimSpace(info.AssignedTo) == "" {
		return ScheduledRun{}, errors.New("task projection is missing task or assignee identity")
	}
	if _, known := pb.TaskStatus_value[info.Status]; !known || info.Status == pb.TaskStatus_TASK_STATUS_UNSPECIFIED.String() {
		return ScheduledRun{}, fmt.Errorf("task projection has unknown status %q", info.Status)
	}

	metadata := info.Metadata
	required := []string{
		"scitrera.schedule_id", "scitrera.schedule_schema", "scitrera.schedule_digest",
		"scitrera.schedule_miss_policy", "scitrera.logical_workspace", "scitrera.thread_id",
		"scitrera.view_id", "scitrera.view_revision", "scitrera.execution_tool_host",
		"aether.schedule.id", "aether.schedule.scheduled_for", "aether.schedule.dispatched_at",
		"aether.schedule.miss_policy",
	}
	for _, key := range required {
		if strings.TrimSpace(metadata[key]) == "" {
			return ScheduledRun{}, fmt.Errorf("task projection is missing metadata %q", key)
		}
	}
	if metadata["scitrera.schedule_schema"] != ScheduledTurnEnvelopeSchema {
		return ScheduledRun{}, fmt.Errorf("task projection has unsupported schedule schema %q", metadata["scitrera.schedule_schema"])
	}
	digest := strings.TrimPrefix(metadata["scitrera.schedule_digest"], scheduledTurnDeclarationDigest)
	if len(digest) != 64 {
		return ScheduledRun{}, errors.New("task projection has an invalid declaration digest")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return ScheduledRun{}, errors.New("task projection has an invalid declaration digest")
	}
	if metadata["scitrera.schedule_miss_policy"] != metadata["aether.schedule.miss_policy"] {
		return ScheduledRun{}, errors.New("task projection has conflicting schedule miss policies")
	}
	switch metadata["aether.schedule.miss_policy"] {
	case ScheduledMissPolicySkip, ScheduledMissPolicyFireOnce, ScheduledMissPolicyFireAll:
	default:
		return ScheduledRun{}, errors.New("task projection has an unknown schedule miss policy")
	}
	if metadata["scitrera.execution_tool_host"] != info.AssignedTo {
		return ScheduledRun{}, errors.New("task projection assignee does not match its execution tool host")
	}
	wantScheduleID := scheduledWorkflowID(r.routingWorkspace, info.AssignedTo, metadata["scitrera.schedule_id"])
	if metadata["aether.schedule.id"] != wantScheduleID {
		return ScheduledRun{}, errors.New("task projection has an unexpected workflow schedule id")
	}
	scheduledFor, err := time.Parse(time.RFC3339Nano, metadata["aether.schedule.scheduled_for"])
	if err != nil {
		return ScheduledRun{}, fmt.Errorf("parse scheduled occurrence: %w", err)
	}
	dispatchedAt, err := time.Parse(time.RFC3339Nano, metadata["aether.schedule.dispatched_at"])
	if err != nil {
		return ScheduledRun{}, fmt.Errorf("parse scheduled dispatch: %w", err)
	}
	if dispatchedAt.Before(scheduledFor) {
		return ScheduledRun{}, errors.New("scheduled dispatch precedes its occurrence")
	}
	if info.CreatedAt <= 0 {
		return ScheduledRun{}, errors.New("task projection is missing its creation timestamp")
	}
	scheduledFor = scheduledFor.UTC()
	dispatchedAt = dispatchedAt.UTC()

	run := ScheduledRun{
		TaskID: info.TaskID, TaskStatus: info.Status, TaskError: info.Error,
		RoutingWorkspace: info.Workspace, AssignedTo: info.AssignedTo,
		LogicalWorkspace: metadata["scitrera.logical_workspace"], ThreadID: metadata["scitrera.thread_id"],
		ViewID: metadata["scitrera.view_id"], ViewRevision: metadata["scitrera.view_revision"],
		ToolHostID: metadata["scitrera.execution_tool_host"], EnvelopeSchema: metadata["scitrera.schedule_schema"],
		DeclarationDigest: metadata["scitrera.schedule_digest"],
		Occurrence: ScheduledRunOccurrence{
			ScheduleID: metadata["aether.schedule.id"], DeclarationID: metadata["scitrera.schedule_id"],
			MissPolicy: metadata["aether.schedule.miss_policy"], ScheduledFor: scheduledFor, DispatchedAt: dispatchedAt,
			DispatchDelayMilliseconds: dispatchedAt.Sub(scheduledFor).Milliseconds(),
		},
		Attempt: info.Attempt, MaxAttempts: info.MaxAttempts,
		CreatedAt: time.Unix(info.CreatedAt, 0).UTC(), StartedAt: optionalUnixTime(info.StartedAt),
		CompletedAt: optionalUnixTime(info.CompletedAt),
	}

	record, err := r.journal.Get(ctx, run.LogicalWorkspace, run.TaskID)
	if err != nil && !errors.Is(err, turnjournal.ErrNotFound) {
		return ScheduledRun{}, fmt.Errorf("read execution journal: %w", err)
	}
	if err == nil {
		if err := record.Validate(); err != nil {
			return ScheduledRun{}, fmt.Errorf("validate execution journal: %w", err)
		}
		if record.WorkspaceID != run.LogicalWorkspace || record.SessionID != run.ThreadID || record.TaskID != run.TaskID {
			return ScheduledRun{}, errors.New("execution journal identity does not match scheduled task")
		}
		run.Execution = &ScheduledRunExecution{
			Revision: record.Revision, OwnerIdentity: record.OwnerIdentity, Phase: record.Phase,
			FailureReason: record.FailureReason, UpdatedAt: record.UpdatedAt, CompletedAt: record.CompletedAt,
		}
	}
	run.State = scheduledRunState(info.Status, run.Execution)

	threadKey := run.LogicalWorkspace + "\x00" + run.ThreadID
	if cached, ok := threadCache[threadKey]; ok {
		copy := *cached
		run.Thread = &copy
	} else if !missingThreads[threadKey] {
		session, found, err := r.threads.LookupWorkspaceThread(ctx, run.LogicalWorkspace, run.ThreadID)
		if err != nil {
			return ScheduledRun{}, fmt.Errorf("read scheduled thread: %w", err)
		}
		if found {
			if session.ID != run.ThreadID {
				return ScheduledRun{}, errors.New("thread lookup identity does not match scheduled task")
			}
			threadCache[threadKey] = &session
			copy := session
			run.Thread = &copy
		} else {
			missingThreads[threadKey] = true
		}
	}
	return run, nil
}

func scheduledRunState(taskStatus string, execution *ScheduledRunExecution) string {
	if execution != nil {
		return string(execution.Phase)
	}
	switch taskStatus {
	case pb.TaskStatus_TASK_STATUS_QUEUED.String():
		return "queued"
	case pb.TaskStatus_TASK_STATUS_RUNNING.String():
		return "admitted"
	case pb.TaskStatus_TASK_STATUS_COMPLETED.String():
		return "completed_without_journal"
	case pb.TaskStatus_TASK_STATUS_FAILED.String():
		return "pre_execution_failed"
	case pb.TaskStatus_TASK_STATUS_CANCELLED.String():
		return "pre_execution_cancelled"
	case pb.TaskStatus_TASK_STATUS_REJECTED.String():
		return "pre_execution_rejected"
	case pb.TaskStatus_TASK_STATUS_HIBERNATED.String():
		return "hibernated"
	default:
		return "waiting"
	}
}

func optionalUnixTime(value int64) *time.Time {
	if value == 0 {
		return nil
	}
	parsed := time.Unix(value, 0).UTC()
	return &parsed
}
