package aether

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

type fakeScheduledRunJournal struct {
	records map[string]turnjournal.Record
	err     error
}

func (f *fakeScheduledRunJournal) Get(_ context.Context, workspaceID, taskID string) (turnjournal.Record, error) {
	if f.err != nil {
		return turnjournal.Record{}, f.err
	}
	record, ok := f.records[workspaceID+"\x00"+taskID]
	if !ok {
		return turnjournal.Record{}, turnjournal.ErrNotFound
	}
	return record, nil
}

type fakeScheduledRunThreads struct {
	sessions map[string]threadindex.Session
	err      error
	calls    []string
}

func (f *fakeScheduledRunThreads) LookupWorkspaceThread(_ context.Context, workspaceID, id string) (threadindex.Session, bool, error) {
	key := workspaceID + "\x00" + id
	f.calls = append(f.calls, key)
	if f.err != nil {
		return threadindex.Session{}, false, f.err
	}
	session, ok := f.sessions[key]
	return session, ok, nil
}

func TestScheduledRunReaderJoinsAuthoritiesAndPreservesCursor(t *testing.T) {
	queued := scheduledRunTask(t, "task-queued", pb.TaskStatus_TASK_STATUS_QUEUED)
	running := scheduledRunTask(t, "task-running", pb.TaskStatus_TASK_STATUS_RUNNING)
	operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{{
		Success: true, Tasks: []*sdk.TaskInfo{queued, running}, TotalCount: 9,
		NextPageToken: "opaque:updated-at/task-id==",
	}}}
	journal := &fakeScheduledRunJournal{records: map[string]turnjournal.Record{
		"project-a\x00task-running": scheduledJournalRecord("task-running", turnjournal.PhaseProviderPending),
	}}
	threads := &fakeScheduledRunThreads{sessions: map[string]threadindex.Session{
		"project-a\x00scheduled-daily-review": {
			ID: "scheduled-daily-review", Title: "Daily review", Created: 10, Updated: 20,
		},
	}}
	reader, err := NewScheduledRunReader(operations, journal, threads, "routing", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.Query(context.Background(), ScheduledRunQuery{
		Limit: 2, PageToken: "incoming-opaque-token",
		Statuses: []pb.TaskStatus{pb.TaskStatus_TASK_STATUS_QUEUED, pb.TaskStatus_TASK_STATUS_RUNNING},
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalCount != 9 || page.NextPageToken != "opaque:updated-at/task-id==" || len(page.Runs) != 2 {
		t.Fatalf("page = %+v", page)
	}
	if page.Runs[0].State != "queued" || page.Runs[0].Execution != nil || page.Runs[0].Thread == nil {
		t.Fatalf("queued run = %+v", page.Runs[0])
	}
	if page.Runs[1].State != string(turnjournal.PhaseProviderPending) || page.Runs[1].Execution == nil ||
		page.Runs[1].Execution.Revision != 1 || page.Runs[1].Thread == nil {
		t.Fatalf("running run = %+v", page.Runs[1])
	}
	if page.Runs[1].Occurrence.DeclarationID != "daily-review" ||
		page.Runs[1].Occurrence.ScheduleID != scheduledWorkflowID("routing", running.AssignedTo, "daily-review") ||
		page.Runs[1].Occurrence.MissPolicy != ScheduledMissPolicyFireOnce || page.Runs[1].Occurrence.Disposition != ScheduledDispositionOrdinary ||
		page.Runs[1].Occurrence.BacklogCount != 1 || page.Runs[1].Occurrence.BacklogIndex != 1 ||
		page.Runs[1].Occurrence.DispatchDelayMilliseconds != 3000 {
		t.Fatalf("occurrence = %+v", page.Runs[1].Occurrence)
	}
	if len(threads.calls) != 1 {
		t.Fatalf("thread lookups = %v, want one cached read", threads.calls)
	}
	if len(operations.listCalls) != 1 {
		t.Fatalf("task calls = %d", len(operations.listCalls))
	}
	filter := operations.listCalls[0]
	if filter.Workspace != "routing" || filter.TaskType != ScheduledTurnTaskType || filter.Limit != 2 ||
		filter.PageToken != "incoming-opaque-token" ||
		!reflect.DeepEqual(filter.Statuses, []pb.TaskStatus{pb.TaskStatus_TASK_STATUS_QUEUED, pb.TaskStatus_TASK_STATUS_RUNNING}) {
		t.Fatalf("task filter = %+v", filter)
	}
}

func TestScheduledRunReaderKeepsExpectedMissingRowsVisible(t *testing.T) {
	failed := scheduledRunTask(t, "task-failed-before-execution", pb.TaskStatus_TASK_STATUS_FAILED)
	failed.Error = "claim was rejected"
	operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{{
		Success: true, Tasks: []*sdk.TaskInfo{failed}, TotalCount: 1,
	}}}
	reader, err := NewScheduledRunReader(
		operations,
		&fakeScheduledRunJournal{records: map[string]turnjournal.Record{}},
		&fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}},
		"routing", 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.Query(context.Background(), ScheduledRunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].State != "pre_execution_failed" ||
		page.Runs[0].TaskError != "claim was rejected" || page.Runs[0].Execution != nil || page.Runs[0].Thread != nil {
		t.Fatalf("run = %+v", page.Runs)
	}
	if operations.listCalls[0].Limit != defaultScheduledRunPageSize {
		t.Fatalf("default limit = %d", operations.listCalls[0].Limit)
	}
}

func TestScheduledRunReaderAcceptsMutableViewWithoutRevision(t *testing.T) {
	task := scheduledRunTask(t, "task-mutable-view", pb.TaskStatus_TASK_STATUS_QUEUED)
	task.Metadata["scitrera.view_revision"] = ""
	operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{{Success: true, Tasks: []*sdk.TaskInfo{task}}}}
	reader, err := NewScheduledRunReader(
		operations, &fakeScheduledRunJournal{records: map[string]turnjournal.Record{}},
		&fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}}, "routing", time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.Query(context.Background(), ScheduledRunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].ViewRevision != "" {
		t.Fatalf("mutable-view projection = %+v", page.Runs)
	}
	delete(task.Metadata, "scitrera.view_revision")
	operations.listCalls = nil
	operations.listResponses = []*sdk.TaskQueryResponse{{Success: true, Tasks: []*sdk.TaskInfo{task}}}
	if _, err := reader.Query(context.Background(), ScheduledRunQuery{}); err == nil || !strings.Contains(err.Error(), "scitrera.view_revision") {
		t.Fatalf("missing revision key error = %v", err)
	}
}

func TestScheduledRunReaderProjectsExactOccurrenceDispositions(t *testing.T) {
	tests := []struct {
		name        string
		policy      string
		disposition string
		count       string
		truncated   string
		index       string
	}{
		{name: "coalesced", policy: ScheduledMissPolicyFireOnce, disposition: ScheduledDispositionCoalesced, count: "4", truncated: "false", index: "1"},
		{name: "catch up", policy: ScheduledMissPolicyFireAll, disposition: ScheduledDispositionCatchUp, count: "101", truncated: "true", index: "37"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := scheduledRunTask(t, "task-"+test.disposition, pb.TaskStatus_TASK_STATUS_QUEUED)
			task.Metadata["scitrera.schedule_miss_policy"] = test.policy
			task.Metadata["aether.schedule.miss_policy"] = test.policy
			task.Metadata["aether.schedule.disposition"] = test.disposition
			task.Metadata["aether.schedule.backlog_count"] = test.count
			task.Metadata["aether.schedule.backlog_truncated"] = test.truncated
			task.Metadata["aether.schedule.backlog_index"] = test.index
			operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{{Success: true, Tasks: []*sdk.TaskInfo{task}}}}
			reader, err := NewScheduledRunReader(
				operations, &fakeScheduledRunJournal{records: map[string]turnjournal.Record{}},
				&fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}}, "routing", time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.Query(context.Background(), ScheduledRunQuery{})
			if err != nil {
				t.Fatal(err)
			}
			got := page.Runs[0].Occurrence
			if got.Disposition != test.disposition || got.BacklogCount != mustAtoi(t, test.count) ||
				got.BacklogTruncated != (test.truncated == "true") || got.BacklogIndex != mustAtoi(t, test.index) {
				t.Fatalf("occurrence = %+v", got)
			}
		})
	}
}

func TestScheduledRunReaderFailsClosedOnCrossSourceIdentityDrift(t *testing.T) {
	task := scheduledRunTask(t, "task-drift", pb.TaskStatus_TASK_STATUS_RUNNING)
	tests := []struct {
		name    string
		mutate  func(*sdk.TaskInfo)
		journal *fakeScheduledRunJournal
		threads *fakeScheduledRunThreads
		want    string
	}{
		{
			name: "metadata", mutate: func(info *sdk.TaskInfo) { info.Metadata["aether.schedule.id"] = "another-schedule" },
			journal: &fakeScheduledRunJournal{records: map[string]turnjournal.Record{}},
			threads: &fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}}, want: "workflow schedule id",
		},
		{
			name: "occurrence", mutate: func(info *sdk.TaskInfo) { info.Metadata["aether.schedule.disposition"] = ScheduledDispositionSkipped },
			journal: &fakeScheduledRunJournal{records: map[string]turnjournal.Record{}},
			threads: &fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}}, want: "unknown schedule disposition",
		},
		{
			name: "journal session", mutate: func(*sdk.TaskInfo) {},
			journal: &fakeScheduledRunJournal{records: map[string]turnjournal.Record{
				"project-a\x00task-drift": scheduledJournalRecordForSession("task-drift", "another-thread"),
			}},
			threads: &fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}}, want: "journal identity",
		},
		{
			name: "thread identity", mutate: func(*sdk.TaskInfo) {},
			journal: &fakeScheduledRunJournal{records: map[string]turnjournal.Record{}},
			threads: &fakeScheduledRunThreads{sessions: map[string]threadindex.Session{
				"project-a\x00scheduled-daily-review": {ID: "another-thread"},
			}}, want: "thread lookup identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			copy := *task
			copy.Metadata = make(map[string]string, len(task.Metadata))
			for key, value := range task.Metadata {
				copy.Metadata[key] = value
			}
			test.mutate(&copy)
			operations := &fakeTaskOperations{listResponses: []*sdk.TaskQueryResponse{{
				Success: true, Tasks: []*sdk.TaskInfo{&copy}, TotalCount: 1,
			}}}
			reader, err := NewScheduledRunReader(operations, test.journal, test.threads, "routing", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.Query(context.Background(), ScheduledRunQuery{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want %q", err, test.want)
			}
		})
	}
}

func mustAtoi(t *testing.T, value string) int {
	t.Helper()
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestScheduledRunReaderPropagatesAuthorityFailuresAndValidatesQuery(t *testing.T) {
	task := scheduledRunTask(t, "task-1", pb.TaskStatus_TASK_STATUS_RUNNING)
	backendError := errors.New("backend unavailable")
	tests := []struct {
		name     string
		query    ScheduledRunQuery
		journal  *fakeScheduledRunJournal
		threads  *fakeScheduledRunThreads
		response *sdk.TaskQueryResponse
		want     string
	}{
		{name: "limit", query: ScheduledRunQuery{Limit: maxScheduledRunPageSize + 1}, want: "page limit"},
		{name: "status", query: ScheduledRunQuery{Statuses: []pb.TaskStatus{pb.TaskStatus_TASK_STATUS_UNSPECIFIED}}, want: "unspecified"},
		{name: "rejected", response: &sdk.TaskQueryResponse{Error: "denied"}, want: "denied"},
		{name: "journal", response: &sdk.TaskQueryResponse{Success: true, Tasks: []*sdk.TaskInfo{task}},
			journal: &fakeScheduledRunJournal{err: backendError}, want: "execution journal"},
		{name: "thread", response: &sdk.TaskQueryResponse{Success: true, Tasks: []*sdk.TaskInfo{task}},
			threads: &fakeScheduledRunThreads{err: backendError}, want: "scheduled thread"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operations := &fakeTaskOperations{}
			if test.response != nil {
				operations.listResponses = []*sdk.TaskQueryResponse{test.response}
			}
			journal := test.journal
			if journal == nil {
				journal = &fakeScheduledRunJournal{records: map[string]turnjournal.Record{}}
			}
			threads := test.threads
			if threads == nil {
				threads = &fakeScheduledRunThreads{sessions: map[string]threadindex.Session{}}
			}
			reader, err := NewScheduledRunReader(operations, journal, threads, "routing", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.Query(context.Background(), test.query)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want %q", err, test.want)
			}
		})
	}
}

func scheduledRunTask(t *testing.T, taskID string, status pb.TaskStatus) *sdk.TaskInfo {
	t.Helper()
	registration := scheduledRegistration()
	envelope, err := scheduledTurnEnvelopeFor(registration)
	if err != nil {
		t.Fatal(err)
	}
	metadata := scheduledTurnMetadata(envelope)
	metadata["aether.schedule.id"] = scheduledWorkflowID("routing", registration.Binding.ToolHostID, registration.ID)
	metadata["aether.schedule.scheduled_for"] = "2026-08-10T12:00:00Z"
	metadata["aether.schedule.dispatched_at"] = "2026-08-10T12:00:03Z"
	metadata["aether.schedule.miss_policy"] = registration.MissPolicy
	metadata["aether.schedule.disposition"] = ScheduledDispositionOrdinary
	metadata["aether.schedule.backlog_count"] = "1"
	metadata["aether.schedule.backlog_truncated"] = "false"
	metadata["aether.schedule.backlog_index"] = "1"
	return &sdk.TaskInfo{
		TaskID: taskID, TaskType: ScheduledTurnTaskType, Status: status.String(),
		Workspace: "routing", AssignedTo: registration.Binding.ToolHostID, Metadata: metadata,
		CreatedAt: 1786363203, Attempt: 1, MaxAttempts: 1,
	}
}

func scheduledJournalRecord(taskID string, phase turnjournal.Phase) turnjournal.Record {
	return scheduledJournalRecordForSessionAndPhase(taskID, "scheduled-daily-review", phase)
}

func scheduledJournalRecordForSession(taskID, sessionID string) turnjournal.Record {
	return scheduledJournalRecordForSessionAndPhase(taskID, sessionID, turnjournal.PhaseProviderPending)
}

func scheduledJournalRecordForSessionAndPhase(taskID, sessionID string, phase turnjournal.Phase) turnjournal.Record {
	now := time.Date(2026, 8, 10, 12, 0, 4, 0, time.UTC)
	return turnjournal.Record{
		SchemaRevision: turnjournal.SchemaRevision, Schema: turnjournal.Schema, Revision: 1,
		WorkspaceID: "project-a", SessionID: sessionID, TaskID: taskID, OwnerIdentity: "worker-1",
		Phase: phase,
		Input: turnjournal.HistoryMessageRef{
			WorkspaceID: "project-a", SessionID: sessionID, MessageID: "input-1",
			Digest: strings.Repeat("0", 64),
		},
		CreatedAt: now, UpdatedAt: now,
	}
}
