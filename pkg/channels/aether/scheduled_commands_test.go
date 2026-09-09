// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

type fakeScheduledStateLister struct {
	states []ScheduledTurnScheduleState
	err    error
	calls  int
}

func (f *fakeScheduledStateLister) ListScheduledTurnScheduleStates(context.Context) ([]ScheduledTurnScheduleState, error) {
	f.calls++
	return f.states, f.err
}

type fakeScheduledRunQuerier struct {
	page  ScheduledRunPage
	err   error
	query ScheduledRunQuery
	calls int
}

func (f *fakeScheduledRunQuerier) Query(_ context.Context, query ScheduledRunQuery) (ScheduledRunPage, error) {
	f.calls++
	f.query = query
	return f.page, f.err
}

type fakeScheduledOperationsAuthorizer struct {
	addr    protocol.MessageAddress
	userID  string
	command string
	err     error
	calls   int
}

func (f *fakeScheduledOperationsAuthorizer) AuthorizeScheduledOperations(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, command string) error {
	f.calls++
	f.addr = addr
	f.userID = user.ID
	f.command = command
	return f.err
}

func TestScheduledOperationsCommandsRendersAuthoritativeScheduleSkip(t *testing.T) {
	lastFired := time.Date(2026, 8, 10, 11, 0, 0, 0, time.UTC)
	scheduledFor := lastFired.Add(time.Hour)
	lister := &fakeScheduledStateLister{states: []ScheduledTurnScheduleState{{
		DeclarationID: "daily-review", ScheduleType: "cron", ScheduleExpression: "0 9 * * *",
		LogicalWorkspace: "project-a", ThreadID: "scheduled-daily", ViewID: "view-a", ViewRevision: "rev-a",
		AssignedTo: "ag::routing::agent-harness::worker", MissPolicy: ScheduledMissPolicySkip, Enabled: true,
		LastFiredAt: &lastFired,
		LastOccurrence: &ScheduledTurnScheduleOccurrence{
			ScheduledFor: scheduledFor, Disposition: ScheduledDispositionSkipped,
			Reason: ScheduledSkipReasonMissPolicy, BacklogCount: scheduledTurnBacklogDetailLimit,
			BacklogTruncated: true,
		},
	}}}
	querier := &fakeScheduledRunQuerier{}
	commands := &ScheduledOperationsCommands{schedules: lister, runs: querier}

	text, err := commands.RunScheduledOperationsCommand(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{}, "schedules", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Aether WorkflowEngine authoritative", "daily-review [enabled]", "workspace=project-a",
		"latest=skipped reason=miss_policy", "backlog=101+", "last-fired=2026-08-10T11:00:00Z",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("schedule output missing %q:\n%s", want, text)
		}
	}
	if lister.calls != 1 || querier.calls != 0 {
		t.Fatalf("calls lister=%d querier=%d", lister.calls, querier.calls)
	}
}

func TestScheduledOperationsCommandsPreservesRunFiltersAndOpaqueCursor(t *testing.T) {
	scheduledFor := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	dispatchedAt := scheduledFor.Add(3 * time.Second)
	lister := &fakeScheduledStateLister{}
	querier := &fakeScheduledRunQuerier{page: ScheduledRunPage{
		TotalCount: 4, NextPageToken: "next_opaque==",
		Runs: []ScheduledRun{{
			TaskID: "task-1", TaskStatus: pb.TaskStatus_TASK_STATUS_FAILED.String(), State: "failed",
			TaskError: "boom\nspoof", LogicalWorkspace: "project-a", ThreadID: "scheduled-daily",
			ViewID: "view-a", ViewRevision: "rev-a", AssignedTo: "ag::routing::agent-harness::worker",
			Occurrence: ScheduledRunOccurrence{
				DeclarationID: "daily-review", Disposition: ScheduledDispositionCoalesced,
				BacklogCount: 4, BacklogIndex: 1, ScheduledFor: scheduledFor, DispatchedAt: dispatchedAt,
				DispatchDelayMilliseconds: 3000,
			},
			Thread: &threadindex.Session{ID: "scheduled-daily", Title: "Daily\nreview"},
		}},
	}}
	authorizer := &fakeScheduledOperationsAuthorizer{}
	commands := &ScheduledOperationsCommands{schedules: lister, runs: querier, authorizer: authorizer}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "ops", TaskID: "task-command"}
	user := protocol.ChatMessage{ID: "user-command"}

	text, err := commands.RunScheduledOperationsCommand(
		context.Background(), addr, user, "runs",
		"--status failed,waiting-input --limit=7 --cursor first_opaque==",
	)
	if err != nil {
		t.Fatal(err)
	}
	if querier.calls != 1 || querier.query.Limit != 7 || querier.query.PageToken != "first_opaque==" ||
		len(querier.query.Statuses) != 2 || querier.query.Statuses[0] != pb.TaskStatus_TASK_STATUS_FAILED ||
		querier.query.Statuses[1] != pb.TaskStatus_TASK_STATUS_WAITING_INPUT {
		t.Fatalf("run query = %+v", querier.query)
	}
	if authorizer.calls != 1 || authorizer.addr.WorkspaceID != addr.WorkspaceID || authorizer.addr.ThreadID != addr.ThreadID ||
		authorizer.addr.TaskID != addr.TaskID || authorizer.userID != user.ID || authorizer.command != "runs" {
		t.Fatalf("authorization = %+v", authorizer)
	}
	for _, want := range []string{
		"task=Aether; execution=turn journal (Aether KV in OSS); thread=MemoryLayer when present",
		"task-1 task=failed state=failed declaration=daily-review", "disposition=coalesced backlog=4 index=1 delay=3000ms",
		`title="Daily review"`, `error="boom spoof"`,
		"Next page: /runs --status failed,waiting_input --limit 7 --cursor next_opaque==",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("run output missing %q:\n%s", want, text)
		}
	}
}

func TestScheduledOperationsCommandsKeepsUsageLocalAndFailuresAuthoritative(t *testing.T) {
	lister := &fakeScheduledStateLister{}
	querier := &fakeScheduledRunQuerier{}
	commands := &ScheduledOperationsCommands{schedules: lister, runs: querier}

	text, err := commands.RunScheduledOperationsCommand(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{}, "runs", "--status future")
	if err != nil || !strings.Contains(text, scheduledRunsCommandUsage) || !strings.Contains(text, `unknown status "future"`) || querier.calls != 0 {
		t.Fatalf("usage text=%q err=%v calls=%d", text, err, querier.calls)
	}
	querier.page.NextPageToken = "bad\ncursor"
	if _, err := commands.RunScheduledOperationsCommand(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{}, "runs", ""); err == nil ||
		!strings.Contains(err.Error(), "invalid next-page cursor") {
		t.Fatalf("invalid server cursor error = %v", err)
	}

	authorizer := &fakeScheduledOperationsAuthorizer{err: errors.New("denied")}
	commands.authorizer = authorizer
	before := querier.calls
	if _, err := commands.RunScheduledOperationsCommand(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{}, "runs", ""); err == nil ||
		!strings.Contains(err.Error(), "denied") || querier.calls != before {
		t.Fatalf("authorization error=%v calls=%d", err, querier.calls)
	}
}
