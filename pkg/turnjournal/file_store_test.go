package turnjournal

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testDigestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testDigestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testDigestC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func newTestStore(t *testing.T) (*FileStore, *time.Time) {
	t.Helper()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	return store, &now
}

func preparedRecord(workspace, session, task, owner string) Record {
	return Record{
		WorkspaceID:   workspace,
		SessionID:     session,
		TaskID:        task,
		OwnerIdentity: owner,
		Phase:         PhasePrepared,
		Input: HistoryMessageRef{
			WorkspaceID: workspace,
			SessionID:   session,
			MessageID:   "user-message-1",
			Digest:      testDigestA,
		},
	}
}

func TestFileStore_RestartRoundTripAndRecoverableChildFlow(t *testing.T) {
	store, now := newTestStore(t)
	ctx := context.Background()
	record, err := store.Create(ctx, preparedRecord("project-a", "thread-1", "task-parent", "agent-parent"))
	if err != nil {
		t.Fatal(err)
	}
	if record.Revision != 1 || record.Schema != Schema || record.CreatedAt != *now {
		t.Fatalf("created record = %+v", record)
	}

	*now = now.Add(time.Second)
	record.Phase = PhaseProviderPending
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	record.Phase = PhaseToolPending
	record.LastMessageID = "assistant-tool-1"
	record.Tool = &ToolCheckpoint{
		InvocationID: "call-1",
		Name:         "spawn_subagent",
		ArgsDigest:   testDigestB,
		Assistant: HistoryMessageRef{
			WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "assistant-tool-1", Digest: testDigestC,
		},
		Outcome: ToolOutcomeRequested,
	}
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	record.Phase = PhaseWaitingExternalChild
	record.Tool.Outcome = ToolOutcomeAdmitted
	external, err := NewExternalChildRef("task-child", "ahx-v1-child", "thread-child", []byte(`{"schema":"child.v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	record.Tool.External = &external
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileStore(store.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Get(ctx, "project-a", "task-parent")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != PhaseWaitingExternalChild || loaded.Tool == nil || loaded.Tool.External == nil || loaded.Tool.External.TaskID != "task-child" {
		t.Fatalf("restart lost child checkpoint: %+v", loaded)
	}
	active, err := reopened.ListActive(ctx, "agent-parent")
	if err != nil || len(active) != 1 || active[0].TaskID != "task-parent" {
		t.Fatalf("ListActive = %+v, err=%v", active, err)
	}

	reopened.now = func() time.Time { return now.Add(time.Second) }
	loaded.Phase = PhaseChildResolved
	loaded.Tool.Outcome = ToolOutcomeConfirmed
	loaded.Tool.Result = &HistoryMessageRef{WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "tool-result-1", Digest: testDigestA}
	loaded.LastMessageID = "tool-result-1"
	loaded, err = reopened.Update(ctx, loaded, loaded.Revision)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Phase = PhaseProviderPending
	loaded.Iteration = 1
	loaded.Tool = nil
	loaded, err = reopened.Update(ctx, loaded, loaded.Revision)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Phase = PhaseCompleted
	loaded.LastMessageID = "assistant-final-1"
	loaded, err = reopened.Update(ctx, loaded, loaded.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Terminal() || loaded.CompletedAt == nil || loaded.Revision != 7 {
		t.Fatalf("terminal record = %+v", loaded)
	}
	active, err = reopened.ListActive(ctx, "agent-parent")
	if err != nil || len(active) != 0 {
		t.Fatalf("terminal execution remained active: %+v, err=%v", active, err)
	}
}

func TestFileStore_OptimisticRevisionAllowsOneConcurrentWriter(t *testing.T) {
	store, _ := newTestStore(t)
	base, err := store.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner"))
	if err != nil {
		t.Fatal(err)
	}
	one := base
	one.Phase = PhaseProviderPending
	two := one

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, candidate := range []Record{one, two} {
		wg.Add(1)
		go func(record Record) {
			defer wg.Done()
			<-start
			_, err := store.Update(context.Background(), record, base.Revision)
			errs <- err
		}(candidate)
	}
	close(start)
	wg.Wait()
	close(errs)
	var successes, conflicts int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected update error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestFileStore_ListActiveIsOwnerScopedAndDeterministic(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	for _, record := range []Record{
		preparedRecord("project-b", "thread-2", "task-z", "owner-a"),
		preparedRecord("project-a", "thread-2", "task-b", "owner-a"),
		preparedRecord("project-a", "thread-1", "task-c", "owner-a"),
		preparedRecord("project-a", "thread-1", "task-a", "owner-b"),
	} {
		if _, err := store.Create(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.ListActive(ctx, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"project-a/thread-1/task-c", "project-a/thread-2/task-b", "project-b/thread-2/task-z"}
	if len(got) != len(want) {
		t.Fatalf("ListActive = %+v", got)
	}
	for i, record := range got {
		value := strings.Join([]string{record.WorkspaceID, record.SessionID, record.TaskID}, "/")
		if value != want[i] {
			t.Fatalf("ListActive[%d] = %q, want %q", i, value, want[i])
		}
	}
}

func TestFileStore_PendingTerminalIntentRemainsRecoverableUntilAcknowledged(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	record, err := store.Create(ctx, preparedRecord("project-a", "thread-1", "task-1", "owner"))
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseCompleting
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if record.Terminal() || !record.PendingTerminal() || record.CompletedAt != nil {
		t.Fatalf("pending intent = %+v", record)
	}
	active, err := store.ListActive(ctx, "owner")
	if err != nil || len(active) != 1 || active[0].Phase != PhaseCompleting {
		t.Fatalf("active intent = %+v err=%v", active, err)
	}
	changedIntent := record
	changedIntent.Phase = PhaseFailing
	changedIntent.FailureReason = "must not change completion to failure"
	if _, err := store.Update(ctx, changedIntent, changedIntent.Revision); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("changed terminal intent error = %v", err)
	}
	record.Phase = PhaseCompleted
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Terminal() || record.CompletedAt == nil {
		t.Fatalf("acknowledged intent = %+v", record)
	}
}

func TestFileStore_RejectsUnsafeTransitionAndUncertainMutationCanOnlyInterrupt(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	record, err := store.Create(ctx, preparedRecord("project-a", "thread-1", "task-1", "owner"))
	if err != nil {
		t.Fatal(err)
	}
	bad := record
	bad.Phase = PhaseWaitingExternalChild
	if _, err := store.Update(ctx, bad, bad.Revision); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unsafe transition error = %v", err)
	}

	record.Phase = PhaseProviderPending
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseToolPending
	record.Tool = &ToolCheckpoint{
		InvocationID: "call-1", Name: "write_file", ArgsDigest: testDigestB,
		Assistant: HistoryMessageRef{WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "assistant-1", Digest: testDigestC},
		Outcome:   ToolOutcomeRequested,
	}
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseInterrupted
	record.Tool.Outcome = ToolOutcomeUncertain
	record.FailureReason = "write_file outcome was not confirmed"
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if record.CompletedAt == nil || record.Tool.Outcome != ToolOutcomeUncertain {
		t.Fatalf("uncertain mutation was not terminal: %+v", record)
	}
	record.Phase = PhaseProviderPending
	if _, err := store.Update(ctx, record, record.Revision); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal replay error = %v", err)
	}
}

func TestFileStore_ExternalChildIdentitySurvivesFailureAndCannotBeReplaced(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	record, err := store.Create(ctx, preparedRecord("project-a", "thread-1", "task-parent", "owner"))
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseProviderPending
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseToolPending
	record.Tool = &ToolCheckpoint{
		InvocationID: "call-1", Name: "spawn_subagent", ArgsDigest: testDigestB,
		Assistant: HistoryMessageRef{WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "assistant-1", Digest: testDigestC},
		Outcome:   ToolOutcomeRequested,
	}
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseWaitingExternalChild
	record.Tool.Outcome = ToolOutcomeAdmitted
	external, err := NewExternalChildRef("task-child", "execution-child", "thread-child", []byte(`{"schema":"child.v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	record.Tool.External = &external
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}

	changed := cloneRecord(record)
	changed.Tool.External.TaskID = "different-child"
	if _, err := store.Update(ctx, changed, changed.Revision); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("changed child identity error = %v", err)
	}
	dropped := cloneRecord(record)
	dropped.Phase = PhaseFailed
	dropped.Tool = nil
	dropped.FailureReason = "child failed"
	if _, err := store.Update(ctx, dropped, dropped.Revision); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("dropped child checkpoint error = %v", err)
	}

	record.Phase = PhaseFailed
	record.FailureReason = "authoritative child task failed"
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if record.Tool == nil || record.Tool.External == nil || record.Tool.External.TaskID != "task-child" || record.CompletedAt == nil {
		t.Fatalf("terminal child failure lost identity: %+v", record)
	}
}

func TestFileStore_ConfirmedToolCanAdvanceToAnotherToolWithoutLosingSafety(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	record, err := store.Create(ctx, preparedRecord("project-a", "thread-1", "task-1", "owner"))
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseProviderPending
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseToolPending
	record.Tool = &ToolCheckpoint{
		InvocationID: "call-1", Name: "read_file", ArgsDigest: testDigestA,
		Assistant: HistoryMessageRef{WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "assistant-1", Digest: testDigestB},
		Outcome:   ToolOutcomeRequested,
	}
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseProviderPending
	record.Tool.Outcome = ToolOutcomeConfirmed
	record.Tool.Result = &HistoryMessageRef{WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "call-1-result", Digest: testDigestC}
	record.LastMessageID = "call-1-result"
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = PhaseToolPending
	record.Tool = &ToolCheckpoint{
		InvocationID: "call-2", Name: "read_file", ArgsDigest: testDigestB,
		Assistant: HistoryMessageRef{WorkspaceID: "project-a", SessionID: "thread-1", MessageID: "assistant-1", Digest: testDigestB},
		Outcome:   ToolOutcomeRequested,
	}
	record, err = store.Update(ctx, record, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if record.Tool.InvocationID != "call-2" || record.Phase != PhaseToolPending {
		t.Fatalf("next tool checkpoint = %+v", record)
	}
}

func TestFileStore_FailsClosedOnUnknownTrailingAndMismatchedState(t *testing.T) {
	for _, mutate := range []func([]byte) []byte{
		func(data []byte) []byte {
			return []byte(strings.Replace(string(data), `"schema":`, `"unknown":true,"schema":`, 1))
		},
		func(data []byte) []byte { return append(data, []byte(`{"trailing":true}`)...) },
		func(data []byte) []byte {
			return []byte(strings.Replace(string(data), `"workspace_id": "project-a"`, `"workspace_id": "project-b"`, 1))
		},
	} {
		store, _ := newTestStore(t)
		record, err := store.Create(context.Background(), preparedRecord("project-a", "thread-1", "task-1", "owner"))
		if err != nil {
			t.Fatal(err)
		}
		path := store.path(record.WorkspaceID, record.TaskID)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, mutate(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(context.Background(), "project-a", "task-1"); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("corrupt state error = %v", err)
		}
	}
}

func TestFileStore_CreateValidatesWorkspaceIsolationAndDuplicateIdentity(t *testing.T) {
	store, _ := newTestStore(t)
	record := preparedRecord("project-a", "thread-1", "task-1", "owner")
	record.Input.WorkspaceID = "project-b"
	if _, err := store.Create(context.Background(), record); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("cross-workspace input error = %v", err)
	}
	record = preparedRecord("project-a", "thread-1", "task-1", "owner")
	if _, err := store.Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), record); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate create error = %v", err)
	}
	if _, err := store.Get(context.Background(), "project-a", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing get error = %v", err)
	}
}
