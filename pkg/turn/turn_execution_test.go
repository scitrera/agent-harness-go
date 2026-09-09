// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tasklifecycle"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

func TestJournalMessageRefSurvivesMemoryLayerProjection(t *testing.T) {
	addr := protocol.MessageAddress{
		WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "task-a",
		UserID: "alice", Ownership: "workspace",
	}
	part, err := protocol.NewTextPart("resume me")
	if err != nil {
		t.Fatal(err)
	}
	message := protocol.ChatMessage{
		SchemaVersion: "1.0", ID: "user-a", Role: protocol.RoleUser,
		Addr: addr, Content: []protocol.ContentPart{part},
		Meta: map[string]json.RawMessage{
			"scitrera": json.RawMessage(`{ "z": 2, "authority_grant_id": "grant-a", "a": 1 }`),
		},
	}
	ref, err := journalMessageRef(message, addr.WorkspaceID, addr.ThreadID)
	if err != nil {
		t.Fatal(err)
	}

	persisted, err := spec.ToMemoryLayerPayload(message)
	if err != nil {
		t.Fatal(err)
	}
	persisted["id"] = "memorylayer-native-id"
	persisted["thread_id"] = addr.ThreadID
	persisted["created_at"] = "2026-08-09T06:13:20.881945+00:00"
	reloaded, err := spec.FromMemoryLayerMessage(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.CreatedAt == "" || reloaded.Meta["app_workspace"] == nil {
		t.Fatalf("test did not reproduce MemoryLayer projection: %+v", reloaded)
	}
	if _, err := journalMessageByRef([]protocol.ChatMessage{reloaded}, ref); err != nil {
		t.Fatalf("stable MemoryLayer projection failed journal verification: %v", err)
	}

	changedPart, err := protocol.NewTextPart("changed input")
	if err != nil {
		t.Fatal(err)
	}
	reloaded.Content = []protocol.ContentPart{changedPart}
	if _, err := journalMessageByRef([]protocol.ChatMessage{reloaded}, ref); err == nil {
		t.Fatal("semantic input mutation passed journal verification")
	}
}

func TestRunnerTurnJournalConfirmsToolAndCompletes(t *testing.T) {
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"recorded":true}`))
	})); err != nil {
		t.Fatal(err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{"value":1}`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Registry: registry,
		Provider: &scriptedProvider{responses: []provider.ChatResponse{
			{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
			{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
		}},
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		TurnJournal: journal, TurnOwnerIdentity: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	userPart, err := protocol.NewTextPart("record this")
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "task-a"}
	if _, err := runner.Run(context.Background(), addr, protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}}); err != nil {
		t.Fatal(err)
	}
	record, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Phase != turnjournal.PhaseCompleted || !record.Terminal() || record.CompletedAt == nil {
		t.Fatalf("terminal record = %+v", record)
	}
	tool := singleJournalTool(record.ToolBatch)
	if tool == nil || tool.InvocationID != "call-1" || tool.Outcome != turnjournal.ToolOutcomeConfirmed || tool.Result == nil || tool.Result.MessageID != "call-1-result" {
		t.Fatalf("tool checkpoint = %+v", record.ToolBatch)
	}
	active, err := journal.ListActive(context.Background(), "agent-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("completed turn remained active: %+v", active)
	}
}

type terminalFailureTaskOps struct {
	completeErr error
	claimed     int
	completed   int
}

func (o *terminalFailureTaskOps) ClaimTask(context.Context, string) error {
	o.claimed++
	return nil
}

func (o *terminalFailureTaskOps) CompleteTask(context.Context, string) error {
	o.completed++
	return o.completeErr
}

func (*terminalFailureTaskOps) FailTask(context.Context, string, string) error { return nil }

func TestRunnerManagedTurnRetainsTerminalIntentUntilTaskAcknowledgment(t *testing.T) {
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	answer, err := protocol.NewTextPart("durable answer")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Registry: tools.NewRegistry(),
		Provider: &scriptedProvider{responses: []provider.ChatResponse{{
			Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{answer}},
		}}},
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		TurnJournal: journal, TurnOwnerIdentity: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	ops := &terminalFailureTaskOps{completeErr: errors.New("completion response lost")}
	executor := tasklifecycle.Wrap(runner, ops)
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "task-a"}
	message, runErr := executor.Run(context.Background(), addr, userMessage(t, "run once"))
	if runErr == nil || textOf(message) != "durable answer" || ops.claimed != 1 || ops.completed != 1 {
		t.Fatalf("message=%+v err=%v ops=%+v", message, runErr, ops)
	}
	pending, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Phase != turnjournal.PhaseCompleting || pending.Terminal() || !pending.PendingTerminal() {
		t.Fatalf("pending completion = %+v", pending)
	}
	active, err := runner.ActiveTurnExecutions(context.Background())
	if err != nil || len(active) != 1 || active[0].TaskID != addr.TaskID {
		t.Fatalf("active terminal intent = %+v err=%v", active, err)
	}
	if err := runner.AcknowledgeTaskTerminal(context.Background(), addr.WorkspaceID, addr.TaskID); err != nil {
		t.Fatal(err)
	}
	terminal, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Phase != turnjournal.PhaseCompleted || !terminal.Terminal() {
		t.Fatalf("acknowledged completion = %+v", terminal)
	}
}

type blockingExternalTaskBackend struct {
	awaiting chan struct{}
}

func (b *blockingExternalTaskBackend) Admit(context.Context, subagent.TaskAdmission) (string, error) {
	return "child-task", nil
}

func (b *blockingExternalTaskBackend) Start(context.Context, string) error { return nil }

func (b *blockingExternalTaskBackend) Finish(context.Context, string, subagent.TaskOutcome, string) error {
	return nil
}

func (b *blockingExternalTaskBackend) Recover(context.Context, string) (subagent.TaskRecovery, error) {
	return subagent.TaskRecoveryRunning, nil
}

func (b *blockingExternalTaskBackend) ExecutesExternally() bool { return true }

func (b *blockingExternalTaskBackend) Await(ctx context.Context, _ string, observe func(subagent.TaskRecovery)) (subagent.TaskRecovery, error) {
	if observe != nil {
		observe(subagent.TaskRecoveryRunning)
	}
	close(b.awaiting)
	<-ctx.Done()
	return "", ctx.Err()
}

func TestRunnerTurnJournalBindsExternalDescriptorBeforeAwait(t *testing.T) {
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	ref := &subagent.Ref{}
	if err := tools.RegisterSubagentWithConfig(registry, tools.SubagentConfig{Runner: ref, MaxDepth: 2}); err != nil {
		t.Fatal(err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "spawn-1", Name: tools.SubagentToolName,
		Args: protocol.RawToArgs(json.RawMessage(`{"task":"wait for cancellation"}`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := &blockingExternalTaskBackend{awaiting: make(chan struct{})}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Registry: registry,
		Provider: &scriptedProvider{responses: []provider.ChatResponse{{
			Message: protocol.ChatMessage{ID: "assistant-spawn", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}},
		}}},
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		SubagentTasks: backend, TurnJournal: journal, TurnOwnerIdentity: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	ref.Set(runner)
	userPart, err := protocol.NewTextPart("delegate")
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "parent-task"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, addr, protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
		done <- runErr
	}()
	<-backend.awaiting
	record, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tool := singleJournalTool(record.ToolBatch)
	if record.Phase != turnjournal.PhaseWaitingExternalChild || tool == nil || tool.Outcome != turnjournal.ToolOutcomeAdmitted || tool.External == nil {
		t.Fatalf("admitted checkpoint = %+v", record)
	}
	if tool.External.TaskID != "child-task" || tool.External.ExecutionID == "" || tool.External.ChildSessionID == "" {
		t.Fatalf("external identity = %+v", tool.External)
	}
	envelope, err := subagent.ParseExecutionEnvelope(tool.External.Descriptor)
	if err != nil {
		t.Fatalf("parse persisted descriptor: %v", err)
	}
	if envelope.ExecutionID != tool.External.ExecutionID || envelope.InvocationID != "spawn-1" || envelope.ParentTaskID != addr.TaskID {
		t.Fatalf("descriptor identity = %+v", envelope)
	}
	cancel()
	if runErr := <-done; !errors.Is(runErr, turncancel.ErrTurnCancelled) {
		t.Fatalf("run error = %v", runErr)
	}
	record, err = journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tool = singleJournalTool(record.ToolBatch)
	if record.Phase != turnjournal.PhaseInterrupted || tool == nil || tool.Outcome != turnjournal.ToolOutcomeAdmitted || tool.External == nil || tool.External.TaskID != "child-task" {
		t.Fatalf("interrupted admitted checkpoint = %+v", record)
	}
}

func TestRunnerTurnJournalRequiresStableOwner(t *testing.T) {
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Registry: tools.NewRegistry(),
		Provider: &scriptedProvider{}, Publisher: &fakePublisher{},
		Assembler:   contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		TurnJournal: journal,
	})
	if err == nil {
		t.Fatal("expected missing owner identity error")
	}
}

type failWaitingJournal struct{ turnjournal.Store }

func (s failWaitingJournal) Update(ctx context.Context, record turnjournal.Record, expected uint64) (turnjournal.Record, error) {
	if record.Phase == turnjournal.PhaseWaitingExternalChild {
		return turnjournal.Record{}, errors.New("checkpoint response lost")
	}
	return s.Store.Update(ctx, record, expected)
}

type admittedOnlyExternalBackend struct {
	awaited bool
}

type completedExternalBackend struct {
	awaits int
}

func (*completedExternalBackend) Admit(context.Context, subagent.TaskAdmission) (string, error) {
	return "child-task", nil
}

func (*completedExternalBackend) Start(context.Context, string) error { return nil }

func (*completedExternalBackend) Finish(context.Context, string, subagent.TaskOutcome, string) error {
	return nil
}

func (*completedExternalBackend) Recover(context.Context, string) (subagent.TaskRecovery, error) {
	return subagent.TaskRecoveryCompleted, nil
}

func (*completedExternalBackend) ExecutesExternally() bool { return true }

func (b *completedExternalBackend) Await(context.Context, string, func(subagent.TaskRecovery)) (subagent.TaskRecovery, error) {
	b.awaits++
	return subagent.TaskRecoveryCompleted, nil
}

func (*admittedOnlyExternalBackend) Admit(context.Context, subagent.TaskAdmission) (string, error) {
	return "possibly-admitted-child", nil
}

func (*admittedOnlyExternalBackend) Start(context.Context, string) error { return nil }

func (*admittedOnlyExternalBackend) Finish(context.Context, string, subagent.TaskOutcome, string) error {
	return nil
}

func (*admittedOnlyExternalBackend) Recover(context.Context, string) (subagent.TaskRecovery, error) {
	return subagent.TaskRecoveryAdmitted, nil
}

func (*admittedOnlyExternalBackend) ExecutesExternally() bool { return true }

func (b *admittedOnlyExternalBackend) Await(context.Context, string, func(subagent.TaskRecovery)) (subagent.TaskRecovery, error) {
	b.awaited = true
	return subagent.TaskRecoveryCompleted, nil
}

func TestRunnerTurnJournalStopsAfterAmbiguousAdmissionCheckpoint(t *testing.T) {
	base, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal := failWaitingJournal{Store: base}
	registry := tools.NewRegistry()
	ref := &subagent.Ref{}
	if err := tools.RegisterSubagentWithConfig(registry, tools.SubagentConfig{Runner: ref, MaxDepth: 2}); err != nil {
		t.Fatal(err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: "spawn-uncertain", Name: tools.SubagentToolName,
		Args: protocol.RawToArgs(json.RawMessage(`{"task":"must not be admitted twice"}`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := &admittedOnlyExternalBackend{}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Registry: registry,
		Provider: &scriptedProvider{responses: []provider.ChatResponse{{
			Message: protocol.ChatMessage{ID: "assistant-spawn", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}},
		}}},
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		SubagentTasks: backend, TurnJournal: journal, TurnOwnerIdentity: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	ref.Set(runner)
	userPart, err := protocol.NewTextPart("delegate once")
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "parent-uncertain"}
	_, runErr := runner.Run(context.Background(), addr, protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser, Content: []protocol.ContentPart{userPart}})
	if !errors.Is(runErr, subagent.ErrParentCheckpointUncertain) {
		t.Fatalf("run error = %v", runErr)
	}
	if backend.awaited {
		t.Fatal("parent awaited after its admission checkpoint became uncertain")
	}
	record, err := base.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tool := singleJournalTool(record.ToolBatch)
	if record.Phase != turnjournal.PhaseInterrupted || tool == nil || tool.Outcome != turnjournal.ToolOutcomeUncertain || tool.External != nil {
		t.Fatalf("uncertain admission checkpoint = %+v", record)
	}
}

func TestRunnerResumeTurnUsesAdmittedChildAndAppendsResultExactlyOnce(t *testing.T) {
	for _, preexistingResult := range []bool{false, true} {
		t.Run(map[bool]string{false: "append-result", true: "reuse-existing-result"}[preexistingResult], func(t *testing.T) {
			journal, err := turnjournal.NewFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			history := &recordingStore{saved: map[string][]protocol.ChatMessage{}}
			addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-thread", TaskID: "parent-task", RequestID: "window-a"}
			userPart, err := protocol.NewTextPart("delegate")
			if err != nil {
				t.Fatal(err)
			}
			user := protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{userPart}}
			args := protocol.RawToArgs(json.RawMessage(`{"task":"return CHILD_OK"}`))
			callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "spawn-1", Name: tools.SubagentToolName, Args: args})
			if err != nil {
				t.Fatal(err)
			}
			assistant := protocol.ChatMessage{ID: "assistant-spawn", Role: protocol.RoleAssistant, Addr: addr, Content: []protocol.ContentPart{callPart}}
			history.saved[addr.ThreadID] = []protocol.ChatMessage{user, assistant}

			req := subagent.Request{
				Task: "return CHILD_OK", Depth: 1, Parent: addr,
				InvocationID: "spawn-1", ParentMessageID: assistant.ID,
			}
			envelope, err := subagent.NewExecutionEnvelope(req, addr.WorkspaceID, "child-thread", false)
			if err != nil {
				t.Fatal(err)
			}
			childInputPart, err := protocol.NewTextPart(req.Task)
			if err != nil {
				t.Fatal(err)
			}
			childAddr := protocol.MessageAddress{WorkspaceID: addr.WorkspaceID, ThreadID: envelope.ChildSessionID, TaskID: "child-task"}
			childInput := protocol.ChatMessage{
				ID: envelope.Input.RecordID, Role: protocol.RoleUser,
				Addr:    protocol.MessageAddress{WorkspaceID: addr.WorkspaceID, ThreadID: envelope.ChildSessionID},
				Content: []protocol.ContentPart{childInputPart},
			}
			childAnswerPart, err := protocol.NewTextPart("CHILD_OK")
			if err != nil {
				t.Fatal(err)
			}
			childAnswer := protocol.ChatMessage{ID: "child-final", Role: protocol.RoleAssistant, Addr: childAddr, Content: []protocol.ContentPart{childAnswerPart}}
			history.saved[envelope.ChildSessionID] = []protocol.ChatMessage{childInput, childAnswer}

			inputRef, err := journalMessageRef(user, addr.WorkspaceID, addr.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			assistantRef, err := journalMessageRef(assistant, addr.WorkspaceID, addr.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			descriptor, err := subagent.MarshalExecutionEnvelope(envelope)
			if err != nil {
				t.Fatal(err)
			}
			external, err := turnjournal.NewExternalChildRef("child-task", envelope.ExecutionID, envelope.ChildSessionID, descriptor)
			if err != nil {
				t.Fatal(err)
			}
			record, err := journal.Create(context.Background(), turnjournal.Record{
				WorkspaceID: addr.WorkspaceID, SessionID: addr.ThreadID, TaskID: addr.TaskID,
				OwnerIdentity: "agent-a", Phase: turnjournal.PhasePrepared, Input: inputRef, LastMessageID: user.ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			record.Phase = turnjournal.PhaseProviderPending
			record, err = journal.Update(context.Background(), record, record.Revision)
			if err != nil {
				t.Fatal(err)
			}
			record.Phase = turnjournal.PhaseToolPending
			record.Iteration = 1
			record.LastMessageID = assistant.ID
			record.ToolBatch = &turnjournal.ToolBatchCheckpoint{
				Assistant: assistantRef,
				Calls: []turnjournal.ToolCheckpoint{{
					InvocationID: "spawn-1", Name: tools.SubagentToolName,
					ArgsDigest: digestJournalBytes(protocol.ArgsToRaw(args)), Outcome: turnjournal.ToolOutcomeRequested,
				}},
			}
			record, err = journal.Update(context.Background(), record, record.Revision)
			if err != nil {
				t.Fatal(err)
			}
			record.Phase = turnjournal.PhaseWaitingExternalChild
			record.ToolBatch.Calls[0].Outcome = turnjournal.ToolOutcomeAdmitted
			record.ToolBatch.Calls[0].External = &external
			if _, err = journal.Update(context.Background(), record, record.Revision); err != nil {
				t.Fatal(err)
			}

			finalPart, err := protocol.NewTextPart("PARENT_SAW_CHILD_OK")
			if err != nil {
				t.Fatal(err)
			}
			provider := &scriptedProvider{responses: []provider.ChatResponse{{
				Message: protocol.ChatMessage{ID: "parent-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}},
			}}}
			backend := &completedExternalBackend{}
			runner, err := NewRunner(Config{
				Store: history, Loader: fakeLoader{}, Registry: tools.NewRegistry(), Provider: provider,
				Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 20}),
				SubagentTasks: backend, TurnJournal: journal, TurnOwnerIdentity: "agent-a",
			})
			if err != nil {
				t.Fatal(err)
			}
			if preexistingResult {
				result, err := runner.recoveredSubagentToolResult(context.Background(), addr, envelope, subagent.TaskRecoveryCompleted, "child-task", "spawn-1")
				if err != nil {
					t.Fatal(err)
				}
				part, err := result.ContentPart()
				if err != nil {
					t.Fatal(err)
				}
				history.saved[addr.ThreadID] = append(history.saved[addr.ThreadID], protocol.ChatMessage{
					ID: "spawn-1-result", Role: protocol.RoleToolResult, Addr: addr,
					Content: append([]protocol.ContentPart{part}, result.Parts...),
				})
			}

			active, err := runner.ActiveTurnExecutions(context.Background())
			if err != nil || len(active) != 1 || active[0].TaskID != addr.TaskID {
				t.Fatalf("active executions = %+v, err=%v", active, err)
			}
			answer, err := runner.ResumeTurn(context.Background(), addr.WorkspaceID, addr.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if textOf(answer) != "PARENT_SAW_CHILD_OK" || backend.awaits != 1 || len(provider.requests) != 1 {
				t.Fatalf("resume result=%q awaits=%d provider_calls=%d", textOf(answer), backend.awaits, len(provider.requests))
			}
			messages, err := history.LoadHistory(context.Background(), addr.ThreadID)
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, message := range messages {
				counts[message.ID]++
			}
			if counts[user.ID] != 1 || counts["spawn-1-result"] != 1 || counts["parent-final"] != 1 {
				t.Fatalf("message counts = %+v", counts)
			}
			terminal, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			tool := singleJournalTool(terminal.ToolBatch)
			if terminal.Phase != turnjournal.PhaseCompleted || tool == nil || tool.Outcome != turnjournal.ToolOutcomeConfirmed || tool.Result == nil || tool.Result.MessageID != "spawn-1-result" {
				t.Fatalf("terminal recovery record = %+v", terminal)
			}
			if _, err := runner.ResumeTurn(context.Background(), addr.WorkspaceID, addr.TaskID); !errors.Is(err, ErrRecoveryUnsafe) {
				t.Fatalf("second resume error = %v", err)
			}
			if backend.awaits != 1 || len(provider.requests) != 1 {
				t.Fatal("terminal execution was replayed")
			}
		})
	}
}

func TestRunnerResumeTurnInterruptsUnsafeProviderBoundary(t *testing.T) {
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "task-a"}
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatal(err)
	}
	user := protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{part}}
	input, err := journalMessageRef(user, addr.WorkspaceID, addr.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	record, err := journal.Create(context.Background(), turnjournal.Record{
		WorkspaceID: addr.WorkspaceID, SessionID: addr.ThreadID, TaskID: addr.TaskID,
		OwnerIdentity: "agent-a", Phase: turnjournal.PhasePrepared, Input: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = turnjournal.PhaseProviderPending
	if _, err := journal.Update(context.Background(), record, record.Revision); err != nil {
		t.Fatal(err)
	}
	history := &recordingStore{saved: map[string][]protocol.ChatMessage{addr.ThreadID: {user}}}
	provider := &scriptedProvider{}
	runner, err := NewRunner(Config{
		Store: history, Loader: fakeLoader{}, Registry: tools.NewRegistry(), Provider: provider,
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		TurnJournal: journal, TurnOwnerIdentity: "agent-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.ResumeTurn(context.Background(), addr.WorkspaceID, addr.TaskID); !errors.Is(err, ErrRecoveryUnsafe) {
		t.Fatalf("resume error = %v", err)
	}
	terminal, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Phase != turnjournal.PhaseInterrupted || terminal.FailureReason == "" || len(provider.requests) != 0 {
		t.Fatalf("unsafe recovery terminal=%+v provider_calls=%d", terminal, len(provider.requests))
	}
}
