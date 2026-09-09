// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

type orderedBatchApprover struct {
	approved atomic.Int32
}

func (a *orderedBatchApprover) ApproveTool(_ context.Context, _ hooks.ToolCall) hooks.Decision {
	a.approved.Add(1)
	return hooks.Allow()
}

func parallelToolPart(t *testing.T, callID, name string) protocol.ContentPart {
	t.Helper()
	part, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{
		CallID: callID, Name: name, Args: protocol.RawToArgs(json.RawMessage(`{}`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return part
}

func parallelRunner(t *testing.T, registry *tools.Registry, calls []protocol.ContentPart, approvers []hooks.ToolApprover, journal turnjournal.Store) (*Runner, *fakeStore) {
	t.Helper()
	final, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	runner, err := NewRunner(Config{
		Store: store, Loader: fakeLoader{}, Registry: registry,
		Provider: &scriptedProvider{responses: []provider.ChatResponse{
			{Message: protocol.ChatMessage{ID: "assistant-tools", Role: protocol.RoleAssistant, Content: calls}},
			{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{final}}},
		}},
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 20}),
		MaxToolIterations: 2, Approvers: approvers, TurnJournal: journal, TurnOwnerIdentity: "parallel-test-owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner, store
}

func TestRunnerParallelTools_PreflightsAllCallsThenOverlapsAndCommitsInSourceOrder(t *testing.T) {
	registry := tools.NewRegistry()
	approver := &orderedBatchApprover{}
	started := make(chan string, 2)
	release := make(chan struct{})
	var startedBeforePreflight atomic.Bool
	for _, name := range []string{"safe_a", "safe_b"} {
		name := name
		if err := registry.Register(name, tools.HandlerFunc(func(ctx context.Context, req tools.Request) (tools.Result, error) {
			if approver.approved.Load() != 2 {
				startedBeforePreflight.Store(true)
			}
			started <- name
			select {
			case <-release:
			case <-ctx.Done():
				return tools.Result{}, ctx.Err()
			}
			return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"tool":"`+name+`"}`))
		})); err != nil {
			t.Fatal(err)
		}
		registry.Describe(tools.Descriptor{Name: name, Concurrency: tools.ConcurrencyParallelSafe})
	}
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner, store := parallelRunner(t, registry, []protocol.ContentPart{
		parallelToolPart(t, "call-a", "safe_a"),
		parallelToolPart(t, "call-b", "safe_b"),
	}, []hooks.ToolApprover{approver}, journal)

	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "task-a"}
	done := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(context.Background(), addr, protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser})
		done <- runErr
	}()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case name := <-started:
			seen[name] = true
		case <-time.After(2 * time.Second):
			t.Fatal("both parallel-safe tool bodies did not overlap")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if startedBeforePreflight.Load() {
		t.Fatal("a tool body started before every approval preflight completed")
	}

	var resultIDs []string
	for _, message := range store.HistoryForTest() {
		if message.Role == protocol.RoleToolResult {
			resultIDs = append(resultIDs, message.ID)
		}
	}
	if len(resultIDs) != 2 || resultIDs[0] != "call-a-result" || resultIDs[1] != "call-b-result" {
		t.Fatalf("persisted result order = %v", resultIDs)
	}
	record, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Phase != turnjournal.PhaseCompleted || record.ToolBatch == nil || len(record.ToolBatch.Calls) != 2 {
		t.Fatalf("terminal batch checkpoint = %+v", record)
	}
	for i := range record.ToolBatch.Calls {
		if record.ToolBatch.Calls[i].Outcome != turnjournal.ToolOutcomeConfirmed || record.ToolBatch.Calls[i].Result == nil {
			t.Fatalf("unconfirmed batch call = %+v", record.ToolBatch.Calls[i])
		}
	}
}

func TestRunnerParallelTools_UnclassifiedSiblingKeepsCompleteBatchSequential(t *testing.T) {
	registry := tools.NewRegistry()
	var active atomic.Int32
	var maximum atomic.Int32
	var mu sync.Mutex
	var order []string
	for _, descriptor := range []tools.Descriptor{
		{Name: "safe", Concurrency: tools.ConcurrencyParallelSafe},
		{Name: "unclassified"},
	} {
		descriptor := descriptor
		if err := registry.Register(descriptor.Name, tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
			current := active.Add(1)
			for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
			}
			mu.Lock()
			order = append(order, req.Name)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{}`))
		})); err != nil {
			t.Fatal(err)
		}
		registry.Describe(descriptor)
	}
	runner, _ := parallelRunner(t, registry, []protocol.ContentPart{
		parallelToolPart(t, "call-safe", "safe"),
		parallelToolPart(t, "call-serial", "unclassified"),
	}, nil, nil)
	if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "thread-a"}, protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser}); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 1 {
		t.Fatalf("mixed batch reached concurrency %d", maximum.Load())
	}
	if len(order) != 2 || order[0] != "safe" || order[1] != "unclassified" {
		t.Fatalf("sequential invocation order = %v", order)
	}
}

func TestRunnerParallelTools_CancellationInterruptsWholeBatchAsUncertain(t *testing.T) {
	registry := tools.NewRegistry()
	started := make(chan struct{}, 2)
	for _, name := range []string{"wait_a", "wait_b"} {
		name := name
		if err := registry.Register(name, tools.HandlerFunc(func(ctx context.Context, _ tools.Request) (tools.Result, error) {
			started <- struct{}{}
			<-ctx.Done()
			return tools.Result{}, ctx.Err()
		})); err != nil {
			t.Fatal(err)
		}
		registry.Describe(tools.Descriptor{Name: name, Concurrency: tools.ConcurrencyParallelSafe})
	}
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner, _ := parallelRunner(t, registry, []protocol.ContentPart{
		parallelToolPart(t, "call-a", "wait_a"),
		parallelToolPart(t, "call-b", "wait_b"),
	}, nil, journal)
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-a", TaskID: "task-cancel"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, addr, protocol.ChatMessage{ID: "user-a", Role: protocol.RoleUser})
		done <- runErr
	}()
	<-started
	<-started
	cancel()
	if err := <-done; err == nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, turncancel.ErrTurnCancelled)) {
		t.Fatalf("cancelled turn error = %v", err)
	}
	record, err := journal.Get(context.Background(), addr.WorkspaceID, addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Phase != turnjournal.PhaseInterrupted || record.ToolBatch == nil || len(record.ToolBatch.Calls) != 2 {
		t.Fatalf("interrupted batch checkpoint = %+v", record)
	}
	for i := range record.ToolBatch.Calls {
		if record.ToolBatch.Calls[i].Outcome != turnjournal.ToolOutcomeUncertain {
			t.Fatalf("cancelled call was not uncertain: %+v", record.ToolBatch.Calls[i])
		}
	}
}
