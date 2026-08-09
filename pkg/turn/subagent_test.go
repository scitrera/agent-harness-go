package turn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type captureSubagentLifecycle struct {
	mu     sync.Mutex
	events []subagent.LifecycleEvent
	err    error
}

type captureSubagentTasks struct {
	mu         sync.Mutex
	calls      []string
	admission  subagent.TaskAdmission
	startErr   error
	finishErr  error
	finishRuns int
	onAdmit    func(subagent.TaskAdmission) error
}

type inlineExternalSubagentTasks struct {
	runner    *Runner
	admission subagent.TaskAdmission
	starts    int
	finishes  int
}

func (t *inlineExternalSubagentTasks) Admit(_ context.Context, admission subagent.TaskAdmission) (string, error) {
	t.admission = admission
	return "external-child-task", nil
}

func (t *inlineExternalSubagentTasks) Start(context.Context, string) error {
	t.starts++
	return nil
}

func (t *inlineExternalSubagentTasks) Finish(context.Context, string, subagent.TaskOutcome, string) error {
	t.finishes++
	return nil
}

func (*inlineExternalSubagentTasks) Recover(context.Context, string) (subagent.TaskRecovery, error) {
	return subagent.TaskRecoveryRunning, nil
}

func (*inlineExternalSubagentTasks) ExecutesExternally() bool { return true }

func (t *inlineExternalSubagentTasks) Await(ctx context.Context, taskID string, observe func(subagent.TaskRecovery)) (subagent.TaskRecovery, error) {
	observe(subagent.TaskRecoveryRunning)
	req, err := subagent.ReconstructExecutionRequest(
		ctx, t.admission.Execution, nil,
		t.admission.GrantID, t.admission.SubjectType, t.admission.SubjectID,
	)
	if err != nil {
		return "", err
	}
	if _, err := t.runner.ExecuteAssignedSubagent(ctx, taskID, t.admission.Execution, req); err != nil {
		return subagent.TaskRecoveryFailed, err
	}
	observe(subagent.TaskRecoveryCompleted)
	return subagent.TaskRecoveryCompleted, nil
}

func (t *captureSubagentTasks) Admit(_ context.Context, admission subagent.TaskAdmission) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, "admit")
	t.admission = admission
	if t.onAdmit != nil {
		if err := t.onAdmit(admission); err != nil {
			return "", err
		}
	}
	return "aether-child-task", nil
}

func (t *captureSubagentTasks) Start(context.Context, string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, "start")
	return t.startErr
}

func (t *captureSubagentTasks) Finish(_ context.Context, _ string, outcome subagent.TaskOutcome, _ string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, "finish:"+string(outcome))
	t.finishRuns++
	return t.finishErr
}

func (t *captureSubagentTasks) Recover(context.Context, string) (subagent.TaskRecovery, error) {
	return subagent.TaskRecoveryInterrupted, nil
}

func (t *captureSubagentTasks) snapshot() ([]string, subagent.TaskAdmission, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.calls...), t.admission, t.finishRuns
}

func (o *captureSubagentLifecycle) ObserveSubagent(_ context.Context, event subagent.LifecycleEvent) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
	return o.err
}

func (o *captureSubagentLifecycle) snapshot() []subagent.LifecycleEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]subagent.LifecycleEvent(nil), o.events...)
}

func Test_Runner_RunSubagent_returns_final_text(t *testing.T) {
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	res, err := r.RunSubagent(context.Background(), subagent.Request{
		Task:   "summarize the design",
		Depth:  1,
		Parent: protocol.MessageAddress{ThreadID: "t1"},
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if res.Text != "assistant response" {
		t.Fatalf("expected sub-agent final text, got %q", res.Text)
	}
}

func Test_Runner_RunSubagent_externalParentOnlyAwaitsSharedHistory(t *testing.T) {
	store := &recordingStore{}
	tasks := &inlineExternalSubagentTasks{}
	observer := &captureSubagentLifecycle{}
	runner, err := NewRunner(Config{
		Store: store, Loader: fakeLoader{}, Provider: &fakeProvider{},
		Assembler:     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		SubagentTasks: tasks, SubagentObserver: observer, SubagentDefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	tasks.runner = runner
	result, err := runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review externally", Depth: 2, InvocationID: "call-external", ParentMessageID: "parent-message",
		Parent: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent", TaskID: "parent-task"},
		Model:  "model-a", GrantID: "task-grant", SubjectType: "user", SubjectID: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "assistant response" || result.ThreadID != tasks.admission.ChildSessionID {
		t.Fatalf("result=%+v admission=%+v", result, tasks.admission)
	}
	if tasks.starts != 0 || tasks.finishes != 0 {
		t.Fatalf("parent mutated externally owned task: starts=%d finishes=%d", tasks.starts, tasks.finishes)
	}
	events := observer.snapshot()
	if len(events) != 3 || events[0].Record.Status != spec.SessionSubagentAdmitted || events[1].Record.Status != spec.SessionSubagentRunning || events[2].Record.Status != spec.SessionSubagentCompleted {
		t.Fatalf("external lifecycle events = %#v", events)
	}
	childHistory := store.saved[result.ThreadID]
	if len(childHistory) != 2 || childHistory[0].ID != tasks.admission.Execution.Input.RecordID || childHistory[1].Role != protocol.RoleAssistant {
		t.Fatalf("shared child history = %#v", childHistory)
	}
}

func Test_Runner_RunSubagent_observesDurableLifecycle(t *testing.T) {
	observer := &captureSubagentLifecycle{}
	now := time.Date(2026, 8, 8, 20, 0, 0, 0, time.UTC)
	runner, err := NewRunner(Config{
		Store:                    &fakeStore{},
		Loader:                   fakeLoader{},
		Provider:                 &fakeProvider{},
		Assembler:                contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Now:                      func() time.Time { return now },
		SubagentObserver:         observer,
		SubagentDefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review", Depth: 1, Parent: protocol.MessageAddress{ThreadID: "parent-1"},
		AgentName: "reviewer", AgentType: "review", Model: "local-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := observer.snapshot()
	if len(events) != 3 {
		t.Fatalf("lifecycle events = %#v", events)
	}
	wantStatuses := []spec.SessionSubagentStatus{
		spec.SessionSubagentAdmitted,
		spec.SessionSubagentRunning,
		spec.SessionSubagentCompleted,
	}
	for i, event := range events {
		if event.WorkspaceID != "project-a" || event.Record.ID != result.ThreadID || event.Record.ParentSessionID != "parent-1" || event.Record.Status != wantStatuses[i] {
			t.Fatalf("lifecycle event %d = %#v", i, event)
		}
	}
	if events[2].Record.CompletedAt == "" || events[2].Record.Name != "reviewer" || events[2].Record.Kind != "review" || events[2].Record.Model != "local-model" {
		t.Fatalf("terminal lifecycle record = %#v", events[2].Record)
	}
}

func Test_Runner_RunSubagent_usesTaskAuthorityBeforeLifecycleProjection(t *testing.T) {
	observer := &captureSubagentLifecycle{}
	store := &fakeStore{}
	tasks := &captureSubagentTasks{onAdmit: func(admission subagent.TaskAdmission) error {
		if len(store.messages) != 1 {
			return fmt.Errorf("durable input messages before admission = %d", len(store.messages))
		}
		_, task, err := admission.Execution.ResolveInput(store.messages)
		if err != nil {
			return err
		}
		if task != "review" {
			return fmt.Errorf("prepared task = %q", task)
		}
		return nil
	}}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store: store, Loader: fakeLoader{}, Provider: &fakeProvider{}, Publisher: publisher,
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		SubagentObserver: observer, SubagentDefaultWorkspace: "project-a", SubagentTasks: tasks,
		StreamSubagents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review", Depth: 2, InvocationID: "tool-call-7", ParentMessageID: "message-3",
		Parent:    protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-1", TaskID: "parent-task"},
		AgentName: "reviewer", AgentType: "review", GrantID: "grant-1", SubjectType: "user", SubjectID: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	calls, admission, finishRuns := tasks.snapshot()
	if strings.Join(calls, ",") != "admit,start,finish:completed" || finishRuns != 1 {
		t.Fatalf("task calls = %#v", calls)
	}
	if admission.ChildSessionID != result.ThreadID || admission.ParentTaskID != "parent-task" || admission.InvocationID != "tool-call-7" || admission.ParentMessageID != "message-3" {
		t.Fatalf("task admission = %+v", admission)
	}
	if err := admission.Execution.Validate(); err != nil || admission.Execution.Input.RecordID == "" {
		t.Fatalf("execution envelope = %+v err=%v", admission.Execution, err)
	}
	resolvedResult, err := admission.Execution.ResolveResult(store.messages)
	if err != nil || resolvedResult.ID != "assistant-1" {
		t.Fatalf("execution result = %+v err=%v", resolvedResult, err)
	}
	events := observer.snapshot()
	if len(events) != 3 {
		t.Fatalf("lifecycle events = %#v", events)
	}
	for i, status := range []spec.SessionSubagentStatus{spec.SessionSubagentAdmitted, spec.SessionSubagentRunning, spec.SessionSubagentCompleted} {
		if events[i].Record.TaskID != "aether-child-task" || events[i].Record.Status != status {
			t.Fatalf("event %d = %#v", i, events[i])
		}
	}
	for _, event := range publisher.events {
		if event.Addr.TaskID != "aether-child-task" || event.Addr.ThreadID != result.ThreadID {
			t.Fatalf("child stream address = %+v", event.Addr)
		}
	}
}

func Test_Runner_RunSubagent_projectsInterruptedWhenTaskFinishIsUncertain(t *testing.T) {
	observer := &captureSubagentLifecycle{}
	tasks := &captureSubagentTasks{finishErr: errors.New("task state unavailable")}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		SubagentObserver: observer, SubagentDefaultWorkspace: "project-a", SubagentTasks: tasks,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review", Parent: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-1"},
	})
	if !errors.Is(err, subagent.ErrTaskOutcomeUncertain) {
		t.Fatalf("error = %v, want ErrTaskOutcomeUncertain", err)
	}
	_, _, finishRuns := tasks.snapshot()
	if finishRuns != 1 {
		t.Fatalf("terminal mutation calls = %d, want 1", finishRuns)
	}
	events := observer.snapshot()
	terminal := events[len(events)-1].Record
	if terminal.Status != spec.SessionSubagentInterrupted || terminal.CompletedAt == "" || terminal.TaskID != "aether-child-task" {
		t.Fatalf("terminal lifecycle = %#v", terminal)
	}
}

func Test_Runner_RunSubagent_neverExecutesAfterTaskStartFailure(t *testing.T) {
	observer := &captureSubagentLifecycle{}
	tasks := &captureSubagentTasks{startErr: errors.New("claim rejected")}
	provider := &scriptedProvider{}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: provider,
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		SubagentObserver: observer, SubagentDefaultWorkspace: "project-a", SubagentTasks: tasks,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review", Parent: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-1"},
	})
	if err == nil || !strings.Contains(err.Error(), "task start") {
		t.Fatalf("error = %v", err)
	}
	if len(provider.requests) != 0 {
		t.Fatalf("provider ran after failed claim: %d requests", len(provider.requests))
	}
	calls, _, _ := tasks.snapshot()
	if strings.Join(calls, ",") != "admit,start,finish:cancelled" {
		t.Fatalf("task calls = %#v", calls)
	}
	events := observer.snapshot()
	if len(events) != 2 || events[0].Record.Status != spec.SessionSubagentAdmitted || events[1].Record.Status != spec.SessionSubagentCancelled {
		t.Fatalf("lifecycle events = %#v", events)
	}
}

func Test_Runner_RunSubagent_persistsReferencedInputBeforeTaskAdmission(t *testing.T) {
	store := &fakeStore{}
	provider := &scriptedProvider{}
	tasks := &captureSubagentTasks{onAdmit: func(admission subagent.TaskAdmission) error {
		if len(store.messages) != 1 {
			return fmt.Errorf("prepared messages = %d", len(store.messages))
		}
		message, task, err := admission.Execution.ResolveInput(store.messages)
		if err != nil {
			return err
		}
		if message.ID != admission.Execution.Input.RecordID || task != "review before claim" {
			return fmt.Errorf("prepared message=%+v task=%q", message, task)
		}
		return errors.New("admission rejected")
	}}
	observer := &captureSubagentLifecycle{}
	runner, err := NewRunner(Config{
		Store: store, Loader: fakeLoader{}, Provider: provider,
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		SubagentObserver: observer, SubagentDefaultWorkspace: "project-a", SubagentTasks: tasks,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review before claim", InvocationID: "tool-call-prepare",
		Parent: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-1", TaskID: "parent-task"},
	})
	if err == nil || !strings.Contains(err.Error(), "admission rejected") {
		t.Fatalf("error = %v", err)
	}
	if len(provider.requests) != 0 {
		t.Fatalf("provider ran after failed admission: %d requests", len(provider.requests))
	}
	if events := observer.snapshot(); len(events) != 0 {
		t.Fatalf("lifecycle projected before admission: %#v", events)
	}
}

func Test_Runner_RunSubagent_observerFailureDoesNotFailTurn(t *testing.T) {
	observer := &captureSubagentLifecycle{err: errors.New("registry unavailable")}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		SubagentObserver: observer, SubagentDefaultWorkspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunSubagent(context.Background(), subagent.Request{
		Task: "review", Depth: 1, Parent: protocol.MessageAddress{ThreadID: "parent-1"},
	}); err != nil {
		t.Fatalf("observer failure broke subagent turn: %v", err)
	}
}

func Test_Runner_RunSubagent_streamsChildActivityWhenEnabled(t *testing.T) {
	part, err := protocol.NewTextPart("live child update")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	childProvider := &streamingScriptedProvider{
		scriptedProvider: scriptedProvider{responses: []provider.ChatResponse{{
			Message: protocol.ChatMessage{ID: "child-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}},
		}}},
		streamText: []string{"live child update"},
	}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:           &fakeStore{},
		Loader:          fakeLoader{},
		Provider:        childProvider,
		Publisher:       publisher,
		Assembler:       contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Streaming:       true,
		StreamSubagents: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	wantCWD := "/selected/project"
	ctx := tools.WithWorkingDirectory(context.Background(), wantCWD)
	res, err := runner.RunSubagent(ctx, subagent.Request{
		Task:   "review it",
		Depth:  1,
		Parent: protocol.MessageAddress{ThreadID: "parent", TaskID: "task-parent"},
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if res.Text != "live child update" {
		t.Fatalf("result = %q", res.Text)
	}
	if len(childProvider.requests) == 0 || !requestTextContains(childProvider.requests[0], "Active working directory: \""+wantCWD+"\"") {
		t.Fatalf("inherited cwd missing from child request: %#v", childProvider.requests)
	}
	var started, delta, finished bool
	for _, event := range publisher.events {
		if !strings.HasPrefix(event.Addr.ThreadID, "parent::sub::") || event.Addr.TaskID != "task-parent" {
			t.Fatalf("child event address = %+v", event.Addr)
		}
		switch event.Type {
		case channel.EventMessageStarted:
			started = true
		case channel.EventTokenDelta:
			delta = strings.Contains(event.Delta, "live child update")
		case channel.EventMessageFinal:
			finished = true
		}
	}
	if !started || !delta || !finished {
		t.Fatalf("child stream lifecycle started=%v delta=%v finished=%v events=%+v", started, delta, finished, publisher.events)
	}
}

func Test_Runner_RunSubagent_applies_catalog_prompt_model_and_tool_surface(t *testing.T) {
	// Given: a production runner with two registered tools and a catalog-selected
	// subagent request that only allows read_file.
	registry := tools.NewRegistry()
	if err := registry.Register("read_file", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register read_file: %v", err)
	}
	if err := registry.Register("shell", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register shell: %v", err)
	}
	provider := &fakeProvider{}
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{files: []bootstrap.File{{Name: "AGENTS.md", Content: "base instructions"}}},
		Registry:  registry,
		Provider:  provider,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:     "default-model",
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: the in-process subagent runner executes the request.
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task:         "inspect the patch",
		Parent:       protocol.MessageAddress{ThreadID: "thread-1"},
		AgentType:    subagent.AgentType("reviewer"),
		Instructions: "reviewer-only instructions",
		AllowedTools: []string{"read_file"},
		DeniedTools:  []string{"shell"},
		Model:        "agent-model",
	})

	// Then: the provider sees the selected model, prompt instructions, and only
	// the allowed tool in the real chat request.
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if provider.request.Model != "agent-model" {
		t.Fatalf("model = %q, want agent-model", provider.request.Model)
	}
	if got := toolNames(provider.request.Tools); len(got) != 1 || got[0] != "read_file" {
		t.Fatalf("tools = %#v, want [read_file]", got)
	}
	if !requestTextContains(provider.request, "reviewer-only instructions") {
		t.Fatalf("subagent instructions missing from provider request: %#v", provider.request.Messages)
	}
}

func Test_Runner_RunSubagent_denies_hidden_or_unlisted_tool_calls(t *testing.T) {
	// Given: shell is registered, but the selected subagent definition denies it.
	calledShell := false
	registry := tools.NewRegistry()
	if err := registry.Register("shell", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		calledShell = true
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ran":true}`))
	})); err != nil {
		t.Fatalf("register shell: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "shell"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("blocked shell")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: the model calls shell anyway, even though it was hidden from Tools.
	res, err := runner.RunSubagent(context.Background(), subagent.Request{
		Task:        "try a shell command",
		Parent:      protocol.MessageAddress{ThreadID: "thread-1"},
		DeniedTools: []string{"shell"},
	})

	// Then: the real tool handler is not invoked; the denial is returned as a
	// tool_result and the provider is reprompted to produce the final answer.
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}
	if calledShell {
		t.Fatal("shell handler should not run for denied subagent tool")
	}
	if res.Text != "blocked shell" {
		t.Fatalf("result = %q, want blocked shell", res.Text)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(provider.requests))
	}
	if !requestTextContains(provider.requests[1], "tool denied") {
		t.Fatalf("denied tool_result missing from reprompt: %#v", provider.requests[1].Messages)
	}
}

func Test_Runner_RunSubagent_applies_max_turns_to_tool_loop(t *testing.T) {
	// Given: the runner normally allows several tool iterations, but the selected
	// subagent caps this run to one tool iteration.
	registry := tools.NewRegistry()
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register record: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool-1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "assistant-tool-2", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
	}}
	runner, err := NewRunner(Config{
		Store:             &fakeStore{},
		Loader:            fakeLoader{},
		Registry:          registry,
		Provider:          provider,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		MaxToolIterations: 4,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: the subagent spends its single allowed tool iteration and asks for
	// another tool call.
	_, err = runner.RunSubagent(context.Background(), subagent.Request{
		Task:     "loop tools",
		Parent:   protocol.MessageAddress{ThreadID: "thread-1"},
		MaxTurns: 1,
	})

	// Then: the production loop stops at the catalog limit, not the runner default.
	if !errors.Is(err, ErrToolLoopLimit) {
		t.Fatalf("expected ErrToolLoopLimit, got %v", err)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(provider.requests))
	}
}

func toolNames(specs []provider.ToolSpec) []string {
	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		out = append(out, spec.Name)
	}
	return out
}

func requestTextContains(req provider.ChatRequest, want string) bool {
	needle := []byte(want)
	for _, msg := range req.Messages {
		for _, part := range msg.Content {
			if text, ok := part.AsText(); ok && bytes.Contains([]byte(text.Text), needle) {
				return true
			}
			if result, ok := part.AsToolResult(); ok && bytes.Contains(result.Output, needle) {
				return true
			}
		}
	}
	return false
}
