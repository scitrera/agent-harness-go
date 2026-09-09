// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type fakeScheduledOperationsCommands struct {
	addr  protocol.MessageAddress
	user  protocol.ChatMessage
	name  string
	args  string
	text  string
	err   error
	calls int
}

type fakeRefinementAuditCommands struct {
	auth  tools.MemoryAuthority
	addr  protocol.MessageAddress
	user  protocol.ChatMessage
	args  string
	text  string
	err   error
	calls int
}

func (f *fakeRefinementAuditCommands) RunRefinementAuditCommand(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, args string) (string, error) {
	f.calls++
	f.auth, _ = tools.MemoryAuthorityFrom(ctx)
	f.addr = addr
	f.user = user
	f.args = args
	return f.text, f.err
}

func (f *fakeScheduledOperationsCommands) RunScheduledOperationsCommand(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, name, args string) (string, error) {
	f.calls++
	f.addr = addr
	f.user = user
	f.name = name
	f.args = args
	return f.text, f.err
}

func newCommandRunner(t *testing.T, provider *fakeProvider, store *fakeStore, reg *commands.Registry) *Runner {
	t.Helper()
	r, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{},
		Provider:  provider,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:     "base-model",
		Commands:  reg,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	return r
}

func Test_Runner_Run_builtin_help_bypasses_model(t *testing.T) {
	provider := &fakeProvider{}
	reg := commands.New([]commands.Command{{Name: "commit", Description: "Make a commit", ArgumentHint: "<msg>"}})
	r := newCommandRunner(t, provider, &fakeStore{}, reg)

	reply, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/help"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if provider.request.Model != "" {
		t.Fatalf("provider should not have been called for /help, got model %q", provider.request.Model)
	}
	text := assistantPlainText(reply)
	if !strings.Contains(text, "/commit") || !strings.Contains(text, "/clear") {
		t.Fatalf("help text missing entries: %q", text)
	}
}

func Test_Runner_Run_builtin_models_alias_bypasses_model(t *testing.T) {
	provider := &fakeProvider{}
	r := newCommandRunner(t, provider, &fakeStore{}, commands.New(nil))

	reply, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/models"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if provider.request.Model != "" {
		t.Fatalf("provider should not have been called for /models, got model %q", provider.request.Model)
	}
	if text := assistantPlainText(reply); !strings.Contains(text, "Active model: base-model") {
		t.Fatalf("models alias reply = %q", text)
	}
}

func Test_Runner_Run_builtin_scheduledOperationsRemainWorkerAuthoritative(t *testing.T) {
	provider := &fakeProvider{}
	operations := &fakeScheduledOperationsCommands{text: "authoritative schedule state"}
	r := newCommandRunner(t, provider, &fakeStore{}, commands.New(nil))
	r.scheduledOperations = operations
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "t1", TaskID: "task-1"}
	user := userMessage(t, "/runs --status failed --limit 10")
	user.ID = "operations-user"

	reply, err := r.Run(context.Background(), addr, user)
	if err != nil {
		t.Fatal(err)
	}
	if provider.request.Model != "" || operations.calls != 1 || operations.name != "runs" ||
		operations.args != "--status failed --limit 10" || operations.addr.WorkspaceID != addr.WorkspaceID ||
		operations.addr.ThreadID != addr.ThreadID || operations.addr.TaskID != addr.TaskID || operations.user.ID != user.ID {
		t.Fatalf("provider=%q operations=%+v", provider.request.Model, operations)
	}
	if got := assistantPlainText(reply); got != operations.text {
		t.Fatalf("operations reply = %q", got)
	}
	if !strings.Contains(r.helpText(), "/schedules") || !strings.Contains(r.helpText(), "/runs") {
		t.Fatalf("configured operations missing from help: %q", r.helpText())
	}
}

func Test_Runner_Run_builtin_scheduledOperationsUnavailableOrFailed(t *testing.T) {
	r := newCommandRunner(t, &fakeProvider{}, &fakeStore{}, commands.New(nil))
	reply, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/schedules"))
	if err != nil || !strings.Contains(assistantPlainText(reply), "not connected to Aether") {
		t.Fatalf("standalone reply=%q err=%v", assistantPlainText(reply), err)
	}
	if strings.Contains(r.helpText(), "/schedules") {
		t.Fatalf("standalone help advertised unavailable operations: %q", r.helpText())
	}
	r.scheduledOperations = &fakeScheduledOperationsCommands{err: errors.New("authority denied")}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/runs")); err == nil ||
		!strings.Contains(err.Error(), "authority denied") {
		t.Fatalf("operations failure = %v", err)
	}
}

func Test_Runner_Run_builtin_refinementAuditUsesDerivedAuthorityBeforeCommandResolution(t *testing.T) {
	provider := &fakeProvider{}
	audit := &fakeRefinementAuditCommands{text: "bounded audit page"}
	r := newCommandRunner(t, provider, &fakeStore{}, commands.New(nil))
	r.refinementAudit = audit
	r.authorityFn = func(protocol.MessageAddress, protocol.ChatMessage) tools.MemoryAuthority {
		return tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"}
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "ops", TaskID: "task-1"}
	user := userMessage(t, "/refinements --attention --limit 7")
	user.ID = "audit-user"
	reply, err := r.Run(context.Background(), addr, user)
	if err != nil {
		t.Fatal(err)
	}
	if provider.request.Model != "" || audit.calls != 1 || audit.args != "--attention --limit 7" ||
		audit.addr.WorkspaceID != "project-a" || audit.user.ID != user.ID || audit.auth.GrantID != "grant-1" || audit.auth.SubjectID != "alice" {
		t.Fatalf("provider=%q audit=%+v", provider.request.Model, audit)
	}
	if got := assistantPlainText(reply); got != audit.text {
		t.Fatalf("audit reply = %q", got)
	}
	if !strings.Contains(r.helpText(), "/refinements") {
		t.Fatalf("configured audit missing from help: %q", r.helpText())
	}
}

func Test_Runner_Run_builtin_refinementAuditUnavailableOrFailed(t *testing.T) {
	r := newCommandRunner(t, &fakeProvider{}, &fakeStore{}, commands.New(nil))
	reply, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/refinements"))
	if err != nil || !strings.Contains(assistantPlainText(reply), "no refinement authority") {
		t.Fatalf("unavailable reply=%q err=%v", assistantPlainText(reply), err)
	}
	if strings.Contains(r.helpText(), "/refinements") {
		t.Fatalf("unconfigured audit advertised in help: %q", r.helpText())
	}
	r.refinementAudit = &fakeRefinementAuditCommands{err: errors.New("denied")}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/refinements")); err == nil ||
		!strings.Contains(err.Error(), "denied") {
		t.Fatalf("audit failure = %v", err)
	}
}

func Test_Runner_Run_builtin_clear_wipes_history(t *testing.T) {
	store := &fakeStore{messages: []protocol.ChatMessage{{ID: "old", Role: protocol.RoleUser}}}
	provider := &fakeProvider{}
	r := newCommandRunner(t, provider, store, commands.New(nil))

	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/clear")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(store.messages) != 0 {
		t.Fatalf("expected history cleared, got %#v", store.messages)
	}
	if provider.request.Model != "" {
		t.Fatal("provider should not be called for /clear")
	}
}

func Test_Runner_Run_builtin_clearOnlyWipesAddressedWorkspace(t *testing.T) {
	ctx := context.Background()
	store := harness.NewMemoryStore()
	seed := []protocol.ChatMessage{{ID: "old", Role: protocol.RoleUser}}
	if err := store.SaveWorkspaceHistory(ctx, "project-a", "shared", seed); err != nil {
		t.Fatalf("seed project-a: %v", err)
	}
	if err := store.SaveWorkspaceHistory(ctx, "project-b", "shared", seed); err != nil {
		t.Fatalf("seed project-b: %v", err)
	}
	runner, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
		Model:     "base-model",
		Commands:  commands.New(nil),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	if _, err := runner.Run(ctx, protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "shared"}, userMessage(t, "/clear")); err != nil {
		t.Fatalf("clear: %v", err)
	}
	projectA, err := store.LoadWorkspaceHistory(ctx, "project-a", "shared")
	if err != nil {
		t.Fatalf("load project-a: %v", err)
	}
	projectB, err := store.LoadWorkspaceHistory(ctx, "project-b", "shared")
	if err != nil {
		t.Fatalf("load project-b: %v", err)
	}
	if len(projectA) != 0 || len(projectB) != 1 {
		t.Fatalf("histories after clear: project-a=%+v project-b=%+v", projectA, projectB)
	}
}

func Test_Runner_Run_unknown_command_bypasses_model(t *testing.T) {
	provider := &fakeProvider{}
	r := newCommandRunner(t, provider, &fakeStore{}, commands.New(nil))

	reply, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/nope do stuff"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if provider.request.Model != "" {
		t.Fatal("provider should not be called for unknown command")
	}
	if !strings.Contains(assistantPlainText(reply), "Unknown command: /nope") {
		t.Fatalf("expected unknown-command reply, got %q", assistantPlainText(reply))
	}
}

func Test_Runner_Run_workspace_command_expands_into_prompt(t *testing.T) {
	provider := &fakeProvider{}
	reg := commands.New([]commands.Command{{
		Name: "commit",
		Body: "Commit staged changes with message: $ARGUMENTS",
	}})
	r := newCommandRunner(t, provider, &fakeStore{}, reg)

	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/commit fix the parser")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if provider.request.Model != "base-model" {
		t.Fatalf("provider should run with base model, got %q", provider.request.Model)
	}
	found := false
	for _, m := range provider.request.Messages {
		if m.Role != protocol.RoleUser {
			continue
		}
		if strings.Contains(messageText(m), "Commit staged changes with message: fix the parser") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expanded prompt not delivered to provider: %#v", provider.request.Messages)
	}
}

func Test_Runner_Run_workspace_command_model_override(t *testing.T) {
	provider := &fakeProvider{}
	reg := commands.New([]commands.Command{{Name: "deep", Body: "Think hard about $ARGUMENTS", Model: "scitrera-deep"}})
	r := newCommandRunner(t, provider, &fakeStore{}, reg)

	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/deep the bug")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if provider.request.Model != "scitrera-deep" {
		t.Fatalf("expected model override scitrera-deep, got %q", provider.request.Model)
	}
}

func assistantPlainText(msg protocol.ChatMessage) string {
	return messageText(msg)
}

func messageText(msg protocol.ChatMessage) string {
	var b strings.Builder
	for _, p := range msg.Content {
		if tp, ok := p.AsText(); ok {
			b.WriteString(tp.Text)
		}
	}
	return b.String()
}
