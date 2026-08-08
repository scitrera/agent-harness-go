package turn

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

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
