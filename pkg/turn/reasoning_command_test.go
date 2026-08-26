// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func reasoningTestRegistry() *modelpkg.Registry {
	return modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "gpt-5.6-sol", Capabilities: modelpkg.Capabilities{Tools: true}, Reasoning: modelpkg.ReasoningConfig{
			DefaultEffort: "high", AllowedEfforts: []string{"medium", "high", "xhigh", "max"},
		}},
		{Name: "fast", Capabilities: modelpkg.Capabilities{Tools: true}, Reasoning: modelpkg.ReasoningConfig{
			DefaultEffort: "low", AllowedEfforts: []string{"low", "medium"},
		}},
	}, "gpt-5.6-sol")
}

func newReasoningTestRunner(t *testing.T, processEffort string) (*Runner, *fakeProvider) {
	t.Helper()
	modelProvider := &fakeProvider{}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: modelProvider,
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
		Model:     "gpt-5.6-sol", ModelRegistry: reasoningTestRegistry(),
		ReasoningEffort: processEffort, Commands: commands.New(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner, modelProvider
}

func TestReasoning_ModelDefaultAndProcessPreferenceReachProvider(t *testing.T) {
	for _, tt := range []struct {
		name, process, want string
	}{
		{name: "model default", want: "high"},
		{name: "process preference", process: "MED", want: "medium"},
		{name: "disallowed process falls through", process: "low", want: "high"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runner, modelProvider := newReasoningTestRunner(t, tt.process)
			if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hello")); err != nil {
				t.Fatal(err)
			}
			if got := modelProvider.request.ReasoningEffort; got != tt.want {
				t.Fatalf("reasoning effort = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReasoning_CommandOverridesAndClearsPerThread(t *testing.T) {
	runner, modelProvider := newReasoningTestRunner(t, "medium")
	ctx := context.Background()
	thread1 := protocol.MessageAddress{WorkspaceID: "workspace", ThreadID: "t1"}
	thread2 := protocol.MessageAddress{WorkspaceID: "workspace", ThreadID: "t2"}

	reply, err := runner.Run(ctx, thread1, userMessage(t, "/reasoning xhigh"))
	if err != nil || !strings.Contains(assistantPlainText(reply), `set to "xhigh"`) {
		t.Fatalf("set reply = %q, err=%v", assistantPlainText(reply), err)
	}
	if _, err := runner.Run(ctx, thread1, userMessage(t, "one")); err != nil {
		t.Fatal(err)
	}
	if got := modelProvider.request.ReasoningEffort; got != "xhigh" {
		t.Fatalf("thread override = %q, want xhigh", got)
	}
	if _, err := runner.Run(ctx, thread2, userMessage(t, "two")); err != nil {
		t.Fatal(err)
	}
	if got := modelProvider.request.ReasoningEffort; got != "medium" {
		t.Fatalf("other thread effort = %q, want process medium", got)
	}
	if _, err := runner.Run(ctx, thread1, userMessage(t, "/reasoning default")); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, thread1, userMessage(t, "three")); err != nil {
		t.Fatal(err)
	}
	if got := modelProvider.request.ReasoningEffort; got != "medium" {
		t.Fatalf("cleared override = %q, want process medium", got)
	}
}

func TestReasoning_OverridesArePerModelAndValidated(t *testing.T) {
	runner, modelProvider := newReasoningTestRunner(t, "")
	ctx := context.Background()
	addr := protocol.MessageAddress{ThreadID: "t1"}
	for _, command := range []string{"/reasoning xhigh", "/model fast", "/reasoning med"} {
		if _, err := runner.Run(ctx, addr, userMessage(t, command)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runner.Run(ctx, addr, userMessage(t, "fast prompt")); err != nil {
		t.Fatal(err)
	}
	if modelProvider.request.Model != "fast" || modelProvider.request.ReasoningEffort != "medium" {
		t.Fatalf("fast request = model %q effort %q", modelProvider.request.Model, modelProvider.request.ReasoningEffort)
	}
	if _, err := runner.Run(ctx, addr, userMessage(t, "/reasoning max")); err != nil {
		t.Fatal(err)
	}
	if got := runner.ActiveReasoningEffort("t1"); got != "medium" {
		t.Fatalf("disallowed command changed effort to %q", got)
	}
	if _, err := runner.Run(ctx, addr, userMessage(t, "/model gpt-5.6-sol")); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, addr, userMessage(t, "primary prompt")); err != nil {
		t.Fatal(err)
	}
	if modelProvider.request.Model != "gpt-5.6-sol" || modelProvider.request.ReasoningEffort != "xhigh" {
		t.Fatalf("restored request = model %q effort %q", modelProvider.request.Model, modelProvider.request.ReasoningEffort)
	}
}

func TestReasoning_StatusAndInvalidRunnerConfig(t *testing.T) {
	runner, _ := newReasoningTestRunner(t, "")
	reply, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "/reasoning"))
	if err != nil {
		t.Fatal(err)
	}
	text := assistantPlainText(reply)
	if !strings.Contains(text, "Reasoning effort: high (model default)") || !strings.Contains(text, "medium, high, xhigh, max") {
		t.Fatalf("status = %q", text)
	}
	_, err = NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{}), ReasoningEffort: "turbo",
	})
	if err == nil || !strings.Contains(err.Error(), "unknown reasoning effort") {
		t.Fatalf("invalid config error = %v", err)
	}
}
