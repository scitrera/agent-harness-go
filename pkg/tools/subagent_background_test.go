// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// spyBackgroundSubagent is a spySubagent that also implements BackgroundRunner.
// bgErr lets a test force a StartBackground failure (including
// ErrBackgroundUnsupported to exercise the synchronous fallback).
type spyBackgroundSubagent struct {
	spySubagent
	bgCalled   bool
	bgThreadID string
	bgErr      error
}

func (s *spyBackgroundSubagent) StartBackground(_ context.Context, req subagent.Request) (string, error) {
	s.bgCalled = true
	s.task = req.Task
	s.depth = req.Depth
	s.executionScope = req.ExecutionScope
	if s.bgErr != nil {
		return "", s.bgErr
	}
	return s.bgThreadID, nil
}

func Test_spawn_subagent_background_returns_running_handle(t *testing.T) {
	reg := NewRegistry()
	spy := &spyBackgroundSubagent{bgThreadID: "parent::sub::5"}
	if err := RegisterSubagentWithConfig(reg, SubagentConfig{Runner: spy, MaxDepth: 2, AllowBackground: true}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// The schema advertises the background arg when enabled.
	for _, d := range reg.Descriptors() {
		if d.Name == "spawn_subagent" && !bytes.Contains(d.Parameters, []byte(`"background"`)) {
			t.Fatalf("background arg missing from schema: %s", d.Parameters)
		}
	}

	binding := spec.NewExecutionBinding("project-a", "view-a", "window-a", spec.ExecutionSiteClient)
	scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := workspacepkg.WithExecutionScope(context.Background(), scope)
	res, err := reg.Invoke(ctx, Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"task":"research X","background":true}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !spy.bgCalled {
		t.Fatal("StartBackground was not called for a background spawn")
	}
	if spy.called {
		t.Fatal("synchronous RunSubagent must not run for a background spawn")
	}
	if !workspacepkg.ExecutionScopesEqual(&scope, spy.executionScope) {
		t.Fatalf("background execution scope = %+v", spy.executionScope)
	}
	if !bytes.Contains(res.Payload, []byte(`"status":"running"`)) {
		t.Fatalf("payload missing running status: %s", res.Payload)
	}
	if !bytes.Contains(res.Payload, []byte(`"thread_id":"parent::sub::5"`)) {
		t.Fatalf("payload missing thread handle: %s", res.Payload)
	}
	if len(res.Parts) != 1 {
		t.Fatalf("expected 1 subagent part, got %d", len(res.Parts))
	}
	sp, ok := res.Parts[0].AsSubagent()
	if !ok || sp.Status != protocol.SubagentRunning {
		t.Fatalf("expected running SubagentPart, got %+v (ok=%v)", sp, ok)
	}
	if sp.ThreadID != "parent::sub::5" {
		t.Fatalf("subagent part thread = %q, want parent::sub::5", sp.ThreadID)
	}
}

func Test_spawn_subagent_background_disabled_runs_synchronously(t *testing.T) {
	reg := NewRegistry()
	spy := &spyBackgroundSubagent{bgThreadID: "parent::sub::5"}
	// AllowBackground defaults false: the background arg is ignored and the schema omits it.
	if err := RegisterSubagentWithConfig(reg, SubagentConfig{Runner: spy, MaxDepth: 2}); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, d := range reg.Descriptors() {
		if d.Name == "spawn_subagent" && bytes.Contains(d.Parameters, []byte(`"background"`)) {
			t.Fatalf("background arg should be absent when disabled: %s", d.Parameters)
		}
	}
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"task":"research X","background":true}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if spy.bgCalled {
		t.Fatal("StartBackground must not run when AllowBackground is false")
	}
	if !spy.called {
		t.Fatal("expected a synchronous RunSubagent")
	}
	if !bytes.Contains(res.Payload, []byte("sub-agent answer")) {
		t.Fatalf("payload missing synchronous result: %s", res.Payload)
	}
}

func Test_spawn_subagent_background_unsupported_falls_back_to_sync(t *testing.T) {
	reg := NewRegistry()
	// A plain spySubagent is NOT a BackgroundRunner → background falls back to sync.
	spy := &spySubagent{}
	if err := RegisterSubagentWithConfig(reg, SubagentConfig{Runner: spy, MaxDepth: 2, AllowBackground: true}); err != nil {
		t.Fatalf("register: %v", err)
	}
	res, err := reg.Invoke(context.Background(), Request{
		CallID:    "c1",
		Name:      "spawn_subagent",
		Arguments: json.RawMessage(`{"task":"research X","background":true}`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !spy.called {
		t.Fatal("expected synchronous fallback when runner is not a BackgroundRunner")
	}
	if !bytes.Contains(res.Payload, []byte("sub-agent answer")) {
		t.Fatalf("payload missing synchronous result: %s", res.Payload)
	}
}
