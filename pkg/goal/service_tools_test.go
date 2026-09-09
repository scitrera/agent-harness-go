// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package goal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func newTestService(t *testing.T) (*Service, *FileStore) {
	t.Helper()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	next := 0
	service, err := NewService(ServiceConfig{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 8, 9, 12, 0, next, 0, time.UTC) },
		NewID: func() (string, error) {
			next++
			return fmt.Sprintf("goal-%02d", next), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, store
}

func TestGoalToolsCreateGetCompleteAndIsolate(t *testing.T) {
	service, _ := newTestService(t)
	registry := tools.NewRegistry()
	if err := RegisterTools(registry, service); err != nil {
		t.Fatal(err)
	}
	if len(registry.Descriptors()) != 3 {
		t.Fatalf("goal descriptors = %#v", registry.Descriptors())
	}
	addrA := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-1"}
	created := invokeGoalTool(t, registry, tools.Request{
		CallID: "create-1", Name: CreateToolName, Addr: addrA,
		Arguments: json.RawMessage(`{"objective":"ship the feature","token_budget":100}`),
	})
	if created.Goal == nil || created.Goal.ID != "goal-01" || created.Goal.Status != spec.SessionGoalActive {
		t.Fatalf("created = %#v", created)
	}
	if created.RemainingTokens == nil || *created.RemainingTokens != 100 {
		t.Fatalf("remaining = %#v", created.RemainingTokens)
	}

	_, err := registry.Invoke(context.Background(), tools.Request{
		CallID: "create-2", Name: CreateToolName, Addr: addrA,
		Arguments: json.RawMessage(`{"objective":"competing goal"}`),
	})
	if !errors.Is(err, ErrOpenGoalExists) {
		t.Fatalf("second create error = %v", err)
	}

	completed := invokeGoalTool(t, registry, tools.Request{
		CallID: "update-1", Name: UpdateToolName, Addr: addrA,
		Arguments: json.RawMessage(`{"status":"completed","evidence":["artifact:report"]}`),
	})
	if completed.Goal == nil || completed.Goal.Status != spec.SessionGoalCompleted || completed.Goal.CompletedAt == "" {
		t.Fatalf("completed = %#v", completed)
	}
	if len(completed.Goal.Evidence) != 1 || completed.Goal.Evidence[0] != "artifact:report" {
		t.Fatalf("evidence = %#v", completed.Goal.Evidence)
	}

	current := invokeGoalTool(t, registry, tools.Request{
		CallID: "get-1", Name: GetToolName, Addr: addrA, Arguments: json.RawMessage(`{}`),
	})
	if current.Goal == nil || current.Goal.ID != created.Goal.ID || current.Goal.Status != spec.SessionGoalCompleted {
		t.Fatalf("current = %#v", current)
	}

	addrB := protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "session-1"}
	isolated := invokeGoalTool(t, registry, tools.Request{
		CallID: "get-b", Name: GetToolName, Addr: addrB, Arguments: json.RawMessage(`{}`),
	})
	if isolated.Goal != nil {
		t.Fatalf("workspace B leaked goal = %#v", isolated.Goal)
	}
}

func TestGoalToolsUseRuntimeDefaultWorkspaceInLegacyMode(t *testing.T) {
	fx := newRuntimeFixture(t, 0, nil, nil)
	registry := tools.NewRegistry()
	if err := RegisterTools(registry, fx.service); err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{ThreadID: "session-1"}
	ctx, err := fx.runtime.BeginTurn(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	result, err := registry.Invoke(ctx, tools.Request{
		CallID: "create", Name: CreateToolName, Addr: addr,
		Arguments: json.RawMessage(`{"objective":"legacy goal"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var response toolResponse
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Goal == nil {
		t.Fatal("legacy create returned no goal")
	}
	stored, err := fx.service.OpenGoal(context.Background(), "default", "session-1")
	if err != nil || stored == nil || stored.ID != response.Goal.ID {
		t.Fatalf("default workspace goal = %#v err=%v", stored, err)
	}
}

func TestGoalServiceConcurrentCreateKeepsOneOpenGoal(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceConfig{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{
				Objective: fmt.Sprintf("goal %d", i),
			})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded := 0
	conflicted := 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrOpenGoalExists):
			conflicted++
		default:
			t.Fatalf("create error = %v", err)
		}
	}
	if succeeded != 1 || conflicted != workers-1 {
		t.Fatalf("succeeded=%d conflicted=%d", succeeded, conflicted)
	}
}

func TestGoalServiceConcurrentCASCreateKeepsOneOpenGoalAcrossReplicas(t *testing.T) {
	blobs := newMemoryCASBlobs()
	firstStore, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	secondStore, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	first, _ := NewService(ServiceConfig{Store: firstStore})
	second, _ := NewService(ServiceConfig{Store: secondStore})
	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			service := first
			if i%2 == 1 {
				service = second
			}
			_, err := service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{
				Objective: fmt.Sprintf("goal %d", i),
			})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded := 0
	for err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		if !errors.Is(err, ErrOpenGoalExists) {
			t.Fatalf("create error = %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful creates = %d, want 1", succeeded)
	}
	current, err := second.OpenGoal(context.Background(), "project-a", "session-1")
	if err != nil || current == nil {
		t.Fatalf("open goal = %#v err=%v", current, err)
	}
}

func TestGoalServiceAccountsCompletingMessageExactlyOnce(t *testing.T) {
	service, _ := newTestService(t)
	goalRecord, err := service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "finish"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpdateGoal(context.Background(), "project-a", "session-1", UpdateInput{
		ID: goalRecord.ID, Status: spec.SessionGoalCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	first, accounted, err := service.AccountUsage(context.Background(), "project-a", "session-1", goalRecord.ID, "assistant-1", 50)
	if err != nil || !accounted || first.TokenUsage != 50 || first.Status != spec.SessionGoalCompleted {
		t.Fatalf("first accounting = %#v accounted=%v err=%v", first, accounted, err)
	}
	second, accounted, err := service.AccountUsage(context.Background(), "project-a", "session-1", goalRecord.ID, "assistant-1", 50)
	if err != nil || accounted || second.TokenUsage != 50 {
		t.Fatalf("duplicate accounting = %#v accounted=%v err=%v", second, accounted, err)
	}
	encoded, err := json.Marshal(second)
	if err != nil || strings.Contains(string(encoded), "accounted_goal_messages") || strings.Contains(string(encoded), "assistant-1") {
		t.Fatalf("private accounting leaked into goal projection: %s err=%v", encoded, err)
	}
	if _, err := service.UpdateGoal(context.Background(), "project-a", "session-1", UpdateInput{
		ID: goalRecord.ID, Status: spec.SessionGoalActive,
	}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal resume error = %v", err)
	}
}

func TestGoalServiceUsageAccountingSurvivesFileStoreRestart(t *testing.T) {
	stateDir := t.TempDir()
	store, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewService(ServiceConfig{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	goalRecord, err := first.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "account durably"})
	if err != nil {
		t.Fatal(err)
	}
	if _, accounted, err := first.AccountUsage(context.Background(), "project-a", "session-1", goalRecord.ID, "assistant-1", 50); err != nil || !accounted {
		t.Fatalf("initial accounting: accounted=%v err=%v", accounted, err)
	}

	reopenedStore, err := NewFileStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewService(ServiceConfig{Store: reopenedStore})
	if err != nil {
		t.Fatal(err)
	}
	updated, accounted, err := reopened.AccountUsage(context.Background(), "project-a", "session-1", goalRecord.ID, "assistant-1", 50)
	if err != nil || accounted || updated.TokenUsage != 50 {
		t.Fatalf("replayed accounting = %#v accounted=%v err=%v", updated, accounted, err)
	}
}

func TestGoalServiceCurrentGoalUsesTimestampOrder(t *testing.T) {
	service, store := newTestService(t)
	records := []spec.SessionGoalRecord{
		{
			ID: "goal-later", Objective: "chronologically later", Status: spec.SessionGoalCompleted,
			CreatedAt: "2026-08-09T11:00:00-01:00", UpdatedAt: "2026-08-09T11:00:00-01:00",
			CompletedAt: "2026-08-09T11:00:00-01:00",
		},
		{
			ID: "goal-earlier", Objective: "lexically later", Status: spec.SessionGoalCancelled,
			CreatedAt: "2026-08-09T12:30:00+01:00", UpdatedAt: "2026-08-09T12:30:00+01:00",
			CompletedAt: "2026-08-09T12:30:00+01:00",
		},
	}
	for _, record := range records {
		if err := store.PutGoal(context.Background(), "project-a", "session-1", record); err != nil {
			t.Fatal(err)
		}
	}
	current, err := service.CurrentGoal(context.Background(), "project-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if current == nil || current.ID != "goal-later" {
		t.Fatalf("current goal = %#v", current)
	}
}

func TestGoalServiceDoesNotOverwriteOnGeneratedIDCollision(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceConfig{
		Store: store,
		NewID: func() (string, error) { return "goal-fixed", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpdateGoal(context.Background(), "project-a", "session-1", UpdateInput{
		ID: created.ID, Status: spec.SessionGoalCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "second"}); !errors.Is(err, ErrGoalExists) {
		t.Fatalf("collision error = %v", err)
	}
	stored, err := service.Goal(context.Background(), "project-a", "session-1", created.ID)
	if err != nil || stored.Objective != "first" {
		t.Fatalf("stored goal = %#v err=%v", stored, err)
	}
}

func TestGoalServiceCASUsageAccountingIsIdempotentAcrossReplicas(t *testing.T) {
	blobs := newMemoryCASBlobs()
	firstStore, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	secondStore, _ := NewCASStore(CASStoreConfig{Blobs: blobs, MaxRetries: 256})
	first, _ := NewService(ServiceConfig{Store: firstStore})
	second, _ := NewService(ServiceConfig{Store: secondStore})
	goalRecord, err := first.CreateGoal(context.Background(), "project-a", "session-1", CreateInput{Objective: "account once"})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			service := first
			if i%2 == 1 {
				service = second
			}
			_, _, err := service.AccountUsage(context.Background(), "project-a", "session-1", goalRecord.ID, "assistant-1", 50)
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	updated, err := second.Goal(context.Background(), "project-a", "session-1", goalRecord.ID)
	if err != nil || updated.TokenUsage != 50 {
		t.Fatalf("usage = %d err=%v", updated.TokenUsage, err)
	}
}

func invokeGoalTool(t *testing.T, registry *tools.Registry, request tools.Request) toolResponse {
	t.Helper()
	result, err := registry.Invoke(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var response toolResponse
	if err := json.Unmarshal(result.Payload, &response); err != nil {
		t.Fatal(err)
	}
	return response
}
