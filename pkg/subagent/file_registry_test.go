// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	timeCreated = "2026-08-08T20:00:00Z"
	timeRunning = "2026-08-08T20:01:00Z"
	timeDone    = "2026-08-08T20:02:00Z"
)

func TestFileRegistryPersistsLifecycleAcrossRestartAndWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	registry, err := NewFileRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	base := spec.SessionSubagentRecord{
		ID:              "child-1",
		ParentSessionID: "parent-1",
		ChildSessionID:  "parent-1::sub::1",
		Name:            "reviewer",
		Depth:           1,
		Status:          spec.SessionSubagentAdmitted,
		CreatedAt:       timeCreated,
		UpdatedAt:       timeCreated,
	}
	if err := registry.ObserveSubagent(ctx, LifecycleEvent{WorkspaceID: "project-a", Record: base}); err != nil {
		t.Fatal(err)
	}
	running := base
	running.Status = spec.SessionSubagentRunning
	running.UpdatedAt = timeRunning
	if err := registry.ObserveSubagent(ctx, LifecycleEvent{WorkspaceID: "project-a", Record: running}); err != nil {
		t.Fatal(err)
	}
	completed := running
	completed.Status = spec.SessionSubagentCompleted
	completed.UpdatedAt = timeDone
	completed.CompletedAt = timeDone
	completed.ChildUsage = map[string]json.RawMessage{"total_tokens": json.RawMessage(`125`)}
	if err := registry.ObserveSubagent(ctx, LifecycleEvent{WorkspaceID: "project-a", Record: completed}); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewFileRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := restarted.ListSubagents(ctx, "project-a", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Status != spec.SessionSubagentCompleted || records[0].CreatedAt != timeCreated || string(records[0].ChildUsage["total_tokens"]) != "125" {
		t.Fatalf("restart records = %#v", records)
	}
	records[0].ChildUsage["total_tokens"][0] = '9'
	again, err := restarted.ListSubagents(ctx, "project-a", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(again[0].ChildUsage["total_tokens"]) != "125" {
		t.Fatalf("caller mutated registry: %#v", again[0].ChildUsage)
	}
	isolated, err := restarted.ListSubagents(ctx, "project-b", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(isolated) != 0 {
		t.Fatalf("workspace B leaked records: %#v", isolated)
	}
}

func TestFileRegistryConcurrentTransitionsRemainSorted(t *testing.T) {
	const count = 32
	registry, err := NewFileRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	errors := make(chan error, count)
	var wait sync.WaitGroup
	for i := count - 1; i >= 0; i-- {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			id := fmt.Sprintf("child-%02d", i)
			errors <- registry.ObserveSubagent(context.Background(), LifecycleEvent{
				WorkspaceID: "project-a",
				Record: spec.SessionSubagentRecord{
					ID: id, ParentSessionID: "parent-1", ChildSessionID: id,
					Status: spec.SessionSubagentRunning, CreatedAt: timeCreated, UpdatedAt: timeRunning,
				},
			})
		}(i)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	records, err := registry.ListSubagents(context.Background(), "project-a", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count {
		t.Fatalf("records = %d, want %d", len(records), count)
	}
	for i, record := range records {
		if record.ID != fmt.Sprintf("child-%02d", i) {
			t.Fatalf("record %d = %q", i, record.ID)
		}
	}
}

func TestFileRegistryRecoversRunningChildrenAsInterrupted(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	registry, err := NewFileRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []spec.SessionSubagentStatus{spec.SessionSubagentAdmitted, spec.SessionSubagentRunning} {
		id := string(status)
		if err := registry.ObserveSubagent(ctx, LifecycleEvent{
			WorkspaceID: "project-a",
			Record: spec.SessionSubagentRecord{
				ID: id, ParentSessionID: "parent-1", ChildSessionID: id,
				Status: status, CreatedAt: timeCreated, UpdatedAt: timeRunning,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	recoveryTime := time.Date(2026, 8, 8, 20, 3, 0, 0, time.UTC)
	restarted, err := NewFileRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverInterrupted(ctx, recoveryTime); err != nil {
		t.Fatal(err)
	}
	records, err := restarted.ListSubagents(ctx, "project-a", "parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("recovered records = %#v", records)
	}
	for _, record := range records {
		if record.Status != spec.SessionSubagentInterrupted || record.CompletedAt != recoveryTime.Format(time.RFC3339Nano) {
			t.Fatalf("recovered record = %#v", record)
		}
	}
}

func TestFileRegistryPreservesDeletionTombstone(t *testing.T) {
	registry, err := NewFileRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record := spec.SessionSubagentRecord{
		ID: "child-1", ParentSessionID: "parent-1", ChildSessionID: "child-1",
		Status: spec.SessionSubagentDeleted, CreatedAt: timeCreated, UpdatedAt: timeDone, DeletedAt: timeDone,
	}
	if err := registry.ObserveSubagent(context.Background(), LifecycleEvent{WorkspaceID: "project-a", Record: record}); err != nil {
		t.Fatal(err)
	}
	record.Status = spec.SessionSubagentRunning
	record.UpdatedAt = timeRunning
	record.DeletedAt = ""
	if err := registry.ObserveSubagent(context.Background(), LifecycleEvent{WorkspaceID: "project-a", Record: record}); !errors.Is(err, ErrDeletedLifecycle) {
		t.Fatalf("resume deleted lifecycle error = %v", err)
	}
}

func TestFileRegistryRejectsCorruptState(t *testing.T) {
	registry, err := NewFileRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := registry.path("project-a", "parent-1")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"workspace_id":"project-b"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ListSubagents(context.Background(), "project-a", "parent-1"); !errors.Is(err, ErrCorruptRegistry) {
		t.Fatalf("corrupt registry error = %v", err)
	}
}
