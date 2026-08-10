package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
)

func TestPrepareScheduledTurnRegistrationsDefaultsAndPinsWorkerView(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "schedules.yaml")
	if err := os.WriteFile(configPath, []byte(`version: 1
schedules:
  - id: workspace-review
    schedule:
      type: interval
      expression: 1h
    prompt: Review the current workspace.
    relative_directory: ""
    allow_mutable_view: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	host, err := aetherchan.NewWorkerToolHost(context.Background(), aetherchan.WorkerToolHostConfig{
		WorkspaceID: "project-a", WorkspaceRoot: root, StateDir: t.TempDir(), ToolHostID: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	registrations, err := prepareScheduledTurnRegistrations(context.Background(), configPath, root, host)
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 1 {
		t.Fatalf("registrations = %d", len(registrations))
	}
	registration := registrations[0]
	if !registration.Enabled || registration.Name != "workspace-review" ||
		registration.ThreadID != "scheduled-workspace-review" || registration.MissPolicy != "fire_once" ||
		registration.TargetOfflinePolicy != "queue" ||
		registration.Binding.ToolHostID != "worker-a" || registration.Binding.WorkspaceID != "project-a" {
		t.Fatalf("registration = %+v", registration)
	}
}

func TestPrepareScheduledTurnRegistrationsAcceptsEmptyDesiredState(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "schedules.yaml")
	if err := os.WriteFile(configPath, []byte("version: 1\nschedules: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	host, err := aetherchan.NewWorkerToolHost(context.Background(), aetherchan.WorkerToolHostConfig{
		WorkspaceID: "project-a", WorkspaceRoot: root, StateDir: t.TempDir(), ToolHostID: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	registrations, err := prepareScheduledTurnRegistrations(context.Background(), configPath, root, host)
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 0 {
		t.Fatalf("registrations = %d, want empty desired state", len(registrations))
	}
}

func TestPrepareScheduledTurnRegistrationsRejectsUnknownFieldsAndImplicitMutableView(t *testing.T) {
	root := t.TempDir()
	host, err := aetherchan.NewWorkerToolHost(context.Background(), aetherchan.WorkerToolHostConfig{
		WorkspaceID: "project-a", WorkspaceRoot: root, StateDir: t.TempDir(), ToolHostID: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"unknown": `version: 1
schedules:
  - id: review
    schedule: {type: interval, expression: 1h}
    prompt: Review.
    typo_field: true
`,
		"mutable": `version: 1
schedules:
  - id: review
    schedule: {type: interval, expression: 1h}
    prompt: Review.
`,
		"offline policy": `version: 1
schedules:
  - id: review
    schedule: {type: interval, expression: 1h}
    prompt: Review.
    allow_mutable_view: true
    offline_policy: eventually
`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedules.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := prepareScheduledTurnRegistrations(context.Background(), path, root, host)
			if err == nil {
				t.Fatal("invalid schedule config was accepted")
			}
			if name == "mutable" && !strings.Contains(err.Error(), "allow_mutable_view") {
				t.Fatalf("mutable view error = %v", err)
			}
		})
	}
}

type recordingScheduledTurnUpdater struct {
	updates chan []aetherchan.ScheduledTurnRegistration
}

func (u *recordingScheduledTurnUpdater) UpdateScheduledTurns(_ context.Context, registrations []aetherchan.ScheduledTurnRegistration) error {
	u.updates <- registrations
	return nil
}

func TestWatchScheduledTurnDeclarationsKeepsLastGoodUntilValidEdit(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "schedules.yaml")
	writeConfig := func(prompt string) {
		t.Helper()
		body := "version: 1\nschedules:\n  - id: review\n    schedule: {type: interval, expression: 1h}\n    prompt: " + prompt + "\n    allow_mutable_view: true\n"
		if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig("Initial")
	host, err := aetherchan.NewWorkerToolHost(context.Background(), aetherchan.WorkerToolHostConfig{
		WorkspaceID: "project-a", WorkspaceRoot: root, StateDir: t.TempDir(), ToolHostID: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, digest, err := prepareScheduledTurnRegistrationsSnapshot(context.Background(), configPath, root, host)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updater := &recordingScheduledTurnUpdater{updates: make(chan []aetherchan.ScheduledTurnRegistration, 1)}
	go watchScheduledTurnDeclarations(ctx, updater, host, appConfig{
		scheduleConfig: configPath, workspaceRoot: root, scheduleReloadInterval: 10 * time.Millisecond,
	}, digest)
	if err := os.WriteFile(configPath, []byte("version: 1\nschedules:\n  - typo: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-updater.updates:
		t.Fatalf("invalid edit was applied: %+v", update)
	case <-time.After(50 * time.Millisecond):
	}
	writeConfig("Updated")
	select {
	case update := <-updater.updates:
		if len(update) != 1 || update[0].Prompt != "Updated" {
			t.Fatalf("reloaded registrations = %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("valid schedule edit was not hot-reloaded")
	}
}
