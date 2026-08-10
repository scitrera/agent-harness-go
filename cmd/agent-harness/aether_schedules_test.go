package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
		registration.Binding.ToolHostID != "worker-a" || registration.Binding.WorkspaceID != "project-a" {
		t.Fatalf("registration = %+v", registration)
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
