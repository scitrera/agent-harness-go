// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

// decodeShellPayload unwraps the model-visible tool payload — the only surface
// that matters here, since Result.Metadata never reaches the model.
func decodeShellPayload(t *testing.T, result Result) struct {
	ExitCode      int    `json:"exit_code"`
	Output        string `json:"output"`
	Truncated     bool   `json:"output_truncated"`
	OutputBytes   int    `json:"output_bytes"`
	OutputArchive string `json:"output_archive"`
} {
	t.Helper()
	var payload struct {
		ExitCode      int    `json:"exit_code"`
		Output        string `json:"output"`
		Truncated     bool   `json:"output_truncated"`
		OutputBytes   int    `json:"output_bytes"`
		OutputArchive string `json:"output_archive"`
	}
	if err := json.Unmarshal(result.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return payload
}

func newShellRegistry(t *testing.T, cfg LocalConfig) (*Registry, *localtools.Workspace) {
	t.Helper()
	workspace, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	cfg.Workspace = workspace
	registry := NewRegistry()
	if err := RegisterLocal(registry, cfg); err != nil {
		t.Fatalf("register tools: %v", err)
	}
	return registry, workspace
}

func TestShellTruncationIsVisibleToTheModel(t *testing.T) {
	// Given: a command whose output blows the visible cap.
	registry, _ := newShellRegistry(t, LocalConfig{MaxOutput: 32})

	// When
	result, err := registry.Invoke(context.Background(), Request{
		CallID: "call-1", Name: "shell",
		Arguments: json.RawMessage(`{"command":"printf 'A%.0s' $(seq 1 500)"}`),
	})

	// Then: the cut is stated in the output itself. Before this, the model read a
	// silently-severed log as if it were complete.
	if err != nil {
		t.Fatalf("invoke shell: %v", err)
	}
	payload := decodeShellPayload(t, result)
	if !payload.Truncated {
		t.Fatalf("expected output_truncated, got %#v", payload)
	}
	if !strings.Contains(payload.Output, "output truncated") {
		t.Fatalf("no truncation marker in model-visible output: %q", payload.Output)
	}
	if payload.OutputBytes != 500 {
		t.Fatalf("output_bytes = %d, want 500", payload.OutputBytes)
	}
}

func TestShellTruncationArchivesRecoverableOutput(t *testing.T) {
	// Given
	registry, workspace := newShellRegistry(t, LocalConfig{MaxOutput: 32})

	// When
	result, err := registry.Invoke(context.Background(), Request{
		CallID: "call-archive", Name: "shell",
		Arguments: json.RawMessage(`{"command":"printf 'HEAD'; printf 'A%.0s' $(seq 1 500); printf 'TAIL'"}`),
	})

	// Then: the marker names a ref, and that ref is a real, readable file.
	if err != nil {
		t.Fatalf("invoke shell: %v", err)
	}
	payload := decodeShellPayload(t, result)
	if payload.OutputArchive == "" {
		t.Fatalf("expected an archive ref, got %#v", payload)
	}
	if !strings.Contains(payload.Output, payload.OutputArchive) {
		t.Fatalf("truncation marker does not name the archive: %q", payload.Output)
	}
	if !strings.Contains(payload.Output, "read_file") {
		t.Fatalf("truncation marker does not say how to read the archive: %q", payload.Output)
	}

	archived, err := os.ReadFile(filepath.Join(workspace.Root(), payload.OutputArchive))
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if !strings.HasPrefix(string(archived), "HEAD") || !strings.HasSuffix(string(archived), "TAIL") {
		t.Fatalf("archive lost an end window: %d bytes", len(archived))
	}
	if !strings.HasPrefix(payload.OutputArchive, DefaultOutputArchiveDir) {
		t.Fatalf("archive ref = %q, want it under %q", payload.OutputArchive, DefaultOutputArchiveDir)
	}
}

func TestShellArchiveRefIsReadableWithReadFile(t *testing.T) {
	// Given: the marker tells the model to use read_file, so read_file must
	// actually resolve the ref it was handed.
	registry, _ := newShellRegistry(t, LocalConfig{MaxOutput: 32})
	shellResult, err := registry.Invoke(context.Background(), Request{
		CallID: "call-roundtrip", Name: "shell",
		Arguments: json.RawMessage(`{"command":"printf 'MARKER'; printf 'A%.0s' $(seq 1 500)"}`),
	})
	if err != nil {
		t.Fatalf("invoke shell: %v", err)
	}
	ref := decodeShellPayload(t, shellResult).OutputArchive
	if ref == "" {
		t.Fatal("expected an archive ref")
	}

	// When
	args, err := json.Marshal(map[string]string{"path": ref})
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	readResult, err := registry.Invoke(context.Background(), Request{
		CallID: "call-read", Name: "read_file", Arguments: args,
	})

	// Then
	if err != nil {
		t.Fatalf("read_file on the archive ref: %v", err)
	}
	if !strings.Contains(string(readResult.Payload), "MARKER") {
		t.Fatalf("read_file did not return the archived output: %s", readResult.Payload)
	}
}

func TestShellArchiveCanBeDisabledWithoutHidingTruncation(t *testing.T) {
	// Given: an operator who does not want tool output written into the workspace.
	registry, _ := newShellRegistry(t, LocalConfig{MaxOutput: 32, OutputArchiveLimit: -1})

	// When
	result, err := registry.Invoke(context.Background(), Request{
		CallID: "call-noarchive", Name: "shell",
		Arguments: json.RawMessage(`{"command":"printf 'A%.0s' $(seq 1 500)"}`),
	})

	// Then: no file is written, but the model is still told the output was cut.
	if err != nil {
		t.Fatalf("invoke shell: %v", err)
	}
	payload := decodeShellPayload(t, result)
	if payload.OutputArchive != "" {
		t.Fatalf("archive ref = %q, want none when archiving is disabled", payload.OutputArchive)
	}
	if !strings.Contains(payload.Output, "output truncated") {
		t.Fatalf("truncation must stay visible with archiving off: %q", payload.Output)
	}
}

func TestShellUntruncatedOutputCarriesNoMarker(t *testing.T) {
	// Given
	registry, _ := newShellRegistry(t, LocalConfig{MaxOutput: 4096})

	// When
	result, err := registry.Invoke(context.Background(), Request{
		CallID: "call-small", Name: "shell",
		Arguments: json.RawMessage(`{"command":"printf hello"}`),
	})

	// Then
	if err != nil {
		t.Fatalf("invoke shell: %v", err)
	}
	payload := decodeShellPayload(t, result)
	if payload.Output != "hello" {
		t.Fatalf("output = %q, want hello with no marker appended", payload.Output)
	}
	if payload.Truncated || payload.OutputArchive != "" {
		t.Fatalf("unexpected truncation reporting: %#v", payload)
	}
}
