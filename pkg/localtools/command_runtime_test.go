package localtools

import (
	"context"
	"strings"
	"testing"
	"time"
)

func Test_Workspace_RunCommand_reports_truncated_output_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	ws := newTestWorkspace(t)

	// When
	got, err := ws.RunCommand(ctx, CommandSpec{
		Name:      "printf",
		Args:      []string{"abcdef"},
		Timeout:   5 * time.Second,
		MaxOutput: 3,
	})

	// Then
	if err != nil {
		t.Fatalf("run command: %v", err)
	}
	if got.Output != "abc" {
		t.Fatalf("output = %q, want abc", got.Output)
	}
	if !got.OutputTruncated {
		t.Fatalf("expected output truncation metadata: %#v", got)
	}
	if got.OutputBytes != len("abcdef") {
		t.Fatalf("output bytes = %d, want 6", got.OutputBytes)
	}
	if got.PID <= 0 {
		t.Fatalf("expected process id metadata, got %#v", got)
	}
}

func Test_Workspace_RunCommand_reports_timeout_as_truncated_result_metadata(t *testing.T) {
	// Given
	ctx := context.Background()
	ws := newTestWorkspace(t)

	// When
	got, err := ws.RunCommand(ctx, CommandSpec{
		Name:      "sh",
		Args:      []string{"-c", "printf start; sleep 2"},
		Timeout:   10 * time.Millisecond,
		MaxOutput: 64,
	})

	// Then
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if got.ExitCode != -1 {
		t.Fatalf("exit code = %d, want -1", got.ExitCode)
	}
	if !strings.Contains(got.Output, "start") {
		t.Fatalf("expected partial output, got %q", got.Output)
	}
	if got.PID <= 0 {
		t.Fatalf("expected process id metadata on timeout, got %#v", got)
	}
}
