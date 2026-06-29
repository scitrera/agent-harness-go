package main

import (
	"path/filepath"
	"testing"
)

func clearLogEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SAHARA_LOG_DESTINATION", "")
	t.Setenv("SAHARA_LOG_FILE", "")
	t.Setenv("SAHARA_LOG_LEVEL", "")
	t.Setenv("SAHARA_LOG_FORMAT", "")
}

func TestResolveLogConfigDefaultsToFileWhenTUIEnabled(t *testing.T) {
	// Given: no explicit log destination is configured.
	clearLogEnv(t)
	workspace := t.TempDir()

	// When: log config is resolved for TUI mode.
	cfg := resolveLogConfig(workspace, true)

	// Then: logs go only to the workspace log file so they do not corrupt the TUI.
	if cfg.Destination != "file" {
		t.Fatalf("Destination = %q, want file", cfg.Destination)
	}
	wantFile := filepath.Join(workspace, "logs", "agent-harness.log")
	if cfg.File != wantFile {
		t.Fatalf("File = %q, want %q", cfg.File, wantFile)
	}
}

func TestResolveLogConfigHonorsExplicitDestinationWhenTUIEnabled(t *testing.T) {
	// Given: the operator explicitly asks for console logs.
	clearLogEnv(t)
	t.Setenv("SAHARA_LOG_DESTINATION", "console")

	// When: log config is resolved for TUI mode.
	cfg := resolveLogConfig(t.TempDir(), true)

	// Then: the explicit destination wins over the TUI default.
	if cfg.Destination != "console" {
		t.Fatalf("Destination = %q, want console", cfg.Destination)
	}
}

func TestResolveLogConfigKeepsConsoleDefaultOutsideTUI(t *testing.T) {
	// Given: no explicit log destination is configured.
	clearLogEnv(t)

	// When: log config is resolved for the default web mode.
	cfg := resolveLogConfig(t.TempDir(), false)

	// Then: existing non-TUI behavior remains console logging.
	if cfg.Destination != "console" {
		t.Fatalf("Destination = %q, want console", cfg.Destination)
	}
}
