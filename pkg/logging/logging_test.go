// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package logging

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupWritesFileDestination(t *testing.T) {
	// Given: file-only logging configured with the existing log knobs.
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	path := filepath.Join(t.TempDir(), "logs", "agent-harness.log")

	// When: logging is installed and a record is emitted.
	err := Setup(Config{Destination: "file", File: path, Level: "info", Format: "text"})
	if err != nil {
		t.Fatalf("setup logging: %v", err)
	}
	slog.Info("tui log smoke", slog.String("mode", "tui"))

	// Then: the record lands in the configured file.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if !strings.Contains(string(data), "tui log smoke") {
		t.Fatalf("log file missing record: %s", data)
	}
}
