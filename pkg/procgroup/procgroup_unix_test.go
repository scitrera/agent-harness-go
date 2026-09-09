// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

//go:build !windows

package procgroup_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/procgroup"
)

// The whole point of the package: a grandchild must not outlive the kill.
// Killing cmd.Process alone reaps the shell and orphans the sleep.
func TestKillReachesGrandchild(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	// Background a sleep, publish its pid, then block so the shell stays alive
	// and the group has two members when the kill lands.
	cmd := exec.Command("sh", "-c", `sleep 60 & echo $! > "$1"; wait`, "sh", pidFile)
	cmd.SysProcAttr = procgroup.Attr()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	grandchild := waitForPID(t, pidFile)
	if err := syscall.Kill(grandchild, 0); err != nil {
		t.Fatalf("grandchild %d not running before kill: %v", grandchild, err)
	}

	if err := procgroup.Kill(cmd.Process); err != nil {
		t.Fatalf("kill group: %v", err)
	}
	_ = cmd.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(grandchild, 0); err != nil {
			return // ESRCH: gone, as intended
		}
		if time.Now().After(deadline) {
			// Clean up so a failure does not leak a 60s sleep into the suite.
			_ = syscall.Kill(grandchild, syscall.SIGKILL)
			t.Fatalf("grandchild %d survived the group kill", grandchild)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNilProcessIsANoOp(t *testing.T) {
	if err := procgroup.Kill(nil); err != nil {
		t.Fatalf("Kill(nil): %v", err)
	}
	if err := procgroup.Terminate(nil); err != nil {
		t.Fatalf("Terminate(nil): %v", err)
	}
}

func waitForPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild never published its pid to %s", path)
	return 0
}
