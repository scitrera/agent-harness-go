// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package mcp

import (
	"context"
	"testing"
	"time"
)

func Test_Manager_Register_keeps_server_cold_until_ensure_started(t *testing.T) {
	// Given
	ctx := context.Background()
	now := time.Unix(100, 0)
	manager := NewManager(func() time.Time { return now })
	if err := manager.Register(ServerConfig{
		Name:    "search",
		Command: "/bin/sh",
		Args:    []string{"-c", "trap 'exit 0' TERM; while true; do sleep 1; done"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// When
	cold := manager.List()
	started, err := manager.EnsureStarted(ctx, "search")
	startedAgain, errAgain := manager.EnsureStarted(ctx, "search")
	stopErr := manager.Stop(ctx, "search")
	stopped := manager.List()

	// Then
	if len(cold) != 1 || cold[0].Running || cold[0].PID != 0 {
		t.Fatalf("expected cold server metadata, got %#v", cold)
	}
	if err != nil || errAgain != nil {
		t.Fatalf("start errors: %v %v", err, errAgain)
	}
	if !started.Running || started.PID == 0 {
		t.Fatalf("expected running server, got %#v", started)
	}
	if startedAgain.PID != started.PID {
		t.Fatalf("expected same pid for already running server: %d != %d", startedAgain.PID, started.PID)
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}
	if len(stopped) != 1 || stopped[0].Running {
		t.Fatalf("expected stopped server, got %#v", stopped)
	}
}

func Test_Manager_StopIdle_stops_server_after_idle_ttl(t *testing.T) {
	// Given
	ctx := context.Background()
	now := time.Unix(200, 0)
	manager := NewManager(func() time.Time { return now })
	if err := manager.Register(ServerConfig{
		Name:    "calc",
		Command: "/bin/sh",
		Args:    []string{"-c", "trap 'exit 0' TERM; while true; do sleep 1; done"},
		IdleTTL: time.Second,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := manager.EnsureStarted(ctx, "calc"); err != nil {
		t.Fatalf("start: %v", err)
	}

	// When
	now = now.Add(2 * time.Second)
	err := manager.StopIdle(ctx)

	// Then
	if err != nil {
		t.Fatalf("stop idle: %v", err)
	}
	statuses := manager.List()
	if len(statuses) != 1 || statuses[0].Running {
		t.Fatalf("expected idle server to stop, got %#v", statuses)
	}
}
