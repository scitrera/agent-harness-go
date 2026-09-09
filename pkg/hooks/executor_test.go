// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func Test_Executor_RunCommandHook_returns_structured_output_when_command_succeeds(t *testing.T) {
	// Given
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})
	hook := helperHook("success")

	// When
	results, err := exec.Dispatch(context.Background(), Invocation{
		Event: EventCommand,
		Input: json.RawMessage(`{"command":"build"}`),
	}, []CommandHook{hook})

	// Then
	if err != nil {
		t.Fatalf("dispatch hook: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %d", len(results))
	}
	if results[0].ExitCode != 0 || results[0].Output.Decision != DecisionAllow {
		t.Fatalf("unexpected result: %#v", results[0])
	}
}

func Test_Executor_RunCommandHook_blocks_when_command_exits_two(t *testing.T) {
	// Given
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})
	hook := helperHook("block")
	hook.Event = EventPreToolUse

	// When
	results, err := exec.Dispatch(context.Background(), Invocation{Event: EventPreToolUse}, []CommandHook{hook})

	// Then
	if err != nil {
		t.Fatalf("dispatch hook: %v", err)
	}
	if len(results) != 1 || !results[0].Blocked || results[0].Reason != "blocked by hook" {
		t.Fatalf("expected blocking result, got %#v", results)
	}
}

func Test_Executor_RunCommandHook_records_nonblocking_failure(t *testing.T) {
	// Given
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})
	hook := helperHook("fail")

	// When
	results, err := exec.Dispatch(context.Background(), Invocation{Event: EventCommand}, []CommandHook{hook})

	// Then
	if err != nil {
		t.Fatalf("dispatch hook: %v", err)
	}
	if len(results) != 1 || results[0].Blocked || results[0].ExitCode != 1 || results[0].Stderr != "soft hook failure" {
		t.Fatalf("expected nonblocking failure result, got %#v", results)
	}
}

func Test_Executor_RunCommandHook_times_out_when_command_hangs(t *testing.T) {
	// Given
	exec := NewExecutor(Config{DefaultTimeout: 20 * time.Millisecond})
	hook := helperHook("sleep")

	// When
	_, err := exec.Dispatch(context.Background(), Invocation{Event: EventCommand}, []CommandHook{hook})

	// Then
	if !errors.Is(err, ErrHookTimeout) {
		t.Fatalf("expected ErrHookTimeout, got %v", err)
	}
}

func Test_Executor_RunCommandHook_respects_context_cancellation(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})

	// When
	_, err := exec.Dispatch(ctx, Invocation{Event: EventCommand}, []CommandHook{helperHook("success")})

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func Test_Executor_RunCommandHook_copies_only_allowed_environment(t *testing.T) {
	// Given
	t.Setenv("HOOK_ALLOWED", "visible")
	t.Setenv("HOOK_SECRET", "hidden")
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second, EnvAllowlist: []string{"HOOK_ALLOWED"}})

	// When
	results, err := exec.Dispatch(context.Background(), Invocation{Event: EventCommand}, []CommandHook{helperHook("env")})

	// Then
	if err != nil {
		t.Fatalf("dispatch hook: %v", err)
	}
	env := results[0].Output.Env
	if env["HOOK_ALLOWED"] != "visible" {
		t.Fatalf("allowed env missing: %#v", env)
	}
	if _, ok := env["HOOK_SECRET"]; ok {
		t.Fatalf("secret env leaked: %#v", env)
	}
}

func Test_Executor_Dispatch_skips_when_recursion_guard_is_active(t *testing.T) {
	// Given
	recordFile := tempRecordFile(t)
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})
	hook := helperHook("record")
	hook.Env["HOOK_RECORD_FILE"] = recordFile

	// When
	results, err := exec.Dispatch(WithRecursionGuard(context.Background()), Invocation{Event: EventCommand}, []CommandHook{hook})

	// Then
	if err != nil {
		t.Fatalf("dispatch hook: %v", err)
	}
	if len(results) != 1 || !results[0].Skipped {
		t.Fatalf("expected skipped recursion result, got %#v", results)
	}
	if b, err := os.ReadFile(recordFile); err == nil && len(b) > 0 {
		t.Fatalf("recursive hook should not execute, record=%q", string(b))
	}
}

func Test_Executor_Dispatch_runs_matching_hooks_in_declared_order(t *testing.T) {
	// Given
	recordFile := tempRecordFile(t)
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})
	first := helperHook("record")
	first.Name = "first"
	first.Env["HOOK_RECORD_FILE"] = recordFile
	first.Env["HOOK_NAME"] = "first"
	second := helperHook("record")
	second.Name = "second"
	second.Env["HOOK_RECORD_FILE"] = recordFile
	second.Env["HOOK_NAME"] = "second"

	// When
	_, err := exec.Dispatch(context.Background(), Invocation{Event: EventCommand}, []CommandHook{first, second})

	// Then
	if err != nil {
		t.Fatalf("dispatch hook: %v", err)
	}
	got, err := os.ReadFile(recordFile)
	if err != nil {
		t.Fatalf("read record file: %v", err)
	}
	if string(got) != "first\nsecond\n" {
		t.Fatalf("hooks ran out of order: %q", string(got))
	}
}

func Test_Executor_RunCommandHook_rejects_malformed_json_output(t *testing.T) {
	// Given
	exec := NewExecutor(Config{DefaultTimeout: 5 * time.Second})

	// When
	_, err := exec.Dispatch(context.Background(), Invocation{Event: EventCommand}, []CommandHook{helperHook("malformed")})

	// Then
	if !errors.Is(err, ErrMalformedHookOutput) {
		t.Fatalf("expected ErrMalformedHookOutput, got %v", err)
	}
}
