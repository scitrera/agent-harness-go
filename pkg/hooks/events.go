// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrInvalidHookCommand  = errors.New("hooks: invalid command hook")
	ErrHookTimeout         = errors.New("hooks: command hook timed out")
	ErrMalformedHookOutput = errors.New("hooks: malformed hook JSON output")
)

type EventName string

const (
	EventPreToolUse         EventName = "PreToolUse"
	EventPostToolUse        EventName = "PostToolUse"
	EventPostToolUseFailure EventName = "PostToolUseFailure"
	EventCommand            EventName = "Command"
	EventUserPromptSubmit   EventName = "UserPromptSubmit"
	EventSessionStart       EventName = "SessionStart"
	EventSessionEnd         EventName = "SessionEnd"
	EventTaskCreated        EventName = "TaskCreated"
	EventTaskCompleted      EventName = "TaskCompleted"
	EventPreCompact         EventName = "PreCompact"
	EventPostCompact        EventName = "PostCompact"
	EventCWDChanged         EventName = "CwdChanged"
	EventFileChanged        EventName = "FileChanged"
	EventToolLifecycle      EventName = "ToolLifecycle"
)

const (
	DecisionAllow = "allow"
	DecisionDeny  = "deny"
)

type Config struct {
	DefaultTimeout time.Duration
	EnvAllowlist   []string
}

type CommandHook struct {
	Name    string
	Event   EventName
	Command []string
	Timeout time.Duration
	Env     map[string]string
}

type Invocation struct {
	Event EventName       `json:"event"`
	Input json.RawMessage `json:"input,omitempty"`
	Tool  *ToolCall       `json:"tool,omitempty"`
}

type HookOutput struct {
	Decision   string            `json:"decision,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	Message    string            `json:"message,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	WatchPaths []string          `json:"watch_paths,omitempty"`
}

type Result struct {
	HookName string
	Event    EventName
	ExitCode int
	Stdout   string
	Stderr   string
	Output   HookOutput
	Blocked  bool
	Reason   string
	Skipped  bool
}

type recursionGuardKey struct{}

func WithRecursionGuard(ctx context.Context) context.Context {
	return context.WithValue(ctx, recursionGuardKey{}, true)
}

func recursionGuarded(ctx context.Context) bool {
	active, _ := ctx.Value(recursionGuardKey{}).(bool)
	return active
}
