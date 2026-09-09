// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// WorkingDirectoryMetaKey carries a client-selected per-turn working directory.
// The local workspace still enforces its read/write roots; this value only
// changes how otherwise-relative tool paths are resolved.
const WorkingDirectoryMetaKey = "working_directory"

type workingDirectoryKey struct{}

// StampWorkingDirectory records cwd on an inbound message. Empty and relative
// values are ignored so an untrusted message cannot silently reinterpret paths.
func StampWorkingDirectory(message *protocol.ChatMessage, cwd string) {
	if message == nil || !filepath.IsAbs(strings.TrimSpace(cwd)) {
		return
	}
	raw, err := json.Marshal(filepath.Clean(cwd))
	if err != nil {
		return
	}
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[WorkingDirectoryMetaKey] = raw
}

// MessageWorkingDirectory returns an absolute client-selected cwd when present.
func MessageWorkingDirectory(message protocol.ChatMessage) (string, bool) {
	raw := message.Meta[WorkingDirectoryMetaKey]
	if len(raw) == 0 {
		return "", false
	}
	var cwd string
	if err := json.Unmarshal(raw, &cwd); err != nil || !filepath.IsAbs(strings.TrimSpace(cwd)) {
		return "", false
	}
	return filepath.Clean(cwd), true
}

// WithWorkingDirectory carries the active cwd across the whole tool loop,
// including synchronous subagents spawned from that turn.
func WithWorkingDirectory(ctx context.Context, cwd string) context.Context {
	if !filepath.IsAbs(strings.TrimSpace(cwd)) {
		return ctx
	}
	return context.WithValue(ctx, workingDirectoryKey{}, filepath.Clean(cwd))
}

// WorkingDirectoryFrom returns the active per-turn cwd.
func WorkingDirectoryFrom(ctx context.Context) (string, bool) {
	cwd, ok := ctx.Value(workingDirectoryKey{}).(string)
	return cwd, ok && cwd != ""
}

// ResolveWorkingPath resolves a tool path from the active per-turn cwd. With no
// active cwd it preserves the original argument byte-for-byte.
func ResolveWorkingPath(ctx context.Context, path string) string {
	cwd, ok := WorkingDirectoryFrom(ctx)
	if !ok || filepath.IsAbs(path) {
		return path
	}
	if strings.TrimSpace(path) == "" {
		return cwd
	}
	return filepath.Join(cwd, path)
}
