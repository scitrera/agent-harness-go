// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"fmt"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// ViewMutatingTool reports tools that can modify the selected execution view.
// Shell and Python are conservatively mutating because their payloads are
// general programs and cannot be proven read-only from the tool name alone.
func ViewMutatingTool(name string) bool {
	switch name {
	case "write_file", "edit_file", "apply_patch", "shell", "python":
		return true
	default:
		return false
	}
}

type viewWriteAccessKey struct{}

// WithViewWriteAccess applies a local execution ceiling even when standalone
// mode has no explicit distributed view binding.
func WithViewWriteAccess(ctx context.Context, access workspacepkg.ViewWriteAccess) context.Context {
	switch access {
	case workspacepkg.ViewWriteAccessReadOnly, workspacepkg.ViewWriteAccessReadWrite:
		return context.WithValue(ctx, viewWriteAccessKey{}, access)
	default:
		return ctx
	}
}

func authorizeExecutionScope(ctx context.Context, req Request) error {
	readOnly := false
	if access, ok := ctx.Value(viewWriteAccessKey{}).(workspacepkg.ViewWriteAccess); ok {
		readOnly = access == workspacepkg.ViewWriteAccessReadOnly
	}
	scope, ok := workspacepkg.ExecutionScopeFrom(ctx)
	if ok && scope.Policy.WriteAccess == workspacepkg.ViewWriteAccessReadOnly {
		readOnly = true
	}
	if !readOnly || !ViewMutatingTool(req.Name) {
		return nil
	}
	return fmt.Errorf("workspace execution view is read-only: tool %q requires write admission", req.Name)
}
