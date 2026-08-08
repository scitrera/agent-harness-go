package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	workspaceModeSingle  = "single"
	workspaceModeProject = "project"
)

// resolveAppWorkspace keeps the historical unscoped layout in single mode
// unless an ID is explicitly pinned. Project mode derives a stable workspace
// from the process cwd and the shared canonical-path index.
func resolveAppWorkspace(ctx context.Context, mode, configuredID, indexDir, cwd string) (workspacepkg.Resolution, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	configuredID = strings.TrimSpace(configuredID)
	switch mode {
	case "", workspaceModeSingle:
		if configuredID == "" {
			return workspacepkg.Resolution{Source: workspacepkg.SourceDefault}, nil
		}
		return workspacepkg.Resolution{WorkspaceID: configuredID, Source: workspacepkg.SourcePinned}, nil
	case workspaceModeProject:
		if configuredID != "" {
			return workspacepkg.Resolution{WorkspaceID: configuredID, Source: workspacepkg.SourcePinned}, nil
		}
		resolver, err := workspacepkg.NewResolver(workspacepkg.Config{
			StateDir: indexDir,
		})
		if err != nil {
			return workspacepkg.Resolution{}, err
		}
		if strings.TrimSpace(cwd) == "" {
			cwd, err = os.Getwd()
			if err != nil {
				return workspacepkg.Resolution{}, fmt.Errorf("current directory: %w", err)
			}
		}
		return resolver.Resolve(ctx, workspacepkg.Request{CWD: cwd})
	default:
		return workspacepkg.Resolution{}, fmt.Errorf("unknown --workspace-mode %q (want single|project)", mode)
	}
}

func defaultWorkspaceIndexDir() string {
	if dir, err := os.UserConfigDir(); err == nil && dir != "" {
		return filepath.Join(dir, "agent-harness")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".agent-harness")
	}
	return ""
}

func effectiveWorkspace(configured, resolved string) string {
	if configured = strings.TrimSpace(configured); configured != "" {
		return configured
	}
	if resolved = strings.TrimSpace(resolved); resolved != "" {
		return resolved
	}
	return "default"
}
