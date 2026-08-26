package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/channels/tui"
)

func openTUIShellPreferences(cfg appConfig) (tui.ShellPreferenceStore, error) {
	path := strings.TrimSpace(cfg.tuiShellPreferencesFile)
	if path == "" {
		path = filepath.Join(workspaceStateDir(cfg), "tui-shell-preferences.json")
	}
	store, err := tui.NewFileShellPreferenceStore(path)
	if err != nil {
		return nil, fmt.Errorf("open TUI shell preferences: %w", err)
	}
	return store, nil
}

func defaultTUIShellPreferencesFile() string {
	dir := defaultWorkspaceIndexDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "tui-shell-preferences.json")
}
