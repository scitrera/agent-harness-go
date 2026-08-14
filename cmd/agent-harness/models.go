package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/configpath"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
)

const modelsConfigRelativePath = "config/models.yaml"

// loadAppModelRegistry loads the OSS model registry from an explicit operator
// path or the conventional workspace path. The conventional file is optional;
// an explicitly selected missing file is a configuration error.
func loadAppModelRegistry(workspaceRoot, configuredFile string) (*modelpkg.Registry, error) {
	path := filepath.Join(workspaceRoot, modelsConfigRelativePath)
	explicit := strings.TrimSpace(configuredFile) != ""
	if explicit {
		path = configpath.ResolveFile(workspaceRoot, configuredFile)
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("explicit models file %s: %w", path, err)
		}
	}
	return modelpkg.LoadRegistry(path)
}

// resolveAppModel makes a valid registry default authoritative when a registry
// is present. A missing/unknown default retains the CLI/env fallback, matching
// the existing distribution composition behavior without making the registry
// parser responsible for deployment policy.
func resolveAppModel(fallback string, registry *modelpkg.Registry) string {
	if registry == nil {
		return fallback
	}
	name := registry.DefaultName()
	if name == "" {
		return fallback
	}
	if _, ok := registry.Get(name); !ok {
		return fallback
	}
	return name
}
