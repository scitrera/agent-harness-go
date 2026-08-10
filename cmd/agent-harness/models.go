package main

import (
	"path/filepath"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
)

const modelsConfigRelativePath = "config/models.yaml"

// loadAppModelRegistry loads the OSS model registry from the same conventional
// workspace path used by distributions built on the core. A missing file keeps
// the historical single-model behavior.
func loadAppModelRegistry(workspaceRoot string) (*modelpkg.Registry, error) {
	return modelpkg.LoadRegistry(filepath.Join(workspaceRoot, modelsConfigRelativePath))
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
