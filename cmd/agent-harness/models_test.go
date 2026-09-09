// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
)

func TestLoadAppModelRegistryMissingKeepsSingleModel(t *testing.T) {
	registry, err := loadAppModelRegistry(t.TempDir(), "")
	if err != nil {
		t.Fatalf("load missing registry: %v", err)
	}
	if registry != nil {
		t.Fatalf("registry = %#v, want nil", registry)
	}
	if got := resolveAppModel("env-model", registry); got != "env-model" {
		t.Fatalf("resolved model = %q, want env-model", got)
	}
}

func TestLoadAppModelRegistrySelectsValidFileDefault(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, modelsConfigRelativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`default: fast
providers:
  - name: local
    base_url: http://127.0.0.1:11434/v1
models:
  - name: fast
    provider: local
    capabilities: {tools: true}
  - name: vision
    capabilities: {tools: true, vision: true}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	registry, err := loadAppModelRegistry(root, "")
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if got := resolveAppModel("env-model", registry); got != "fast" {
		t.Fatalf("resolved model = %q, want fast", got)
	}
	if provider, ok := registry.ProviderFor("fast"); !ok || provider.Name != "local" {
		t.Fatalf("fast provider = %#v, %v", provider, ok)
	}
	vision, ok := registry.Get("vision")
	if !ok || !vision.Capabilities.Vision {
		t.Fatalf("vision model = %#v, %v", vision, ok)
	}
}

func TestLoadAppModelRegistryRejectsMalformedFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, modelsConfigRelativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("models: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadAppModelRegistry(root, "")
	if !errors.Is(err, modelpkg.ErrInvalidRegistryFile) {
		t.Fatalf("error = %v, want ErrInvalidRegistryFile", err)
	}
}

func TestLoadAppModelRegistryExplicitRelativePath(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "operator", "registry.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("default: host\nmodels:\n  - name: host\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	registry, err := loadAppModelRegistry(root, "operator/registry.yaml")
	if err != nil {
		t.Fatalf("load explicit relative registry: %v", err)
	}
	if registry.DefaultName() != "host" {
		t.Fatalf("default = %q, want host", registry.DefaultName())
	}
}

func TestLoadAppModelRegistryExplicitMissingFails(t *testing.T) {
	_, err := loadAppModelRegistry(t.TempDir(), "missing/models.yaml")
	if err == nil || !strings.Contains(err.Error(), "explicit models file") {
		t.Fatalf("missing explicit registry error = %v", err)
	}
}

func TestResolveAppModelFallsBackForUnknownDefault(t *testing.T) {
	registry := modelpkg.NewRegistry([]modelpkg.Model{{Name: "known"}}, "missing")
	if got := resolveAppModel("env-model", registry); got != "env-model" {
		t.Fatalf("resolved model = %q, want env-model", got)
	}
}
