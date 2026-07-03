package model

import (
	"os"
	"path/filepath"
	"testing"
)

func writeModelsFile(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "models.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write models.yaml: %v", err)
	}
	return path
}

func TestLoadRegistryMissingFile(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "nope.yaml"))
	if err != nil || reg != nil {
		t.Fatalf("missing file want (nil,nil), got reg=%v err=%v", reg, err)
	}
}

func TestLoadRegistryProvidersAndModelProvider(t *testing.T) {
	path := writeModelsFile(t, `default: primary
providers:
  - name: openrouter
    base_url: https://openrouter.example/v1
    api_key_env: OPENROUTER_KEY
    format: openai
  - name: local
    base_url: http://llm.local
    api_key: inline-secret
    format: native
models:
  - name: primary
    tier: primary
    provider: openrouter
    capabilities: { vision: true, tools: true }
  - name: bare
    tier: light
    capabilities: { tools: true }
`)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if reg.DefaultName() != "primary" {
		t.Fatalf("default = %q", reg.DefaultName())
	}
	m, ok := reg.Get("primary")
	if !ok || m.Provider != "openrouter" || !m.Capabilities.Vision {
		t.Fatalf("primary model wrong: %+v ok=%v", m, ok)
	}

	// A model referencing a provider resolves to that provider's config.
	pc, ok := reg.ProviderFor("primary")
	if !ok {
		t.Fatal("ProviderFor(primary) not found")
	}
	if pc.Name != "openrouter" || pc.BaseURL != "https://openrouter.example/v1" ||
		pc.APIKeyEnv != "OPENROUTER_KEY" || pc.Format != "openai" {
		t.Fatalf("openrouter config wrong: %+v", pc)
	}

	// A bare model (no provider) → (_, false).
	if _, ok := reg.ProviderFor("bare"); ok {
		t.Fatal("bare model should have no provider")
	}
	// An unknown model → (_, false).
	if _, ok := reg.ProviderFor("ghost"); ok {
		t.Fatal("unknown model should have no provider")
	}
}

func TestLoadRegistryModelReferencesUnknownProvider(t *testing.T) {
	path := writeModelsFile(t, `default: x
models:
  - name: x
    provider: nowhere
    capabilities: { tools: true }
`)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// Provider name is declared on the model but not configured → (_, false):
	// the caller falls back to the default provider (never a hard failure).
	if _, ok := reg.ProviderFor("x"); ok {
		t.Fatal("model referencing an unknown provider should resolve to (_, false)")
	}
}

func TestLoadRegistryEmptyModelsErrors(t *testing.T) {
	path := writeModelsFile(t, "default: x\nmodels: []\n")
	if _, err := LoadRegistry(path); err == nil {
		t.Fatal("no models should error")
	}
}

func TestLoadRegistryMalformedErrors(t *testing.T) {
	path := writeModelsFile(t, "default: [not-a-string\n")
	if _, err := LoadRegistry(path); err == nil {
		t.Fatal("malformed yaml should error")
	}
}

func TestProviderForNilRegistry(t *testing.T) {
	var r *Registry
	if _, ok := r.ProviderFor("x"); ok {
		t.Fatal("nil registry ProviderFor should be false")
	}
}
