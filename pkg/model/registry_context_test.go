package model

import (
	"os"
	"path/filepath"
	"testing"
)

// The per-model `context:` window is loaded from models.yaml into Model.Context
// (previously parsed-but-ignored). Absent → 0, which callers treat as "unknown".
func TestLoadRegistryContextWindow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "models.yaml")
	yaml := "" +
		"default: orch\n" +
		"models:\n" +
		"  - name: orch\n" +
		"    context: 1000000\n" +
		"    capabilities: {tools: true, vision: false}\n" +
		"  - name: vision\n" +
		"    context: 262000\n" +
		"    capabilities: {tools: true, vision: true}\n" +
		"  - name: nowindow\n" +
		"    capabilities: {tools: true}\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write models.yaml: %v", err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	for name, want := range map[string]int{"orch": 1000000, "vision": 262000, "nowindow": 0} {
		m, ok := reg.Get(name)
		if !ok {
			t.Fatalf("model %q missing", name)
		}
		if m.Context != want {
			t.Fatalf("model %q context = %d, want %d", name, m.Context, want)
		}
	}
}
