package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_Write_creates_parent_and_file(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "state.json")
	if err := Write(path, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != `{"a":1}` {
		t.Fatalf("content = %q", got)
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o644 {
		t.Fatalf("perm = %v, want 0644", info.Mode().Perm())
	}
}

func Test_Write_overwrites_and_leaves_no_temp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := Write(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := Write(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("second write: %v", err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "new" {
		t.Fatalf("content = %q, want new", got)
	}
	// No stray temp file left behind (the .tmp leak the old copies had).
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected only the target file, got %v", names)
	}
}
