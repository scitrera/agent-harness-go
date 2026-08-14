package configpath

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseListPreservesOrderAndSkipsEmpty(t *testing.T) {
	want := []string{"skills", "/opt/skills", "later"}
	if got := ParseList(" skills, /opt/skills ,, later "); !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseList() = %#v, want %#v", got, want)
	}
}

func TestValidateAbsolute(t *testing.T) {
	if err := ValidateAbsolute("system skills", []string{"/opt/skills", "/srv/more"}); err != nil {
		t.Fatalf("absolute roots: %v", err)
	}
	err := ValidateAbsolute("system skills", []string{"/opt/skills", "relative"})
	if err == nil || !strings.Contains(err.Error(), "relative") {
		t.Fatalf("relative root error = %v", err)
	}
}

func TestValidateRelative(t *testing.T) {
	if err := ValidateRelative("workspace skills", []string{"skills", ".agent-harness-skills"}); err != nil {
		t.Fatalf("relative roots: %v", err)
	}
	err := ValidateRelative("workspace skills", []string{"skills", "/opt/skills"})
	if err == nil || !strings.Contains(err.Error(), "/opt/skills") {
		t.Fatalf("absolute workspace-root error = %v", err)
	}
	err = ValidateRelative("workspace skills", []string{"skills", "nested/../../outside"})
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("escaping workspace-root error = %v", err)
	}
}

func TestResolveFile(t *testing.T) {
	root := t.TempDir()
	if got, want := ResolveFile(root, "config/models.yaml"), filepath.Join(root, "config/models.yaml"); got != want {
		t.Fatalf("relative = %q, want %q", got, want)
	}
	abs := filepath.Join(t.TempDir(), "models.yaml")
	if got := ResolveFile(root, abs); got != abs {
		t.Fatalf("absolute = %q, want %q", got, abs)
	}
	if got := ResolveFile(root, "  "); got != "" {
		t.Fatalf("empty = %q", got)
	}
}
