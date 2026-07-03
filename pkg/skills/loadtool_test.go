package skills

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
)

func writePrereqSkill(t *testing.T, root, name string, prereqs []string) catalog.SkillSpec {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + name + "\ndescription: test skill " + name + "\n"
	if len(prereqs) > 0 {
		body += "metadata:\n  scitrera:\n    prereq_skills:\n"
		for _, p := range prereqs {
			body += "      - " + p + "\n"
		}
	}
	body += "---\n\n# " + name + "\nbody of " + name + "\n"
	path := filepath.Join(dir, "SKILL.md")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return catalog.SkillSpec{Name: name, Path: path, Enabled: true}
}

func TestLoadOrder_TransitivePrereqsDedupTargetLast(t *testing.T) {
	root := t.TempDir()
	reg := BuildRegistry([]catalog.SkillSpec{
		writePrereqSkill(t, root, "a", []string{"b", "c"}),
		writePrereqSkill(t, root, "b", []string{"c"}),
		writePrereqSkill(t, root, "c", nil),
	}, root)
	order, warnings := reg.loadOrder("a", true)
	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
	want := []string{"c", "b", "a"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestLoadOrder_MissingPrereqWarnsButLoadsTarget(t *testing.T) {
	root := t.TempDir()
	reg := BuildRegistry([]catalog.SkillSpec{writePrereqSkill(t, root, "a", []string{"ghost"})}, root)
	order, warnings := reg.loadOrder("a", true)
	if len(order) != 1 || order[0] != "a" {
		t.Fatalf("order = %v, want [a]", order)
	}
	if len(warnings) == 0 {
		t.Fatal("expected a missing-prereq warning")
	}
}

func TestLoadOrder_CycleBreaks(t *testing.T) {
	root := t.TempDir()
	reg := BuildRegistry([]catalog.SkillSpec{
		writePrereqSkill(t, root, "x", []string{"y"}),
		writePrereqSkill(t, root, "y", []string{"x"}),
	}, root)
	order, warnings := reg.loadOrder("x", true)
	if len(order) != 2 || order[len(order)-1] != "x" {
		t.Fatalf("order = %v, want 2 entries ending in x", order)
	}
	if len(warnings) == 0 {
		t.Fatal("expected a cycle warning")
	}
}

func TestBuildRegistry_ParsesPrereqsAndBody(t *testing.T) {
	root := t.TempDir()
	reg := BuildRegistry([]catalog.SkillSpec{writePrereqSkill(t, root, "a", []string{"b", "c"})}, root)
	sk, ok := reg.byName["a"]
	if !ok {
		t.Fatal("skill a not in registry")
	}
	if len(sk.Prereqs) != 2 || sk.Prereqs[0] != "b" || sk.Prereqs[1] != "c" {
		t.Fatalf("prereqs = %v, want [b c]", sk.Prereqs)
	}
	if sk.Body == "" {
		t.Fatal("body not loaded")
	}
}

func TestBuildRegistry_LoadsInlineContentWithoutPath(t *testing.T) {
	root := t.TempDir()
	body := "---\nname: remote\ndescription: remote skill\n---\n\n# Remote\nUse this skill."
	reg := BuildRegistry([]catalog.SkillSpec{{Name: "remote", Description: "remote skill", Content: body, Enabled: true}}, root)
	sk, ok := reg.byName["remote"]
	if !ok {
		t.Fatal("skill remote not in registry")
	}
	if sk.Body != body {
		t.Fatalf("body = %q, want %q", sk.Body, body)
	}
	if sk.Path != "" {
		t.Fatalf("path = %q, want empty for inline content", sk.Path)
	}
}
