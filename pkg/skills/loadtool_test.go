package skills

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/tools"
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

func TestBuildRegistry_ParsesPreferredModel(t *testing.T) {
	body := "---\nname: vis\ndescription: d\nmetadata:\n  scitrera:\n    preferred_model: sahara-vision-advanced\n---\nbody"
	reg := BuildRegistry([]catalog.SkillSpec{{Name: "vis", Content: body, Enabled: true}}, t.TempDir())
	if got := reg.byName["vis"].PreferredModel; got != "sahara-vision-advanced" {
		t.Fatalf("PreferredModel = %q, want sahara-vision-advanced", got)
	}
}

func TestLoadToolReturnsAuditableSkillReferences(t *testing.T) {
	reg := BuildRegistry([]catalog.SkillSpec{{Name: "review", Content: "instructions", Enabled: true}}, t.TempDir())
	result, err := LoadTool(reg)(context.Background(), tools.Request{
		CallID: "load-1", Name: LoadToolName, Arguments: json.RawMessage(`{"name":"review"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Metadata.References) != 1 {
		t.Fatalf("references = %#v", result.Metadata.References)
	}
	reference := result.Metadata.References[0]
	if reference.System != "skill-catalog" || reference.Kind != "target" || reference.ID != "review" {
		t.Fatalf("reference = %#v", reference)
	}
}

// A MemoryLayer catalog strips SKILL.md frontmatter into a metadata field, so the
// Content is frontmatter-less and prereqs/preferred_model arrive on the spec. Those
// must be used (else no auto prereq load, no model switch for ML skills).
func TestBuildRegistry_UsesSpecPrereqsAndModelWhenBodyHasNoFrontmatter(t *testing.T) {
	reg := BuildRegistry([]catalog.SkillSpec{{
		Name:           "p2",
		Content:        "just the body, no --- frontmatter",
		Enabled:        true,
		Prereqs:        []string{"p0-a", "p1-b"},
		PreferredModel: "sahara-text-advanced",
	}}, t.TempDir())
	sk := reg.byName["p2"]
	if sk == nil {
		t.Fatal("p2 not registered")
	}
	if got := sk.PreferredModel; got != "sahara-text-advanced" {
		t.Fatalf("PreferredModel = %q, want sahara-text-advanced", got)
	}
	if len(sk.Prereqs) != 2 || sk.Prereqs[0] != "p0-a" || sk.Prereqs[1] != "p1-b" {
		t.Fatalf("Prereqs = %v, want [p0-a p1-b]", sk.Prereqs)
	}
}

// load_skill honors the target skill's preferred_model via the ctx ModelPreference
// seam: it calls the pin fn and reports model_switched only when the fn applied it.
func TestLoadTool_PreferredModelSwitch(t *testing.T) {
	withModel := "---\nname: vis\ndescription: d\nmetadata:\n  scitrera:\n    preferred_model: m-vision\n---\nbody"
	plain := "---\nname: plain\ndescription: d\n---\nbody"
	reg := BuildRegistry([]catalog.SkillSpec{
		{Name: "vis", Content: withModel, Enabled: true},
		{Name: "plain", Content: plain, Enabled: true},
	}, t.TempDir())

	load := func(t *testing.T, name string, available bool) (asked string, payload map[string]any) {
		t.Helper()
		ctx := tools.WithModelPreference(context.Background(), func(m string) bool {
			asked = m
			return available
		})
		res, err := LoadTool(reg)(ctx, tools.Request{Name: LoadToolName, Arguments: json.RawMessage(`{"name":"` + name + `"}`)})
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		if err := json.Unmarshal(res.Payload, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		return asked, payload
	}

	// Available: pin fn called with the preferred model + result records the switch.
	if asked, p := load(t, "vis", true); asked != "m-vision" || p["model_switched"] != "m-vision" {
		t.Fatalf("available: asked=%q model_switched=%v", asked, p["model_switched"])
	}
	// Unavailable: pin fn is consulted but declines, so no switch is claimed.
	if asked, p := load(t, "vis", false); asked != "m-vision" || p["model_switched"] != nil {
		t.Fatalf("unavailable: asked=%q model_switched=%v (want none)", asked, p["model_switched"])
	}
	// No preferred_model: the pin fn is never called.
	if asked, p := load(t, "plain", true); asked != "" || p["model_switched"] != nil {
		t.Fatalf("plain: asked=%q model_switched=%v (want none)", asked, p["model_switched"])
	}
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

func TestLoadTool_UsesContextualWorkspaceRegistry(t *testing.T) {
	defaultRegistry := BuildRegistry([]catalog.SkillSpec{{
		Name: "default-skill", Content: "default body", Enabled: true,
	}}, t.TempDir())
	projectRegistry := BuildRegistry([]catalog.SkillSpec{{
		Name: "project-skill", Content: "project body", Enabled: true,
	}}, t.TempDir())
	handler := LoadTool(defaultRegistry)
	ctx := WithRegistry(context.Background(), projectRegistry)

	result, err := handler(ctx, tools.Request{
		CallID: "call-1", Name: LoadToolName,
		Arguments: json.RawMessage(`{"name":"project-skill"}`),
	})
	if err != nil || result.IsError {
		t.Fatalf("load contextual skill: result=%s err=%v", result.Payload, err)
	}
	if !strings.Contains(string(result.Payload), "project body") || strings.Contains(string(result.Payload), "default body") {
		t.Fatalf("contextual payload = %s", result.Payload)
	}

	missing, err := handler(ctx, tools.Request{
		CallID: "call-2", Name: LoadToolName,
		Arguments: json.RawMessage(`{"name":"default-skill"}`),
	})
	if err != nil || !missing.IsError {
		t.Fatalf("cross-workspace skill leaked: result=%s err=%v", missing.Payload, err)
	}
}
