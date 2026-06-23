package skills

import (
	"os"
	"path/filepath"
	"testing"
)

func writeSkill(t *testing.T, root, dir, name, content string) {
	t.Helper()
	folder := filepath.Join(root, dir, name)
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	if content != "" {
		if err := os.WriteFile(filepath.Join(folder, "SKILL.md"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDiscoverFrontmatterAndFallback(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, ".memorylayer-skills", "contract-review",
		"---\nname: Contract Review\ndescription: Review contracts for risk\n---\n# Contract Review\nbody")
	writeSkill(t, root, "skills", "data-analysis",
		"# Data Analysis\nAnalyze CSVs and produce charts.")
	writeSkill(t, root, "skills", "not-a-skill", "") // folder without SKILL.md -> ignored

	specs, err := Discover(root, []string{".memorylayer-skills", "skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	byName := map[string]struct {
		desc, path string
	}{}
	for _, s := range specs {
		byName[s.Name] = struct{ desc, path string }{s.Description, s.Path}
		if !s.Enabled {
			t.Fatalf("%s should be enabled", s.Name)
		}
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 skills, got %d: %#v", len(specs), specs)
	}
	cr, ok := byName["Contract Review"]
	if !ok || cr.desc != "Review contracts for risk" || cr.path != filepath.Join(".memorylayer-skills", "contract-review", "SKILL.md") {
		t.Fatalf("contract review skill wrong: %+v", cr)
	}
	da, ok := byName["data-analysis"]
	if !ok || da.desc != "Analyze CSVs and produce charts." {
		t.Fatalf("data-analysis skill wrong (name from folder, desc from first line): %+v", da)
	}
}

func TestDiscoverDedupFirstDirWins(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, ".memorylayer-skills", "shared", "---\nname: shared\ndescription: from primary\n---\n")
	writeSkill(t, root, "skills", "shared", "---\nname: shared\ndescription: from secondary\n---\n")

	specs, err := Discover(root, []string{".memorylayer-skills", "skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 1 || specs[0].Description != "from primary" {
		t.Fatalf("expected first-dir-wins dedup, got %#v", specs)
	}
}

func TestDiscoverMissingDirs(t *testing.T) {
	specs, err := Discover(t.TempDir(), []string{"does-not-exist"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 0 {
		t.Fatalf("expected no skills, got %#v", specs)
	}
}
