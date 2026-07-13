package skills

import (
	"os"
	"path/filepath"
	"strings"
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

// A YAML folded block scalar (description: >-) must resolve to its folded text,
// not leak the ">-" indicator, and multi-line content collapses to one line.
func TestDiscoverFoldedDescription(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "skills", "welcome",
		"---\nname: welcome\ndescription: >-\n  Greet the user and\n  bootstrap the session.\n---\n# Welcome\nbody")
	specs, err := Discover(root, []string{"skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("want 1 skill, got %d", len(specs))
	}
	if got := specs[0].Description; got != "Greet the user and bootstrap the session." {
		t.Fatalf("folded description mis-parsed: %q", got)
	}
}

// A realistic "when to use this" description (well over the old 240 cap) is kept
// in full and produces NO warning — long descriptions are not load failures.
func TestDiscoverLongDescriptionKeptAndNotWarned(t *testing.T) {
	root := t.TempDir()
	desc := strings.TrimSpace(strings.Repeat("word ", 120)) // ~595 chars
	writeSkill(t, root, "skills", "welcome", "---\nname: welcome\ndescription: "+desc+"\n---\nbody")
	specs, warnings, err := DiscoverWithWarnings(root, []string{"skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 1 || specs[0].Description != desc {
		t.Fatalf("description not kept in full: %q", specs[0].Description)
	}
	for _, w := range warnings {
		if strings.Contains(w, "description") {
			t.Fatalf("a long description must not warn: %q", w)
		}
	}
}

// An oversize description is silently truncated (rune-bounded, ellipsis) with no
// warning.
func TestDiscoverOversizeDescriptionSilentlyTruncated(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "skills", "big", "---\nname: big\ndescription: "+strings.Repeat("x", 2000)+"\n---\nbody")
	specs, warnings, err := DiscoverWithWarnings(root, []string{"skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 1 || !strings.HasSuffix(specs[0].Description, "…") {
		t.Fatalf("expected truncation ellipsis, got %q", specs[0].Description)
	}
	if n := len([]rune(specs[0].Description)); n > maxDescription+1 {
		t.Fatalf("description not bounded: %d runes (cap %d)", n, maxDescription)
	}
	for _, w := range warnings {
		if strings.Contains(w, "description") {
			t.Fatalf("truncation must not warn: %q", w)
		}
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

func TestDiscoverWithWarningsBadNameAndDuplicate(t *testing.T) {
	root := t.TempDir()
	// Frontmatter name doesn't match the naming convention (uppercase, spaces)
	// and doesn't equal the folder name -> warning, but the skill still loads
	// (name/description validation is advisory, not a rejection).
	writeSkill(t, root, "skills", "bad-name-skill",
		"---\nname: Bad Name!\ndescription: has a bad name\n---\n")
	// Duplicate name across dirs -> second occurrence dropped with a warning.
	writeSkill(t, root, ".memorylayer-skills", "shared", "---\nname: shared\ndescription: primary\n---\n")
	writeSkill(t, root, "skills", "shared-dup", "---\nname: shared\ndescription: secondary\n---\n")

	specs, warnings, err := DiscoverWithWarnings(root, []string{".memorylayer-skills", "skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 2 { // bad-name-skill (kept) + shared (primary; dup dropped)
		t.Fatalf("expected 2 specs, got %d: %#v", len(specs), specs)
	}
	var sawBadName, sawDup bool
	for _, w := range warnings {
		if strings.Contains(w, "bad-name-skill") && strings.Contains(w, "Bad Name!") {
			sawBadName = true
		}
		if strings.Contains(w, "shared") && strings.Contains(w, "duplicate") {
			sawDup = true
		}
	}
	if !sawBadName {
		t.Fatalf("expected a bad-name warning, got %#v", warnings)
	}
	if !sawDup {
		t.Fatalf("expected a duplicate-name warning, got %#v", warnings)
	}
}

func TestDiscoverWithWarningsAllowedTools(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "skills", "yaml-list-tools",
		"---\nname: yaml-list-tools\ndescription: yaml list form\nallowed-tools:\n  - read_file\n  - write_file\n---\n")
	writeSkill(t, root, "skills", "csv-tools",
		"---\nname: csv-tools\ndescription: csv form\nallowed-tools: read_file, bash\n---\n")

	specs, _, err := DiscoverWithWarnings(root, []string{"skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	byName := map[string][]string{}
	for _, s := range specs {
		byName[s.Name] = s.AllowedTools
	}
	if got := byName["yaml-list-tools"]; len(got) != 2 || got[0] != "read_file" || got[1] != "write_file" {
		t.Fatalf("yaml-list allowed-tools wrong: %#v", got)
	}
	if got := byName["csv-tools"]; len(got) != 2 || got[0] != "read_file" || got[1] != "bash" {
		t.Fatalf("csv allowed-tools wrong: %#v", got)
	}
}

func TestDiscoverUnchangedByWarnings(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "skills", "data-analysis",
		"# Data Analysis\nAnalyze CSVs and produce charts.")
	specs, err := Discover(root, []string{"skills"})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "data-analysis" {
		t.Fatalf("Discover should still work unchanged: %#v", specs)
	}
}

func Test_Discover_absolute_root_yields_absolute_path(t *testing.T) {
	root := t.TempDir() // an absolute system-skills root, e.g. /opt/agent-skills
	writeSkill(t, root, "", "alpha", "---\nname: alpha\ndescription: does alpha\n---\nbody")
	specs, err := Discover("/some/workspace", []string{root})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "alpha" || specs[0].Description != "does alpha" {
		t.Fatalf("unexpected specs: %+v", specs)
	}
	want := filepath.Join(root, "alpha", "SKILL.md")
	if specs[0].Path != want {
		t.Fatalf("absolute root should yield absolute Path %q, got %q", want, specs[0].Path)
	}
}
