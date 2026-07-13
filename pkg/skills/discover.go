// Package skills discovers workspace skills from the filesystem: a skill is a
// folder containing a SKILL.md, mirroring OpenClaw's skills-folder model. The
// SKILL.md body is loaded lazily by the model (via read_file on the returned
// Path); discovery only surfaces name + description for the prompt.
package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
)

const (
	skillFileName = "SKILL.md"
	maxSkillBytes = 16 << 10
	// maxDescription bounds the per-skill description shown in the "## Skills"
	// listing. A skill's `description` frontmatter is a "when to use this" blurb
	// (the relevance signal the model + the ranker read), routinely 400-600 chars,
	// so this is generous — it's a prompt-size guard, not a spec limit. Over-length
	// descriptions are silently truncated (not a load failure), measured in runes.
	maxDescription = 1024
	maxSkills      = 128
	// maxWarnings/maxWarningLen bound the injection-safe warnings collected by
	// DiscoverWithWarnings: these strings are derived from untrusted on-disk
	// skill folders, so both count and length are capped defensively.
	maxWarnings   = 20
	maxWarningLen = 200
)

// skillNameRe is the required shape of a frontmatter `name:` (and, per the
// naming convention, the folder name): lowercase alphanumerics with single
// hyphen separators.
var skillNameRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// Discover scans each dir (relative to workspaceRoot) for <dir>/<skill>/SKILL.md
// files and returns one enabled SkillSpec per skill (name + description + the
// workspace-relative Path to its SKILL.md). Missing dirs are skipped. Skills are
// de-duplicated by name (first occurrence wins; dirs are scanned in order).
func Discover(workspaceRoot string, dirs []string) ([]catalog.SkillSpec, error) {
	specs, _, err := DiscoverWithWarnings(workspaceRoot, dirs)
	return specs, err
}

// DiscoverWithWarnings does everything Discover does, and additionally collects
// human-readable warnings for skill folders that look like skills but fail
// validation: an unreadable/over-cap SKILL.md, a frontmatter name that doesn't
// match skillNameRe or doesn't equal the folder name, or a duplicate name
// (collision dropped). Warnings never change which skills are returned (name
// validation is advisory) except for the duplicate case, which already dropped
// the skill before this change. A long description is NOT a warning — it is
// silently truncated for the listing (see parseSkillMeta), not a load failure.
//
// Warnings are derived from untrusted on-disk content (a workspace file an
// attacker controls could shape them), so they are kept factual and bounded
// (maxWarnings count, maxWarningLen length each); the sysprompt renderer is
// responsible for HTML-escaping them before they reach the model.
func DiscoverWithWarnings(workspaceRoot string, dirs []string) ([]catalog.SkillSpec, []string, error) {
	if workspaceRoot == "" {
		return nil, nil, nil
	}
	seen := map[string]bool{}
	var out []catalog.SkillSpec
	var warnings []string
	warn := func(format string, args ...interface{}) {
		if len(warnings) >= maxWarnings {
			return
		}
		w := fmt.Sprintf(format, args...)
		if len(w) > maxWarningLen {
			w = w[:maxWarningLen] + "…"
		}
		warnings = append(warnings, w)
	}
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		// A dir may be workspace-relative (joined under workspaceRoot) or an
		// ABSOLUTE root (e.g. image-baked system skills at /opt/agent-skills),
		// scanned in place. Absolute roots yield absolute skill Paths the model
		// reads via a localtools read-only root.
		absolute := filepath.IsAbs(dir)
		abs := dir
		if !absolute {
			abs = filepath.Join(workspaceRoot, dir)
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, warnings, fmt.Errorf("read skills dir %s: %w", dir, err)
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, sub := range names {
			full := filepath.Join(abs, sub, skillFileName)
			content, overCap, err := readCapped(full)
			if err != nil {
				if !os.IsNotExist(err) {
					warn("skill %q: SKILL.md unreadable: %v", sub, err)
				}
				continue // no SKILL.md (or unreadable) -> not a skill folder
			}
			if overCap {
				warn("skill %q: SKILL.md exceeds %dKB cap; content was truncated", sub, maxSkillBytes>>10)
			}
			meta := parseSkillMeta(content, sub)
			if meta.rawName != "" && (!skillNameRe.MatchString(meta.rawName) || meta.rawName != sub) {
				warn("skill %q: frontmatter name %q must be lowercase-hyphenated and match the folder name", sub, meta.rawName)
			}
			if meta.name == "" {
				continue
			}
			if seen[meta.name] {
				warn("skill %q: duplicate name (dir %s); dropped", meta.name, dir)
				continue
			}
			seen[meta.name] = true
			// Path is what the model read_files: an absolute root yields the absolute
			// SKILL.md path; a workspace-relative root yields the workspace-relative one.
			specPath := full
			if !absolute {
				specPath = filepath.Join(dir, sub, skillFileName)
			}
			out = append(out, catalog.SkillSpec{
				Name:         meta.name,
				Description:  meta.description,
				Path:         specPath,
				Enabled:      true,
				AllowedTools: meta.allowedTools,
			})
			if len(out) >= maxSkills {
				return out, warnings, nil
			}
		}
	}
	return out, warnings, nil
}

// readCapped reads up to maxSkillBytes of path, reporting whether the file was
// larger than the cap (content truncated) alongside any read error.
func readCapped(path string) (content string, overCap bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	if info, statErr := f.Stat(); statErr == nil && info.Size() > maxSkillBytes {
		overCap = true
	}
	buf := make([]byte, maxSkillBytes)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return "", overCap, err
	}
	return string(buf[:n]), overCap, nil
}

// skillMeta is the result of parsing a SKILL.md's frontmatter + body.
type skillMeta struct {
	name         string // resolved name (frontmatter name: else folder name)
	rawName      string // frontmatter `name:` as written, empty if absent
	description  string
	allowedTools []string
}

// skillMetaFrontmatter is the SKILL.md frontmatter subset discovery surfaces:
// name, description, and the optional allowed-tools scope. Parsed via yaml.v3 so
// folded/literal block scalars (`description: >-` with the text on the following
// indented lines) and quoted values resolve to their real content instead of
// leaking the YAML block indicator.
type skillMetaFrontmatter struct {
	Name         string           `yaml:"name"`
	Description  string           `yaml:"description"`
	AllowedTools allowedToolsList `yaml:"allowed-tools"`
}

// parseSkillMeta derives name + description from YAML frontmatter (name:,
// description:) when present, else falls back to the folder name and the first
// meaningful body line. It also parses the optional `allowed-tools` frontmatter
// key (YAML list or comma-separated string). All three come from a single yaml.v3
// parse so block scalars are handled correctly.
func parseSkillMeta(content, folderName string) skillMeta {
	m := skillMeta{name: folderName}
	var fm skillMetaFrontmatter
	if block, ok := frontmatterBlock(content); ok {
		_ = yaml.Unmarshal([]byte(block), &fm) // best-effort: malformed -> fall back
	}
	if name := strings.TrimSpace(fm.Name); name != "" {
		m.name = name
		m.rawName = name
	}
	// Collapse the whitespace a folded/literal scalar preserves so a multi-line
	// description renders on a single prompt line.
	m.description = strings.Join(strings.Fields(fm.Description), " ")
	m.allowedTools = []string(fm.AllowedTools)

	if m.description == "" {
		for _, l := range bodyLines(content) {
			t := strings.TrimSpace(l)
			if t == "" || t == "---" || strings.HasPrefix(t, "#") {
				continue
			}
			m.description = t
			break
		}
	}
	// Silent, rune-safe truncation to bound the prompt listing (not a load failure).
	if runes := []rune(m.description); len(runes) > maxDescription {
		m.description = string(runes[:maxDescription]) + "…"
	}
	return m
}

// bodyLines returns the SKILL.md lines after the leading `---` frontmatter block
// (or all lines when there is no frontmatter).
func bodyLines(content string) []string {
	lines := strings.Split(content, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return lines
	}
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			return lines[i+1:]
		}
	}
	return nil
}

// allowedToolsList unmarshals `allowed-tools` from either a YAML sequence
// (`- read_file`) or a scalar comma-separated string (`read_file, write_file`).
type allowedToolsList []string

func (a *allowedToolsList) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.SequenceNode:
		var items []string
		if err := value.Decode(&items); err != nil {
			return err
		}
		*a = cleanToolNames(items)
	case yaml.ScalarNode:
		var s string
		if err := value.Decode(&s); err != nil {
			return err
		}
		*a = cleanToolNames(strings.Split(s, ","))
	}
	return nil
}

func cleanToolNames(items []string) []string {
	var out []string
	for _, it := range items {
		if it = strings.TrimSpace(it); it != "" {
			out = append(out, it)
		}
	}
	return out
}
