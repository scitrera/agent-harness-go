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
	skillFileName  = "SKILL.md"
	maxSkillBytes  = 16 << 10
	maxDescription = 240
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
// match skillNameRe or doesn't equal the folder name, a duplicate name
// (collision dropped), or an over-cap description. Warnings never change which
// skills are returned (name/description validation is advisory) except for the
// duplicate case, which already dropped the skill before this change.
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
			if meta.descriptionOverCap {
				warn("skill %q: description exceeds %d chars; truncated", sub, maxDescription)
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
	name               string // resolved name (frontmatter name: else folder name)
	rawName            string // frontmatter `name:` as written, empty if absent
	description        string
	descriptionOverCap bool
	allowedTools       []string
}

// parseSkillMeta derives name + description from YAML-ish frontmatter (name:,
// description:) when present, else falls back to the folder name and the first
// meaningful body line. It also parses the optional `allowed-tools` frontmatter
// key (YAML list or comma-separated string) via yaml.v3.
func parseSkillMeta(content, folderName string) skillMeta {
	m := skillMeta{name: folderName}
	lines := strings.Split(content, "\n")
	body := lines
	if len(lines) > 0 && strings.TrimSpace(lines[0]) == "---" {
		for i := 1; i < len(lines); i++ {
			if strings.TrimSpace(lines[i]) == "---" {
				body = lines[i+1:]
				break
			}
			if k, v, ok := splitKV(lines[i]); ok {
				switch strings.ToLower(k) {
				case "name":
					if v != "" {
						m.name = v
						m.rawName = v
					}
				case "description":
					if v != "" {
						m.description = v
					}
				}
			}
		}
	}
	if m.description == "" {
		for _, l := range body {
			t := strings.TrimSpace(l)
			if t == "" || t == "---" || strings.HasPrefix(t, "#") {
				continue
			}
			m.description = t
			break
		}
	}
	if len(m.description) > maxDescription {
		m.description = m.description[:maxDescription] + "…"
		m.descriptionOverCap = true
	}
	m.allowedTools = parseAllowedTools(content)
	return m
}

// skillToolsFrontmatter is the subset of SKILL.md YAML frontmatter needed to
// parse the optional `allowed-tools` key. Surfaced to the model only (see
// LoadableSkill.AllowedTools) — enforcement is a turn-loop follow-up, not
// implemented here.
type skillToolsFrontmatter struct {
	AllowedTools allowedToolsList `yaml:"allowed-tools"`
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

// parseAllowedTools extracts allowed-tools from a SKILL.md's leading `---` YAML
// frontmatter block (reusing frontmatterBlock from loadtool.go). Best-effort:
// malformed YAML yields no allowed-tools rather than an error.
func parseAllowedTools(content string) []string {
	block, ok := frontmatterBlock(content)
	if !ok {
		return nil
	}
	var fm skillToolsFrontmatter
	if err := yaml.Unmarshal([]byte(block), &fm); err != nil {
		return nil
	}
	return []string(fm.AllowedTools)
}

func splitKV(line string) (key, value string, ok bool) {
	idx := strings.Index(line, ":")
	if idx <= 0 {
		return "", "", false
	}
	key = strings.TrimSpace(line[:idx])
	value = strings.TrimSpace(line[idx+1:])
	value = strings.Trim(value, `"'`)
	return key, value, key != ""
}
