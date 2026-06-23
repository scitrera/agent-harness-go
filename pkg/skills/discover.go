// Package skills discovers workspace skills from the filesystem: a skill is a
// folder containing a SKILL.md, mirroring OpenClaw's skills-folder model. The
// SKILL.md body is loaded lazily by the model (via read_file on the returned
// Path); discovery only surfaces name + description for the prompt.
package skills

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
)

const (
	skillFileName  = "SKILL.md"
	maxSkillBytes  = 16 << 10
	maxDescription = 240
	maxSkills      = 128
)

// Discover scans each dir (relative to workspaceRoot) for <dir>/<skill>/SKILL.md
// files and returns one enabled SkillSpec per skill (name + description + the
// workspace-relative Path to its SKILL.md). Missing dirs are skipped. Skills are
// de-duplicated by name (first occurrence wins; dirs are scanned in order).
func Discover(workspaceRoot string, dirs []string) ([]catalog.SkillSpec, error) {
	if workspaceRoot == "" {
		return nil, nil
	}
	seen := map[string]bool{}
	var out []catalog.SkillSpec
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		abs := filepath.Join(workspaceRoot, dir)
		entries, err := os.ReadDir(abs)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read skills dir %s: %w", dir, err)
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, sub := range names {
			rel := filepath.Join(dir, sub, skillFileName)
			content, err := readCapped(filepath.Join(workspaceRoot, rel))
			if err != nil {
				continue // no SKILL.md (or unreadable) -> not a skill folder
			}
			name, description := parseSkill(content, sub)
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, catalog.SkillSpec{
				Name:        name,
				Description: description,
				Path:        rel,
				Enabled:     true,
			})
			if len(out) >= maxSkills {
				return out, nil
			}
		}
	}
	return out, nil
}

func readCapped(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf := make([]byte, maxSkillBytes)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return "", err
	}
	return string(buf[:n]), nil
}

// parseSkill derives name + description from YAML-ish frontmatter (name:,
// description:) when present, else falls back to the folder name and the first
// meaningful body line.
func parseSkill(content, folderName string) (name, description string) {
	name = folderName
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
						name = v
					}
				case "description":
					if v != "" {
						description = v
					}
				}
			}
		}
	}
	if description == "" {
		for _, l := range body {
			t := strings.TrimSpace(l)
			if t == "" || t == "---" || strings.HasPrefix(t, "#") {
				continue
			}
			description = t
			break
		}
	}
	if len(description) > maxDescription {
		description = description[:maxDescription] + "…"
	}
	return name, description
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
