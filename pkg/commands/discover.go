package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	commandExt      = ".md"
	maxCommandBytes = 32 << 10
	maxDescription  = 240
	maxCommands     = 256
)

// ReservedNames are built-in command names handled by the runner; workspace
// files using these names are ignored so built-ins cannot be shadowed. The value
// is the help-line description.
var ReservedNames = map[string]string{
	"help":     "List available commands.",
	"commands": "List available commands.",
	"clear":    "Clear this thread's history.",
}

// Discover scans each dir (relative to workspaceRoot) for "<name>.md" command
// files plus one level of namespace dirs ("<ns>/<name>.md" -> "<ns>:<name>").
// Missing dirs are skipped. Commands are de-duplicated by canonical name (first
// occurrence wins; dirs are scanned in order). Reserved names are skipped.
func Discover(workspaceRoot string, dirs []string) ([]Command, error) {
	if workspaceRoot == "" {
		return nil, nil
	}
	seen := map[string]bool{}
	var out []Command
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		found, err := scanDir(workspaceRoot, dir, seen)
		if err != nil {
			return nil, err
		}
		out = append(out, found...)
		if len(out) >= maxCommands {
			return out[:maxCommands], nil
		}
	}
	return out, nil
}

func scanDir(root, relDir string, seen map[string]bool) ([]Command, error) {
	absDir := filepath.Join(root, relDir)
	entries, err := os.ReadDir(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read commands dir %s: %w", relDir, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var out []Command
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			// One level of namespacing: "<dir>/<ns>/<cmd>.md" -> "<ns>:<cmd>".
			subRel := filepath.Join(relDir, name)
			subEntries, err := os.ReadDir(filepath.Join(absDir, name))
			if err != nil {
				continue
			}
			sort.Slice(subEntries, func(i, j int) bool { return subEntries[i].Name() < subEntries[j].Name() })
			for _, se := range subEntries {
				if se.IsDir() || !strings.HasSuffix(se.Name(), commandExt) {
					continue
				}
				base := strings.TrimSuffix(se.Name(), commandExt)
				if cmd, ok := loadCommand(root, filepath.Join(subRel, se.Name()), name+":"+base, seen); ok {
					out = append(out, cmd)
				}
			}
			continue
		}
		if !strings.HasSuffix(name, commandExt) {
			continue
		}
		base := strings.TrimSuffix(name, commandExt)
		if cmd, ok := loadCommand(root, filepath.Join(relDir, name), base, seen); ok {
			out = append(out, cmd)
		}
	}
	return out, nil
}

func loadCommand(root, rel, rawName string, seen map[string]bool) (Command, bool) {
	name := strings.ToLower(strings.TrimSpace(rawName))
	if name == "" {
		return Command{}, false
	}
	key := CanonicalKey(name)
	if _, reserved := ReservedNames[key]; reserved {
		return Command{}, false
	}
	if seen[key] {
		return Command{}, false
	}
	content, err := readCapped(filepath.Join(root, rel))
	if err != nil {
		return Command{}, false
	}
	cmd := parseCommand(content)
	cmd.Name = name
	cmd.Path = rel
	if cmd.Description == "" {
		cmd.Description = "(no description)"
	}
	seen[key] = true
	return cmd, true
}

func readCapped(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf := make([]byte, maxCommandBytes)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return "", err
	}
	return string(buf[:n]), nil
}

// parseCommand extracts frontmatter (description, argument-hint, model,
// allowed-tools) and returns the body with frontmatter stripped.
func parseCommand(content string) Command {
	var cmd Command
	lines := strings.Split(content, "\n")
	body := lines
	if len(lines) > 0 && strings.TrimSpace(lines[0]) == "---" {
		for i := 1; i < len(lines); i++ {
			if strings.TrimSpace(lines[i]) == "---" {
				body = lines[i+1:]
				break
			}
			k, v, ok := splitKV(lines[i])
			if !ok {
				continue
			}
			switch strings.ToLower(k) {
			case "description":
				cmd.Description = v
			case "argument-hint", "argument_hint", "args":
				cmd.ArgumentHint = v
			case "model":
				cmd.Model = v
			case "allowed-tools", "allowed_tools":
				cmd.AllowedTools = splitList(v)
			}
		}
	}
	cmd.Body = strings.TrimSpace(strings.Join(body, "\n"))
	if len(cmd.Description) > maxDescription {
		cmd.Description = cmd.Description[:maxDescription] + "…"
	}
	return cmd
}

func splitKV(line string) (key, value string, ok bool) {
	idx := strings.Index(line, ":")
	if idx <= 0 {
		return "", "", false
	}
	key = strings.TrimSpace(line[:idx])
	value = strings.Trim(strings.TrimSpace(line[idx+1:]), `"'`)
	return key, value, key != ""
}

func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
