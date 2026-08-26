package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
)

func seedDefaults(root string) {
	for _, f := range sysprompt.DefaultWorkspaceFiles() {
		path := filepath.Join(root, f.Name)
		if _, err := os.Stat(path); err == nil {
			continue // no-clobber
		}
		_ = os.WriteFile(path, []byte(f.Content), 0o644)
	}
}

func skillSummaries(specs []catalog.SkillSpec) []sysprompt.SkillSummary {
	out := make([]sysprompt.SkillSummary, 0, len(specs))
	for _, s := range specs {
		if !s.Enabled || (s.Path == "" && s.Content == "") {
			continue
		}
		out = append(out, sysprompt.SkillSummary{Name: s.Name, Description: s.Description, Path: s.Path})
	}
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return def
	}
	return parsed
}
