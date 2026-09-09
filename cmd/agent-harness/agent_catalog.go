// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const referenceSubagentMaxDepth = 2

var defaultAgentCatalogDirs = []string{"agents", ".agent-harness-agents"}

func agentCatalogForWorkspace(workspaceRoot string) (subagent.Catalog, error) {
	if strings.TrimSpace(workspaceRoot) == "" {
		return nil, nil
	}
	for _, rel := range defaultAgentCatalogDirs {
		rel = strings.TrimSpace(rel)
		if rel == "" {
			continue
		}
		dir := filepath.Join(workspaceRoot, rel)
		info, err := os.Stat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("stat agent catalog %s: %w", rel, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("agent catalog %s is not a directory", rel)
		}
		return subagent.NewFileCatalog(dir), nil
	}
	return nil, nil
}

func registerReferenceSubagent(reg *tools.Registry, runner subagent.Runner, catalog subagent.Catalog, allowBackground bool) error {
	return tools.RegisterSubagentWithConfig(reg, tools.SubagentConfig{
		Runner:          runner,
		MaxDepth:        referenceSubagentMaxDepth,
		Catalog:         catalog,
		AllowBackground: allowBackground,
	})
}
