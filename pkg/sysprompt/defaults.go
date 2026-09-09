// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sysprompt

import (
	"embed"
	"io/fs"
	"sort"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
)

// defaultsFS holds the canonical default workspace bootstrap files the harness
// ships (AGENTS.md, SOUL.md, IDENTITY.md, USER.md, TOOLS.md). The harness owns
// these as the OpenClaw image is retired; they define the default Scitrera
// persona/operating conventions that layer over DefaultBase.
//
//go:embed defaults/*.md
var defaultsFS embed.FS

// DefaultWorkspaceFiles returns the embedded default bootstrap files, sorted by
// name. Callers seed these into a fresh workspace (no-clobber) so a new sandbox
// has a real persona rather than an empty workspace.
func DefaultWorkspaceFiles() []bootstrap.File {
	entries, err := fs.ReadDir(defaultsFS, "defaults")
	if err != nil {
		return nil
	}
	files := make([]bootstrap.File, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := defaultsFS.ReadFile("defaults/" + e.Name())
		if err != nil {
			continue
		}
		files = append(files, bootstrap.File{Name: e.Name(), Content: string(data)})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return files
}
