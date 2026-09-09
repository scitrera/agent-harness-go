// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package workspace

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestPathSegmentKeepsOpaqueIDsDistinctAndBounded(t *testing.T) {
	first := PathSegment("project/a")
	second := PathSegment("project\\a")
	if first == second || strings.ContainsAny(first+second, `/\\`) {
		t.Fatalf("unsafe or colliding segments: %q and %q", first, second)
	}
	if got := len(PathSegment(strings.Repeat("x", 1000))); got > 160 {
		t.Fatalf("large ID segment length = %d", got)
	}
}

func TestStateDirPreservesLegacyAndScopesWorkspace(t *testing.T) {
	base := filepath.Join("state", "root")
	if got := StateDir(base, ""); got != base {
		t.Fatalf("legacy state dir = %q", got)
	}
	if got := StateDir(base, "project-a"); got == base || filepath.Dir(got) != filepath.Join(base, "workspaces") {
		t.Fatalf("scoped state dir = %q", got)
	}
}
