// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package threadindex

import (
	"context"
	"testing"
)

func TestWorkspaceIndexIsolatesSameThreadID(t *testing.T) {
	stateDir := t.TempDir()
	index, err := NewWorkspaceIndex(stateDir, "project-a", fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	if err := index.TouchWorkspaceThread("project-a", "shared", "question for A"); err != nil {
		t.Fatal(err)
	}
	if err := index.TouchWorkspaceThread("project-b", "shared", "question for B"); err != nil {
		t.Fatal(err)
	}

	projectA := index.ListWorkspace("project-a")
	projectB := index.ListWorkspace("project-b")
	if len(projectA) != 1 || projectA[0].Title != "question for A" {
		t.Fatalf("project A threads = %+v", projectA)
	}
	if len(projectB) != 1 || projectB[0].Title != "question for B" {
		t.Fatalf("project B threads = %+v", projectB)
	}
	if got := index.List(); len(got) != 1 || got[0].Title != "question for A" {
		t.Fatalf("default workspace threads = %+v", got)
	}

	reopened, err := NewWorkspaceIndex(stateDir, "project-a", fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.RefreshWorkspace(context.Background(), "project-b"); err != nil {
		t.Fatal(err)
	}
	if got := reopened.ListWorkspace("project-b"); len(got) != 1 || got[0].Title != "question for B" {
		t.Fatalf("reopened project B threads = %+v", got)
	}
}
