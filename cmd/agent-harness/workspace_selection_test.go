package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

func TestResolveAppWorkspaceSingleModePreservesLegacyDefault(t *testing.T) {
	resolution, err := resolveAppWorkspace(context.Background(), workspaceModeSingle, "", "", "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolution.WorkspaceID != "" || resolution.Source != workspacepkg.SourceDefault {
		t.Fatalf("resolution = %+v", resolution)
	}

	pinned, err := resolveAppWorkspace(context.Background(), workspaceModeSingle, " project-a ", "", "")
	if err != nil {
		t.Fatalf("resolve pinned: %v", err)
	}
	if pinned.WorkspaceID != "project-a" || pinned.Source != workspacepkg.SourcePinned {
		t.Fatalf("pinned resolution = %+v", pinned)
	}
}

func TestResolveAppWorkspaceProjectModePersistsProjectIdentity(t *testing.T) {
	indexDir := t.TempDir()
	root := filepath.Join(t.TempDir(), "Prime Agent")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("project dir: %v", err)
	}

	first, err := resolveAppWorkspace(context.Background(), workspaceModeProject, "", indexDir, root)
	if err != nil {
		t.Fatalf("resolve first: %v", err)
	}
	second, err := resolveAppWorkspace(context.Background(), workspaceModeProject, "", indexDir, root)
	if err != nil {
		t.Fatalf("resolve second: %v", err)
	}
	if first.WorkspaceID != "prime-agent" || second.WorkspaceID != first.WorkspaceID {
		t.Fatalf("resolutions = %+v and %+v", first, second)
	}
}

func TestResolveAppWorkspaceRejectsUnknownMode(t *testing.T) {
	if _, err := resolveAppWorkspace(context.Background(), "fleet", "", t.TempDir(), t.TempDir()); err == nil {
		t.Fatal("expected unknown mode error")
	}
}

func TestResolveAppWorkspacePinnedProjectDoesNotNeedIndex(t *testing.T) {
	resolution, err := resolveAppWorkspace(context.Background(), workspaceModeProject, "deployment", "", "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolution.WorkspaceID != "deployment" || resolution.Source != workspacepkg.SourcePinned {
		t.Fatalf("resolution = %+v", resolution)
	}
}

func TestEffectiveWorkspaceUsesExplicitThenResolvedThenDefault(t *testing.T) {
	if got := effectiveWorkspace("transport", "project"); got != "transport" {
		t.Fatalf("explicit = %q", got)
	}
	if got := effectiveWorkspace("", "project"); got != "project" {
		t.Fatalf("resolved = %q", got)
	}
	if got := effectiveWorkspace("", ""); got != "default" {
		t.Fatalf("default = %q", got)
	}
}
