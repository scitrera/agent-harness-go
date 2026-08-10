package workspace

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

type recordingViewPublisher struct {
	views        []View
	observations []ViewObservation
}

func (p *recordingViewPublisher) PublishWorkspaceView(_ context.Context, view View, observation ViewObservation) error {
	p.views = append(p.views, view)
	p.observations = append(p.observations, observation)
	return nil
}

func TestViewRegistryBindsRelativeDirectoryWithoutPublishingAbsolutePath(t *testing.T) {
	root := t.TempDir()
	subdir := filepath.Join(root, "src", "pkg")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	publisher := &recordingViewPublisher{}
	registry, err := NewViewRegistry(context.Background(), ViewRegistryConfig{
		InitialWorkspaceID: "project-a",
		InitialRoot:        root,
		StateDir:           t.TempDir(),
		ToolHostID:         "us::drew::window-1",
		ExecutionSite:      spec.ExecutionSiteClient,
		Publisher:          publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := registry.ExecutionBindingForDirectory(context.Background(), subdir)
	if err != nil {
		t.Fatal(err)
	}
	if binding.WorkspaceID != "project-a" || binding.RelativeDirectory != "src/pkg" {
		t.Fatalf("binding = %+v", binding)
	}
	if binding.ToolHostID != "us::drew::window-1" || binding.RootRef == "" {
		t.Fatalf("binding host/root = %+v", binding)
	}
	if len(publisher.views) != 2 {
		t.Fatalf("published %d views, want initial registration plus current observation", len(publisher.views))
	}
	encoded, err := json.Marshal(publisher.views[len(publisher.views)-1].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), root) {
		t.Fatalf("durable descriptor leaked absolute root %q", root)
	}
	resolved, err := registry.ResolveBinding(binding)
	if err != nil || resolved.Root != root {
		t.Fatalf("resolve binding = %+v, %v", resolved, err)
	}

	wrongHost := binding
	wrongHost.ToolHostID = "us::drew::window-2"
	if _, err := registry.ResolveBinding(wrongHost); err == nil {
		t.Fatal("binding for another host was accepted")
	}
}

func TestViewRegistryAssignsAnotherProjectItsOwnWorkspace(t *testing.T) {
	initial := t.TempDir()
	external := t.TempDir()
	registry, err := NewViewRegistry(context.Background(), ViewRegistryConfig{
		InitialWorkspaceID: "project-a",
		InitialRoot:        initial,
		StateDir:           t.TempDir(),
		ToolHostID:         "us::drew::window-1",
		ExecutionSite:      spec.ExecutionSiteClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := registry.ExecutionBindingForDirectory(context.Background(), external)
	if err != nil {
		t.Fatal(err)
	}
	if binding.WorkspaceID == "" || binding.WorkspaceID == "project-a" {
		t.Fatalf("external project workspace = %q", binding.WorkspaceID)
	}
	if binding.RelativeDirectory != "" {
		t.Fatalf("external project relative directory = %q", binding.RelativeDirectory)
	}
}

func TestGitWorktreesShareWorkspaceButKeepDistinctViews(t *testing.T) {
	repository := t.TempDir()
	runGit(t, repository, "init")
	runGit(t, repository, "config", "user.email", "tests@example.invalid")
	runGit(t, repository, "config", "user.name", "Workspace Tests")
	if err := os.WriteFile(filepath.Join(repository, "tracked.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked.txt")
	runGit(t, repository, "commit", "-m", "initial")
	worktreeParent := t.TempDir()
	worktree := filepath.Join(worktreeParent, "feature")
	runGit(t, repository, "worktree", "add", "-b", "feature", worktree)

	registry, err := NewViewRegistry(context.Background(), ViewRegistryConfig{
		InitialWorkspaceID: "project-a", InitialRoot: repository, StateDir: t.TempDir(),
		ToolHostID: "us::drew::window-1", ExecutionSite: spec.ExecutionSiteClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	mainBinding, err := registry.ExecutionBindingForDirectory(context.Background(), repository)
	if err != nil {
		t.Fatal(err)
	}
	worktreeBinding, err := registry.ExecutionBindingForDirectory(context.Background(), worktree)
	if err != nil {
		t.Fatal(err)
	}
	if worktreeBinding.WorkspaceID != mainBinding.WorkspaceID {
		t.Fatalf("worktree workspace = %q, main = %q", worktreeBinding.WorkspaceID, mainBinding.WorkspaceID)
	}
	if worktreeBinding.ViewID == mainBinding.ViewID {
		t.Fatalf("worktrees reused view id %q", worktreeBinding.ViewID)
	}
}

func TestResolveCurrentBindingRejectsChangedGitRevision(t *testing.T) {
	repository := t.TempDir()
	runGit(t, repository, "init")
	runGit(t, repository, "config", "user.email", "tests@example.invalid")
	runGit(t, repository, "config", "user.name", "Workspace Tests")
	tracked := filepath.Join(repository, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked.txt")
	runGit(t, repository, "commit", "-m", "initial")
	registry, err := NewViewRegistry(context.Background(), ViewRegistryConfig{
		InitialWorkspaceID: "project-a", InitialRoot: repository, StateDir: t.TempDir(),
		ToolHostID: "us::drew::window-1", ExecutionSite: spec.ExecutionSiteClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := registry.ExecutionBindingForDirectory(context.Background(), repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tracked, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked.txt")
	runGit(t, repository, "commit", "-m", "next")
	if _, err := registry.ResolveCurrentBinding(context.Background(), binding); err == nil ||
		!strings.Contains(err.Error(), "revision is no longer current") {
		t.Fatalf("stale binding error = %v", err)
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	commandArgs := append([]string{"-C", dir}, args...)
	if output, err := exec.Command("git", commandArgs...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
