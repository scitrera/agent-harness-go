package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type detectorFunc func(context.Context, string) (Project, error)

func (f detectorFunc) Detect(ctx context.Context, cwd string) (Project, error) {
	return f(ctx, cwd)
}

func TestResolverPrecedence(t *testing.T) {
	t.Run("explicit beats pinned and discovery", func(t *testing.T) {
		calls := 0
		resolver, err := NewResolver(Config{
			PinnedWorkspace: "deployment",
			Detector: detectorFunc(func(context.Context, string) (Project, error) {
				calls++
				return Project{}, errors.New("should not be called")
			}),
		})
		if err != nil {
			t.Fatalf("new resolver: %v", err)
		}

		got, err := resolver.Resolve(context.Background(), Request{WorkspaceID: " requested ", CWD: "/ignored"})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got.WorkspaceID != "requested" || got.Source != SourceExplicit || calls != 0 {
			t.Fatalf("resolution = %+v, detector calls = %d", got, calls)
		}
	})

	t.Run("pinned beats discovery", func(t *testing.T) {
		calls := 0
		resolver, err := NewResolver(Config{
			PinnedWorkspace: " deployment ",
			Detector: detectorFunc(func(context.Context, string) (Project, error) {
				calls++
				return Project{}, errors.New("should not be called")
			}),
		})
		if err != nil {
			t.Fatalf("new resolver: %v", err)
		}

		got, err := resolver.Resolve(context.Background(), Request{CWD: "/ignored"})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got.WorkspaceID != "deployment" || got.Source != SourcePinned || calls != 0 {
			t.Fatalf("resolution = %+v, detector calls = %d", got, calls)
		}
	})

	t.Run("default is only used without a project", func(t *testing.T) {
		resolver, err := NewResolver(Config{DefaultWorkspace: " fallback "})
		if err != nil {
			t.Fatalf("new resolver: %v", err)
		}

		got, err := resolver.Resolve(context.Background(), Request{})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got.WorkspaceID != "fallback" || got.Source != SourceDefault {
			t.Fatalf("resolution = %+v", got)
		}
	})
}

func TestResolverErrorsWhenWorkspaceCannotBeStable(t *testing.T) {
	resolver, err := NewResolver(Config{})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	if _, err := resolver.Resolve(context.Background(), Request{CWD: t.TempDir()}); !errors.Is(err, ErrStateDirRequired) {
		t.Fatalf("discovery error = %v, want ErrStateDirRequired", err)
	}
	if _, err := resolver.Resolve(context.Background(), Request{}); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("empty error = %v, want ErrWorkspaceUnavailable", err)
	}
}

func TestResolverPersistsProjectAssignmentsAndSeparatesCollisions(t *testing.T) {
	stateDir := t.TempDir()
	firstRoot := filepath.Join(t.TempDir(), "service")
	secondRoot := filepath.Join(t.TempDir(), "service")
	for _, root := range []string{firstRoot, secondRoot} {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", root, err)
		}
	}

	detector := detectorFunc(func(_ context.Context, cwd string) (Project, error) {
		return Project{Root: cwd, Git: true}, nil
	})
	resolver, err := NewResolver(Config{StateDir: stateDir, Detector: detector})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	first, err := resolver.Resolve(context.Background(), Request{CWD: firstRoot})
	if err != nil {
		t.Fatalf("resolve first: %v", err)
	}
	second, err := resolver.Resolve(context.Background(), Request{CWD: secondRoot})
	if err != nil {
		t.Fatalf("resolve second: %v", err)
	}

	if first.WorkspaceID != "service" {
		t.Fatalf("first workspace ID = %q", first.WorkspaceID)
	}
	if second.WorkspaceID == first.WorkspaceID || !strings.HasPrefix(second.WorkspaceID, "service-") {
		t.Fatalf("colliding workspace IDs = %q and %q", first.WorkspaceID, second.WorkspaceID)
	}
	if first.Source != SourceGitRoot || second.Source != SourceGitRoot || !first.Git || !second.Git {
		t.Fatalf("Git resolutions = %+v and %+v", first, second)
	}

	restarted, err := NewResolver(Config{StateDir: stateDir, Detector: detector})
	if err != nil {
		t.Fatalf("restart resolver: %v", err)
	}
	secondAgain, err := restarted.Resolve(context.Background(), Request{CWD: secondRoot})
	if err != nil {
		t.Fatalf("resolve second after restart: %v", err)
	}
	firstAgain, err := restarted.Resolve(context.Background(), Request{CWD: firstRoot})
	if err != nil {
		t.Fatalf("resolve first after restart: %v", err)
	}
	if firstAgain.WorkspaceID != first.WorkspaceID || secondAgain.WorkspaceID != second.WorkspaceID {
		t.Fatalf("assignments changed after restart: before=(%q,%q) after=(%q,%q)", first.WorkspaceID, second.WorkspaceID, firstAgain.WorkspaceID, secondAgain.WorkspaceID)
	}

	entries := restarted.List()
	if len(entries) != 2 || entries[0].WorkspaceID > entries[1].WorkspaceID {
		t.Fatalf("sorted entries = %+v", entries)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "workspaces", "index.json")); err != nil {
		t.Fatalf("workspace index: %v", err)
	}
}

func TestResolverUsesDirectorySource(t *testing.T) {
	root := t.TempDir()
	resolver, err := NewResolver(Config{
		StateDir: t.TempDir(),
		Detector: detectorFunc(func(context.Context, string) (Project, error) {
			return Project{Root: root}, nil
		}),
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	got, err := resolver.Resolve(context.Background(), Request{CWD: root})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Source != SourceDirectory || got.Git {
		t.Fatalf("resolution = %+v", got)
	}
	entries := resolver.List()
	if len(entries) != 1 || entries[0].Kind != ProjectKindDirectory {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestResolverRefreshesProjectKindWithoutChangingIdentity(t *testing.T) {
	root := t.TempDir()
	git := false
	resolver, err := NewResolver(Config{
		StateDir: t.TempDir(),
		Detector: detectorFunc(func(context.Context, string) (Project, error) {
			return Project{Root: root, Git: git}, nil
		}),
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}

	before, err := resolver.Resolve(context.Background(), Request{CWD: root})
	if err != nil {
		t.Fatalf("resolve directory: %v", err)
	}
	git = true
	after, err := resolver.Resolve(context.Background(), Request{CWD: root})
	if err != nil {
		t.Fatalf("resolve Git project: %v", err)
	}

	if before.WorkspaceID != after.WorkspaceID || after.Source != SourceGitRoot {
		t.Fatalf("before = %+v, after = %+v", before, after)
	}
	entries := resolver.List()
	if len(entries) != 1 || entries[0].Kind != ProjectKindGit {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestResolverRejectsCorruptIndex(t *testing.T) {
	stateDir := t.TempDir()
	indexPath := filepath.Join(stateDir, "workspaces", "index.json")
	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		t.Fatalf("mkdir index: %v", err)
	}
	if err := os.WriteFile(indexPath, []byte(`{"version":99,"entries":[]}`), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	_, err := NewResolver(Config{StateDir: stateDir})
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("new resolver error = %v, want ErrInvalidIndex", err)
	}
}

func TestFilesystemDetectorCanonicalizesSymlink(t *testing.T) {
	realRoot := t.TempDir()
	symlink := filepath.Join(t.TempDir(), "project-link")
	if err := os.Symlink(realRoot, symlink); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	project, err := (filesystemDetector{}).Detect(context.Background(), symlink)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if project.Root != realRoot {
		t.Fatalf("project root = %q, want %q", project.Root, realRoot)
	}
}

func TestProjectSlug(t *testing.T) {
	tests := map[string]string{
		"Prime Agent":  "prime-agent",
		".hidden_repo": "hidden-repo",
		"世界":           "project",
		"___":          "project",
	}
	for input, want := range tests {
		if got := projectSlug(input); got != want {
			t.Errorf("projectSlug(%q) = %q, want %q", input, got, want)
		}
	}
}
