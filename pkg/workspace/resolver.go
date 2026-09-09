// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package workspace resolves local programming projects to stable OSS
// workspace identities. Explicit and deployment-pinned identities take
// precedence; automatic discovery uses a canonical Git root or directory and
// persists the assignment under the harness state directory.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var (
	// ErrWorkspaceUnavailable indicates that no explicit, pinned, discovered,
	// or default workspace can be selected.
	ErrWorkspaceUnavailable = errors.New("workspace unavailable")
	// ErrStateDirRequired indicates that automatic project discovery would be
	// unstable because no location was supplied for the durable path index.
	ErrStateDirRequired = errors.New("state directory required for automatic workspace discovery")
)

// Source records how a workspace was selected.
type Source string

const (
	SourceExplicit  Source = "explicit"
	SourcePinned    Source = "pinned"
	SourceGitRoot   Source = "git_root"
	SourceDirectory Source = "directory"
	SourceDefault   Source = "default"
)

// Project is the canonical local root discovered for a programming project.
type Project struct {
	Root string
	Git  bool
}

// ProjectDetector finds the canonical project containing cwd. The default
// detector prefers a Git worktree root and falls back to the canonical cwd.
type ProjectDetector interface {
	Detect(ctx context.Context, cwd string) (Project, error)
}

// Config controls workspace precedence and local project indexing.
type Config struct {
	StateDir         string
	PinnedWorkspace  string
	DefaultWorkspace string
	Detector         ProjectDetector
}

// Request contains per-attachment workspace inputs. WorkspaceID is an explicit
// caller selection. CWD enables automatic programming-project discovery.
type Request struct {
	WorkspaceID string
	CWD         string
}

// Resolution is the selected workspace and its local resolution provenance.
// ProjectRoot is only populated for automatically discovered workspaces.
type Resolution struct {
	WorkspaceID string
	ProjectRoot string
	Source      Source
	Git         bool
}

// Resolver applies deterministic precedence and owns a durable local project
// index when StateDir is configured.
type Resolver struct {
	pinnedWorkspace  string
	defaultWorkspace string
	detector         ProjectDetector
	index            *projectIndex
}

// NewResolver constructs a resolver and validates any existing project index.
func NewResolver(config Config) (*Resolver, error) {
	detector := config.Detector
	if detector == nil {
		detector = filesystemDetector{}
	}
	r := &Resolver{
		pinnedWorkspace:  strings.TrimSpace(config.PinnedWorkspace),
		defaultWorkspace: strings.TrimSpace(config.DefaultWorkspace),
		detector:         detector,
	}
	if strings.TrimSpace(config.StateDir) != "" {
		index, err := newProjectIndex(config.StateDir)
		if err != nil {
			return nil, err
		}
		r.index = index
	}
	return r, nil
}

// Resolve selects a workspace in this order: explicit request, pinned process
// configuration, persisted/discovered project, then configured default.
func (r *Resolver) Resolve(ctx context.Context, request Request) (Resolution, error) {
	if workspaceID := strings.TrimSpace(request.WorkspaceID); workspaceID != "" {
		return Resolution{WorkspaceID: workspaceID, Source: SourceExplicit}, nil
	}
	if r.pinnedWorkspace != "" {
		return Resolution{WorkspaceID: r.pinnedWorkspace, Source: SourcePinned}, nil
	}

	cwd := strings.TrimSpace(request.CWD)
	if cwd != "" {
		if r.index == nil {
			return Resolution{}, ErrStateDirRequired
		}
		project, err := r.detector.Detect(ctx, cwd)
		if err != nil {
			return Resolution{}, fmt.Errorf("detect workspace project: %w", err)
		}
		root, err := canonicalDirectory(project.Root)
		if err != nil {
			return Resolution{}, fmt.Errorf("canonicalize workspace project: %w", err)
		}
		kind := ProjectKindDirectory
		source := SourceDirectory
		if project.Git {
			kind = ProjectKindGit
			source = SourceGitRoot
		}
		entry, err := r.index.assign(root, kind)
		if err != nil {
			return Resolution{}, err
		}
		return Resolution{
			WorkspaceID: entry.WorkspaceID,
			ProjectRoot: entry.ProjectRoot,
			Source:      source,
			Git:         project.Git,
		}, nil
	}

	if r.defaultWorkspace != "" {
		return Resolution{WorkspaceID: r.defaultWorkspace, Source: SourceDefault}, nil
	}
	return Resolution{}, ErrWorkspaceUnavailable
}

// List returns a workspace-ID-sorted snapshot of automatically indexed local
// projects. Explicit, pinned, and fallback IDs are not index entries.
func (r *Resolver) List() []Entry {
	if r.index == nil {
		return nil
	}
	return r.index.list()
}

type filesystemDetector struct{}

func (filesystemDetector) Detect(ctx context.Context, cwd string) (Project, error) {
	root, err := canonicalDirectory(cwd)
	if err != nil {
		return Project{}, err
	}

	command := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--show-toplevel")
	output, commandErr := command.Output()
	if commandErr == nil {
		gitRoot := strings.TrimSpace(string(output))
		if gitRoot != "" {
			canonicalRoot, err := canonicalDirectory(gitRoot)
			if err != nil {
				return Project{}, fmt.Errorf("canonicalize Git root: %w", err)
			}
			return Project{Root: canonicalRoot, Git: true}, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return Project{}, err
	}
	return Project{Root: root}, nil
}

func canonicalDirectory(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("absolute path: %w", err)
	}
	realPath, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks for %q: %w", absolute, err)
	}
	info, err := os.Stat(realPath)
	if err != nil {
		return "", fmt.Errorf("stat %q: %w", realPath, err)
	}
	if !info.IsDir() {
		realPath = filepath.Dir(realPath)
		info, err = os.Stat(realPath)
		if err != nil {
			return "", fmt.Errorf("stat parent %q: %w", realPath, err)
		}
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%q is not a directory", realPath)
	}
	return filepath.Clean(realPath), nil
}
