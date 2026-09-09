// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Workspace struct {
	root string
	mu   sync.RWMutex
	// patchMu serializes multi-file patch transactions so validation and commit
	// observe one coherent workspace state.
	patchMu sync.Mutex
	// readRoots are additional absolute, symlink-evaluated roots that read-only
	// operations (ReadFile/ReadBytes/InspectFile via resolveExisting) may read
	// from when given an ABSOLUTE path within one — e.g. image-baked system skills
	// at /opt/agent-skills. Writes never consult them (resolveForWrite stays
	// root-only), and relative paths still resolve under root only.
	readRoots []string
	// writeRoots are projects the user explicitly selected as logical
	// workspaces. Ordinary external working-directory grants never enter this
	// list, preserving their read-only behavior.
	writeRoots []string
}

func NewWorkspace(root string) (*Workspace, error) {
	if root == "" {
		return nil, ErrInvalidRoot
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRoot, err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("create workspace root: %w", err)
	}
	evaluated, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("%w: eval root: %w", ErrInvalidRoot, err)
	}
	return &Workspace{root: evaluated}, nil
}

func (w *Workspace) Root() string {
	return w.root
}

// AddReadRoots registers additional read-only roots (absolute paths) that
// read-only file ops may read from. Each is resolved to an absolute,
// symlink-evaluated path; an entry that doesn't exist is skipped (so a deployment
// with no baked assets is a no-op rather than a startup error). Writes never use
// these roots.
func (w *Workspace) AddReadRoots(dirs ...string) error {
	for _, d := range dirs {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		abs, err := filepath.Abs(d)
		if err != nil {
			return fmt.Errorf("%w: read root %s: %w", ErrInvalidRoot, d, err)
		}
		evaluated, err := filepath.EvalSymlinks(abs)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("%w: eval read root %s: %w", ErrInvalidRoot, d, err)
		}
		info, err := os.Stat(evaluated)
		if err != nil {
			return fmt.Errorf("%w: stat read root %s: %w", ErrInvalidRoot, d, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("%w: read root %s is not a directory", ErrInvalidRoot, d)
		}
		w.mu.Lock()
		alreadyRegistered := false
		for _, existing := range w.readRoots {
			if existing == evaluated {
				alreadyRegistered = true
				break
			}
		}
		if !alreadyRegistered {
			w.readRoots = append(w.readRoots, evaluated)
		}
		w.mu.Unlock()
	}
	return nil
}

// GrantWorkingDirectory registers a user-selected directory for read-only file
// tools and as an allowed cwd for command execution. The structured write/edit
// APIs remain confined to Root().
func (w *Workspace) GrantWorkingDirectory(dir string) error {
	return w.AddReadRoots(dir)
}

// GrantWorkspaceDirectory grants a user-selected project to both read and
// write tools. Callers must expose this only through an explicit workspace
// selection action such as project-mode /cd.
func (w *Workspace) GrantWorkspaceDirectory(dir string) error {
	if err := w.AddReadRoots(dir); err != nil {
		return err
	}
	abs, err := filepath.Abs(strings.TrimSpace(dir))
	if err != nil {
		return fmt.Errorf("%w: workspace root %s: %w", ErrInvalidRoot, dir, err)
	}
	evaluated, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return fmt.Errorf("%w: eval workspace root %s: %w", ErrInvalidRoot, dir, err)
	}
	info, err := os.Stat(evaluated)
	if err != nil {
		return fmt.Errorf("%w: stat workspace root %s: %w", ErrInvalidRoot, dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: workspace root %s is not a directory", ErrInvalidRoot, dir)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, existing := range w.writeRoots {
		if existing == evaluated {
			return nil
		}
	}
	w.writeRoots = append(w.writeRoots, evaluated)
	return nil
}

func (w *Workspace) readRootsSnapshot() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]string(nil), w.readRoots...)
}

func (w *Workspace) writeRootsSnapshot() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]string(nil), w.writeRoots...)
}

// defaultReadFileMaxBytes bounds an uncapped read_file so a huge file can't OOM
// the harness. Far larger than any model context window, so it never truncates a
// legitimately-usable text read — purely a DoS guardrail.
const defaultReadFileMaxBytes = 10 << 20 // 10 MiB

func (w *Workspace) ReadFile(ctx context.Context, relPath string, maxBytes int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := w.resolveExisting(relPath)
	if err != nil {
		return "", err
	}
	limit := maxBytes
	if limit <= 0 {
		limit = defaultReadFileMaxBytes
	}
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", relPath, err)
	}
	defer func() { _ = f.Close() }()
	// Bound the read: never pull more than the cap into memory. os.ReadFile would
	// read the whole file first, so max_bytes gave no OOM protection. Text reads
	// truncate to the cap (unlike ReadBytes, which errors — a truncated binary is
	// corrupt).
	data, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", relPath, err)
	}
	return string(data), nil
}

// ReadBytes returns the raw bytes of relPath without coercing to a string, so it
// is safe for binary artifacts the agent presents (images, PDFs). Unlike
// ReadFile it errors rather than truncating when the file exceeds maxBytes
// (<=0 → no cap) — a truncated binary is corrupt.
func (w *Workspace) ReadBytes(ctx context.Context, relPath string, maxBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := w.resolveExisting(relPath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", relPath, err)
	}
	defer func() { _ = f.Close() }()
	var rdr io.Reader = f
	if maxBytes > 0 {
		rdr = io.LimitReader(f, maxBytes+1)
	}
	data, err := io.ReadAll(rdr)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", relPath, err)
	}
	if maxBytes > 0 && int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: %s exceeds limit %d bytes", ErrInvalidFile, relPath, maxBytes)
	}
	return data, nil
}

func (w *Workspace) WriteFile(ctx context.Context, relPath string, content string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := w.resolveForWrite(relPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", relPath, err)
	}
	return nil
}

func (w *Workspace) EditFile(ctx context.Context, relPath string, oldText string, newText string) error {
	if oldText == "" {
		return fmt.Errorf("%w: old text must not be empty", ErrInvalidFile)
	}
	current, err := w.ReadFile(ctx, relPath, 0)
	if err != nil {
		return err
	}
	switch strings.Count(current, oldText) {
	case 0:
		return ErrOldTextNotFound
	case 1:
		// Exactly one match is required so the model cannot accidentally edit the
		// wrong occurrence in a large file.
	default:
		return ErrOldTextNotUnique
	}
	updated := strings.Replace(current, oldText, newText, 1)
	if err := w.WriteFile(ctx, relPath, updated); err != nil {
		return err
	}
	return nil
}

func (w *Workspace) resolveExisting(relPath string) (string, error) {
	// Reads may target the workspace root or any registered read-only root.
	candidate, err := w.join(relPath, true)
	if err != nil {
		return "", err
	}
	evaluated, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", relPath, err)
	}
	if !w.containedInAny(evaluated) {
		return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
	}
	return evaluated, nil
}

func (w *Workspace) resolveForWrite(relPath string) (string, error) {
	// Writes are confined to the launch root or an explicitly granted writable
	// root. Walk to the nearest existing ancestor before evaluating symlinks so
	// a missing directory below a symlink cannot escape containment.
	candidate, err := w.join(relPath, false)
	if err != nil {
		return "", err
	}
	if info, statErr := os.Lstat(candidate); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%w: refusing to write through symlink %s", ErrInvalidFile, relPath)
		}
	} else if !os.IsNotExist(statErr) {
		return "", fmt.Errorf("stat %s: %w", relPath, statErr)
	}

	ancestor := filepath.Dir(candidate)
	missing := make([]string, 0, 4)
	for {
		if _, statErr := os.Lstat(ancestor); statErr == nil {
			break
		} else if !os.IsNotExist(statErr) {
			return "", fmt.Errorf("stat parent %s: %w", relPath, statErr)
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", fmt.Errorf("%w: no existing parent for %s", ErrInvalidFile, relPath)
		}
		missing = append(missing, filepath.Base(ancestor))
		ancestor = parent
	}
	evaluatedAncestor, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		return "", fmt.Errorf("resolve parent %s: %w", relPath, err)
	}
	if !w.containedInWritable(evaluatedAncestor) {
		return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		evaluatedAncestor = filepath.Join(evaluatedAncestor, missing[i])
	}
	return filepath.Join(evaluatedAncestor, filepath.Base(candidate)), nil
}

func (w *Workspace) join(relPath string, allowReadRoots bool) (string, error) {
	if filepath.IsAbs(relPath) {
		// An absolute path is accepted when it lexically points inside the workspace
		// root (models routinely pass /sahara/...), or — for reads — inside a
		// registered read-only root (e.g. image-baked system skills). Symlink escapes
		// are still caught downstream by EvalSymlinks + containedInAny().
		clean := filepath.Clean(relPath)
		if within(w.root, clean) {
			return clean, nil
		}
		if allowReadRoots {
			for _, r := range w.readRootsSnapshot() {
				if within(r, clean) {
					return clean, nil
				}
			}
		} else {
			for _, r := range w.writeRootsSnapshot() {
				if within(r, clean) {
					return clean, nil
				}
			}
		}
		return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
	}
	clean := filepath.Clean(relPath)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
	}
	if clean == "." {
		return w.root, nil
	}
	return filepath.Join(w.root, clean), nil
}

// containedInAny reports whether absPath is within the workspace root or any
// registered read-only root (used after EvalSymlinks on the read path).
func (w *Workspace) containedInAny(absPath string) bool {
	if w.contains(absPath) {
		return true
	}
	for _, r := range w.readRootsSnapshot() {
		if within(r, absPath) {
			return true
		}
	}
	return false
}

// within reports whether absPath is root itself or lexically inside root (both
// expected to be absolute, cleaned, symlink-evaluated paths).
func within(root, absPath string) bool {
	rel, err := filepath.Rel(root, absPath)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel))
}

func (w *Workspace) contains(absPath string) bool {
	rel, err := filepath.Rel(w.root, absPath)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel))
}

func (w *Workspace) containedInWritable(absPath string) bool {
	if w.contains(absPath) {
		return true
	}
	for _, root := range w.writeRootsSnapshot() {
		if within(root, absPath) {
			return true
		}
	}
	return false
}
