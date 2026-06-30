package localtools

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Workspace struct {
	root string
	// readRoots are additional absolute, symlink-evaluated roots that read-only
	// operations (ReadFile/ReadBytes/InspectFile via resolveExisting) may read
	// from when given an ABSOLUTE path within one — e.g. image-baked system skills
	// at /opt/agent-skills. Writes never consult them (resolveForWrite stays
	// root-only), and relative paths still resolve under root only.
	readRoots []string
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
		w.readRoots = append(w.readRoots, evaluated)
	}
	return nil
}

func (w *Workspace) ReadFile(ctx context.Context, relPath string, maxBytes int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := w.resolveExisting(relPath)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", relPath, err)
	}
	if maxBytes > 0 && int64(len(data)) > maxBytes {
		data = data[:maxBytes]
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
	defer f.Close()
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
	current, err := w.ReadFile(ctx, relPath, 0)
	if err != nil {
		return err
	}
	if !strings.Contains(current, oldText) {
		return ErrOldTextNotFound
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
	// Writes are confined to the workspace root — never a read-only root.
	candidate, err := w.join(relPath, false)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(candidate)
	evaluatedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		if os.IsNotExist(err) {
			evaluatedParent = parent
		} else {
			return "", fmt.Errorf("resolve parent %s: %w", relPath, err)
		}
	}
	if !w.contains(evaluatedParent) {
		return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
	}
	return candidate, nil
}

func (w *Workspace) join(relPath string, allowReadRoots bool) (string, error) {
	if filepath.IsAbs(relPath) {
		// An absolute path is accepted when it lexically points inside the workspace
		// root (models routinely pass /workspace/...), or — for reads — inside a
		// registered read-only root (e.g. image-baked system skills). Symlink escapes
		// are still caught downstream by EvalSymlinks + containedInAny().
		clean := filepath.Clean(relPath)
		if within(w.root, clean) {
			return clean, nil
		}
		if allowReadRoots {
			for _, r := range w.readRoots {
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
	for _, r := range w.readRoots {
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
