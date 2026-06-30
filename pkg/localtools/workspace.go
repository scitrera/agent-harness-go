package localtools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Workspace struct {
	root string
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
	candidate, err := w.join(relPath)
	if err != nil {
		return "", err
	}
	evaluated, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", relPath, err)
	}
	if !w.contains(evaluated) {
		return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
	}
	return evaluated, nil
}

func (w *Workspace) resolveForWrite(relPath string) (string, error) {
	candidate, err := w.join(relPath)
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

func (w *Workspace) join(relPath string) (string, error) {
	if filepath.IsAbs(relPath) {
		// An absolute path is accepted only when it lexically points inside the
		// workspace root. Models routinely pass /workspace/... (the root itself),
		// and rejecting every absolute path outright forced a needless retry.
		// Symlink escapes are still caught downstream by EvalSymlinks + contains().
		clean := filepath.Clean(relPath)
		if clean != w.root && !strings.HasPrefix(clean, w.root+string(filepath.Separator)) {
			return "", fmt.Errorf("%w: %s", ErrPathOutsideRoot, relPath)
		}
		return clean, nil
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

func (w *Workspace) contains(absPath string) bool {
	rel, err := filepath.Rel(w.root, absPath)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel))
}
