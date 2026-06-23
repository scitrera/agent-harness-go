package localtools

import (
	"context"
	"fmt"
	"os"
)

type DirEntry struct {
	Name  string
	IsDir bool
}

func (w *Workspace) ListDir(ctx context.Context, relPath string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := w.resolveExisting(relPath)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", relPath, err)
	}
	out := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, DirEntry{Name: entry.Name(), IsDir: entry.IsDir()})
	}
	return out, nil
}
