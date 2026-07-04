// Package atomicfile writes a file durably. Content is written to a temp file in
// the destination directory, then renamed into place, so a concurrent reader
// never observes a partial file. Parent directories are created and the temp file
// is removed on any error. It consolidates several hand-rolled copies (task state,
// team graph, history + session stores) that had diverged — two of them used a
// non-atomic WriteFile+Rename that leaked a stray .tmp on a crash between write
// and rename.
package atomicfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// Write atomically writes data to path with the given file mode, creating parent
// directories (0o755) as needed. A concurrent reader sees either the old file or
// the new one, never a partial write; the temp file is cleaned up on any error.
func Write(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", tmpName, err)
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("commit %s: %w", path, err)
	}
	committed = true
	return nil
}
