// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileAuditSink struct {
	path string
	mu   sync.Mutex
}

func NewFileAuditSink(path string) (*FileAuditSink, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: audit path required", ErrInvalidTool)
	}
	return &FileAuditSink{path: path}, nil
}

func (s *FileAuditSink) Record(ctx context.Context, record AuditRecord) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create audit dir: %w", err)
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open audit log: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close audit log: %w", closeErr)
		}
	}()
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal audit record: %w", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write audit record: %w", err)
	}
	return nil
}
