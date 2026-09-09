// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

// record.go provides the opt-in trace/training recorder for the reference CLI: a
// file-backed turn.TurnRecorder that appends one JSON line per LLM call. It is
// wired only when --record <path> is set, so recording is off (zero overhead) by
// default.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/turn"
)

// fileTurnRecorder appends turn.LLMCallRecords as JSONL to a file. Encode writes
// through to the file per call, so records are durable without an explicit close;
// the mutex makes it safe for concurrent sub-agent turns.
type fileTurnRecorder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newFileTurnRecorder(path string) (*fileTurnRecorder, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileTurnRecorder{enc: json.NewEncoder(f)}, nil
}

func (r *fileTurnRecorder) RecordLLMCall(_ context.Context, rec turn.LLMCallRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enc.Encode(rec)
}
