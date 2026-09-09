// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type FileChangeKind string

const (
	FileChangeAdd    FileChangeKind = "add"
	FileChangeChange FileChangeKind = "change"
	FileChangeUnlink FileChangeKind = "unlink"
)

type FileEvent struct {
	Path string         `json:"file_path"`
	Kind FileChangeKind `json:"event"`
}

type fileState struct {
	size  int64
	modNS int64
	mode  os.FileMode
}

type PollingWatcher struct {
	paths []string
	state map[string]fileState
}

func NewPollingWatcher(paths []string) (*PollingWatcher, error) {
	copied := make([]string, len(paths))
	copy(copied, paths)
	state, err := snapshot(copied)
	if err != nil {
		return nil, err
	}
	return &PollingWatcher{paths: copied, state: state}, nil
}

func (w *PollingWatcher) Poll(ctx context.Context) ([]FileEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	next, err := snapshot(w.paths)
	if err != nil {
		return nil, err
	}
	events := diffState(w.state, next)
	w.state = next
	return events, nil
}

type CWDEvent struct {
	OldCWD string `json:"old_cwd"`
	NewCWD string `json:"new_cwd"`
}

type CWDWatcher struct {
	cwd string
}

func NewCWDWatcher() (*CWDWatcher, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get cwd: %w", err)
	}
	return &CWDWatcher{cwd: cwd}, nil
}

func (w *CWDWatcher) Poll(ctx context.Context) (CWDEvent, bool, error) {
	if err := ctx.Err(); err != nil {
		return CWDEvent{}, false, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return CWDEvent{}, false, fmt.Errorf("get cwd: %w", err)
	}
	if cwd == w.cwd {
		return CWDEvent{}, false, nil
	}
	event := CWDEvent{OldCWD: w.cwd, NewCWD: cwd}
	w.cwd = cwd
	return event, true, nil
}

func snapshot(paths []string) (map[string]fileState, error) {
	state := make(map[string]fileState)
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("stat %s: %w", path, err)
		}
		if info.IsDir() {
			if err := snapshotDir(state, path); err != nil {
				return nil, err
			}
			continue
		}
		state[path] = stateFromInfo(info)
	}
	return state, nil
}

func snapshotDir(state map[string]fileState, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dir, err)
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		if info.IsDir() {
			continue
		}
		state[path] = stateFromInfo(info)
	}
	return nil
}

func stateFromInfo(info os.FileInfo) fileState {
	return fileState{size: info.Size(), modNS: info.ModTime().UnixNano(), mode: info.Mode()}
}

func diffState(prev map[string]fileState, next map[string]fileState) []FileEvent {
	paths := make([]string, 0, len(prev)+len(next))
	seen := make(map[string]struct{}, len(prev)+len(next))
	for path := range prev {
		paths = append(paths, path)
		seen[path] = struct{}{}
	}
	for path := range next {
		if _, ok := seen[path]; !ok {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	events := make([]FileEvent, 0)
	for _, path := range paths {
		before, hadBefore := prev[path]
		after, hasAfter := next[path]
		switch {
		case !hadBefore && hasAfter:
			events = append(events, FileEvent{Path: path, Kind: FileChangeAdd})
		case hadBefore && !hasAfter:
			events = append(events, FileEvent{Path: path, Kind: FileChangeUnlink})
		case hadBefore && hasAfter && before != after:
			events = append(events, FileEvent{Path: path, Kind: FileChangeChange})
		}
	}
	return events
}
