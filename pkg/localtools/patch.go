// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type PatchOperationType string

const (
	PatchCreate PatchOperationType = "create_file"
	PatchUpdate PatchOperationType = "update_file"
	PatchDelete PatchOperationType = "delete_file"
)

type PatchOperation struct {
	Type     PatchOperationType
	Path     string
	MovePath string
	Content  string
	Chunks   []PatchChunk
}

type PatchChunk struct {
	Context string
	Old     []string
	New     []string
	EOF     bool
}

type NativePatchOperation struct {
	Type string `json:"type"`
	Path string `json:"path"`
	Diff string `json:"diff,omitempty"`
}

type PatchChange struct {
	Path string
	Kind string
}

// ParseNativePatchOperation converts the Responses API apply_patch operation
// object into the same V4A operation representation used by custom tool calls.
func ParseNativePatchOperation(op NativePatchOperation) ([]PatchOperation, error) {
	if strings.TrimSpace(op.Path) == "" {
		return nil, fmt.Errorf("%w: operation path is required", ErrInvalidPatch)
	}
	if strings.HasPrefix(strings.TrimSpace(op.Diff), "*** Begin Patch") {
		return ParsePatch(op.Diff)
	}
	var body string
	switch PatchOperationType(op.Type) {
	case PatchCreate:
		body = "*** Add File: " + op.Path + "\n" + strings.TrimSuffix(op.Diff, "\n")
	case PatchUpdate:
		body = "*** Update File: " + op.Path + "\n" + strings.TrimSuffix(op.Diff, "\n")
	case PatchDelete:
		body = "*** Delete File: " + op.Path
	default:
		return nil, fmt.Errorf("%w: unsupported operation type %q", ErrInvalidPatch, op.Type)
	}
	return ParsePatch("*** Begin Patch\n" + body + "\n*** End Patch")
}

// ParsePatch parses the V4A patch format used by Codex custom apply_patch
// calls. It intentionally accepts only explicit file markers and line prefixes;
// path authorization happens later, before any filesystem mutation.
func ParsePatch(patch string) ([]PatchOperation, error) {
	patch = strings.ReplaceAll(patch, "\r\n", "\n")
	patch = strings.TrimSpace(patch)
	lines := strings.Split(patch, "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) != "*** Begin Patch" || strings.TrimSpace(lines[len(lines)-1]) != "*** End Patch" {
		return nil, fmt.Errorf("%w: expected *** Begin Patch and *** End Patch markers", ErrInvalidPatch)
	}

	var operations []PatchOperation
	for i := 1; i < len(lines)-1; {
		line := lines[i]
		switch {
		case strings.HasPrefix(line, "*** Add File: "):
			path := strings.TrimSpace(strings.TrimPrefix(line, "*** Add File: "))
			i++
			var content []string
			for i < len(lines)-1 && !isPatchFileMarker(lines[i]) {
				if !strings.HasPrefix(lines[i], "+") {
					return nil, patchLineError(i+1, "add-file lines must start with +")
				}
				content = append(content, strings.TrimPrefix(lines[i], "+"))
				i++
			}
			if path == "" || len(content) == 0 {
				return nil, patchLineError(i+1, "add-file hunk requires a path and content")
			}
			operations = append(operations, PatchOperation{Type: PatchCreate, Path: path, Content: strings.Join(content, "\n") + "\n"})

		case strings.HasPrefix(line, "*** Delete File: "):
			path := strings.TrimSpace(strings.TrimPrefix(line, "*** Delete File: "))
			if path == "" {
				return nil, patchLineError(i+1, "delete-file path is empty")
			}
			operations = append(operations, PatchOperation{Type: PatchDelete, Path: path})
			i++

		case strings.HasPrefix(line, "*** Update File: "):
			path := strings.TrimSpace(strings.TrimPrefix(line, "*** Update File: "))
			if path == "" {
				return nil, patchLineError(i+1, "update-file path is empty")
			}
			op := PatchOperation{Type: PatchUpdate, Path: path}
			i++
			if i < len(lines)-1 && strings.HasPrefix(lines[i], "*** Move to: ") {
				op.MovePath = strings.TrimSpace(strings.TrimPrefix(lines[i], "*** Move to: "))
				if op.MovePath == "" {
					return nil, patchLineError(i+1, "move destination is empty")
				}
				i++
			}
			for i < len(lines)-1 && !isPatchFileMarker(lines[i]) {
				chunk := PatchChunk{}
				if lines[i] == "@@" || strings.HasPrefix(lines[i], "@@ ") {
					chunk.Context = strings.TrimSpace(strings.TrimPrefix(lines[i], "@@"))
					i++
				}
				start := i
				for i < len(lines)-1 && !isPatchFileMarker(lines[i]) && lines[i] != "@@" && !strings.HasPrefix(lines[i], "@@ ") {
					change := lines[i]
					switch {
					case change == "*** End of File":
						chunk.EOF = true
					case strings.HasPrefix(change, " "):
						text := strings.TrimPrefix(change, " ")
						chunk.Old = append(chunk.Old, text)
						chunk.New = append(chunk.New, text)
					case strings.HasPrefix(change, "-"):
						chunk.Old = append(chunk.Old, strings.TrimPrefix(change, "-"))
					case strings.HasPrefix(change, "+"):
						chunk.New = append(chunk.New, strings.TrimPrefix(change, "+"))
					default:
						return nil, patchLineError(i+1, "update lines must start with space, +, or -")
					}
					i++
				}
				if i == start && chunk.Context == "" {
					return nil, patchLineError(i+1, "empty update chunk")
				}
				op.Chunks = append(op.Chunks, chunk)
			}
			if len(op.Chunks) == 0 && op.MovePath == "" {
				return nil, patchLineError(i+1, "update-file hunk is empty")
			}
			operations = append(operations, op)

		default:
			return nil, patchLineError(i+1, "expected add, update, or delete file marker")
		}
	}
	if len(operations) == 0 {
		return nil, fmt.Errorf("%w: patch contains no operations", ErrInvalidPatch)
	}
	return operations, nil
}

func patchLineError(line int, message string) error {
	return fmt.Errorf("%w: line %d: %s", ErrInvalidPatch, line, message)
}

func isPatchFileMarker(line string) bool {
	return strings.HasPrefix(line, "*** Add File: ") ||
		strings.HasPrefix(line, "*** Delete File: ") ||
		strings.HasPrefix(line, "*** Update File: ") ||
		strings.TrimSpace(line) == "*** End Patch"
}

type patchFileState struct {
	exists  bool
	content []byte
	mode    os.FileMode
}

// ApplyPatch validates every operation and computes every resulting file in
// memory before committing. If a filesystem error occurs during commit, the
// touched files are restored to their original state.
func (w *Workspace) ApplyPatch(ctx context.Context, operations []PatchOperation) ([]PatchChange, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("%w: patch contains no operations", ErrInvalidPatch)
	}
	w.patchMu.Lock()
	defer w.patchMu.Unlock()

	states := make(map[string]patchFileState)
	original := make(map[string]patchFileState)
	changed := make(map[string]struct{})
	var changes []PatchChange

	load := func(requestPath string) (string, patchFileState, error) {
		resolved, err := w.resolveForWrite(requestPath)
		if err != nil {
			return "", patchFileState{}, err
		}
		if state, ok := states[resolved]; ok {
			return resolved, state, nil
		}
		state := patchFileState{mode: 0o644}
		info, err := os.Lstat(resolved)
		switch {
		case err == nil:
			if !info.Mode().IsRegular() {
				return "", patchFileState{}, fmt.Errorf("%w: %s is not a regular file", ErrInvalidFile, requestPath)
			}
			data, readErr := os.ReadFile(resolved)
			if readErr != nil {
				return "", patchFileState{}, fmt.Errorf("read %s: %w", requestPath, readErr)
			}
			state = patchFileState{exists: true, content: data, mode: info.Mode().Perm()}
		case os.IsNotExist(err):
		default:
			return "", patchFileState{}, fmt.Errorf("stat %s: %w", requestPath, err)
		}
		states[resolved] = state
		original[resolved] = clonePatchState(state)
		return resolved, state, nil
	}

	for _, op := range operations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		path, state, err := load(op.Path)
		if err != nil {
			return nil, err
		}
		switch op.Type {
		case PatchCreate:
			if state.exists {
				return nil, fmt.Errorf("%w: create target already exists: %s", ErrInvalidPatch, op.Path)
			}
			state.exists, state.content = true, []byte(op.Content)
			states[path] = state
			changed[path] = struct{}{}
			changes = append(changes, PatchChange{Path: op.Path, Kind: "create"})

		case PatchDelete:
			if !state.exists {
				return nil, fmt.Errorf("%w: delete target does not exist: %s", ErrInvalidPatch, op.Path)
			}
			state.exists, state.content = false, nil
			states[path] = state
			changed[path] = struct{}{}
			changes = append(changes, PatchChange{Path: op.Path, Kind: "delete"})

		case PatchUpdate:
			if !state.exists {
				return nil, fmt.Errorf("%w: update target does not exist: %s", ErrInvalidPatch, op.Path)
			}
			updated, applyErr := applyPatchChunks(string(state.content), op.Path, op.Chunks)
			if applyErr != nil {
				return nil, applyErr
			}
			state.content = []byte(updated)
			if op.MovePath == "" {
				states[path] = state
				changed[path] = struct{}{}
				changes = append(changes, PatchChange{Path: op.Path, Kind: "update"})
				continue
			}
			destination, destState, destErr := load(op.MovePath)
			if destErr != nil {
				return nil, destErr
			}
			if destState.exists {
				return nil, fmt.Errorf("%w: move destination already exists: %s", ErrInvalidPatch, op.MovePath)
			}
			destState.exists, destState.content, destState.mode = true, state.content, state.mode
			states[destination] = destState
			state.exists, state.content = false, nil
			states[path] = state
			changed[path], changed[destination] = struct{}{}, struct{}{}
			changes = append(changes, PatchChange{Path: op.Path, Kind: "delete"}, PatchChange{Path: op.MovePath, Kind: "create"})

		default:
			return nil, fmt.Errorf("%w: unsupported operation type %q", ErrInvalidPatch, op.Type)
		}
	}

	paths := make([]string, 0, len(changed))
	for path := range changed {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	if err := commitPatchStates(paths, states); err != nil {
		rollbackErr := commitPatchStates(paths, original)
		if rollbackErr != nil {
			return nil, fmt.Errorf("commit patch: %v; rollback: %w", err, rollbackErr)
		}
		return nil, fmt.Errorf("commit patch: %w", err)
	}
	return changes, nil
}

func clonePatchState(state patchFileState) patchFileState {
	state.content = append([]byte(nil), state.content...)
	return state
}

func commitPatchStates(paths []string, states map[string]patchFileState) error {
	// Materialize every new version before deleting old paths, which keeps move
	// operations recoverable if a destination write fails.
	for _, path := range paths {
		state := states[path]
		if !state.exists {
			continue
		}
		if err := atomicWritePatchFile(path, state.content, state.mode); err != nil {
			return err
		}
	}
	for _, path := range paths {
		if states[path].exists {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete %s: %w", path, err)
		}
	}
	return nil
}

func atomicWritePatchFile(path string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".apply-patch-*")
	if err != nil {
		return fmt.Errorf("create patch temp for %s: %w", path, err)
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if mode == 0 {
		mode = 0o644
	}
	if err := temp.Chmod(mode.Perm()); err != nil {
		_ = temp.Close()
		return fmt.Errorf("chmod patch temp for %s: %w", path, err)
	}
	if _, err := temp.Write(content); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write patch temp for %s: %w", path, err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close patch temp for %s: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

func applyPatchChunks(content, path string, chunks []PatchChunk) (string, error) {
	if len(chunks) == 0 {
		return content, nil
	}
	lineEnding := "\n"
	if strings.Contains(content, "\r\n") {
		lineEnding = "\r\n"
	}
	normalized := strings.ReplaceAll(content, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	cursor := 0
	for _, chunk := range chunks {
		if chunk.Context != "" {
			idx := findPatchSequence(lines, []string{chunk.Context}, cursor, false)
			if idx < 0 {
				return "", fmt.Errorf("%w: failed to find context %q in %s", ErrInvalidPatch, chunk.Context, path)
			}
			cursor = idx + 1
		}
		if len(chunk.Old) == 0 {
			insertAt := len(lines)
			lines = append(lines[:insertAt], append(append([]string(nil), chunk.New...), lines[insertAt:]...)...)
			cursor = insertAt + len(chunk.New)
			continue
		}
		idx := findPatchSequence(lines, chunk.Old, cursor, chunk.EOF)
		if idx < 0 {
			return "", fmt.Errorf("%w: failed to find expected lines in %s:\n%s", ErrInvalidPatch, path, strings.Join(chunk.Old, "\n"))
		}
		replacement := append([]string(nil), chunk.New...)
		lines = append(lines[:idx], append(replacement, lines[idx+len(chunk.Old):]...)...)
		cursor = idx + len(replacement)
	}
	return strings.Join(lines, lineEnding) + lineEnding, nil
}

func findPatchSequence(lines, pattern []string, start int, eof bool) int {
	if len(pattern) == 0 {
		return start
	}
	if len(pattern) > len(lines) || start > len(lines)-len(pattern) {
		return -1
	}
	begin := start
	end := len(lines) - len(pattern)
	if eof {
		begin = end
	}
	comparators := []func(string) string{
		func(s string) string { return s },
		func(s string) string { return strings.TrimRight(s, " \t") },
		strings.TrimSpace,
	}
	for _, normalize := range comparators {
		for i := begin; i <= end; i++ {
			matched := true
			for j := range pattern {
				got := normalize(lines[i+j])
				want := normalize(pattern[j])
				if got != want {
					matched = false
					break
				}
			}
			if matched {
				return i
			}
		}
	}
	return -1
}
