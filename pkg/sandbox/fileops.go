package sandbox

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// pythonBin is the interpreter the derived file operations shell out to inside
// the sandbox. python3 is assumed present (see the package doc); it is the one
// hard dependency of the derive-from-Execute approach.
const pythonBin = "python3"

const (
	// defaultReadMaxBytes bounds a Read so a huge file can't flood the context.
	defaultReadMaxBytes = 2 << 20 // 2 MiB
	// maxGlobResults caps how many paths Glob returns.
	maxGlobResults = 5000
	// maxGrepMatches caps how many matches Grep returns.
	maxGrepMatches = 1000
)

// DirEntry is one entry returned by Ls.
type DirEntry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

// GrepMatch is one matching line returned by Grep.
type GrepMatch struct {
	Path       string
	LineNumber int
	Line       string
}

// FileOps derives the full filesystem operation surface (ls/read/write/edit/
// delete/glob/grep) from an Executor's three primitives. Each method generates a
// minimal python3 helper whose inputs are base64-encoded and whose stdout is a
// JSON contract this file unmarshals.
type FileOps struct {
	Exec Executor
}

// NewFileOps returns a FileOps backed by the given Executor.
func NewFileOps(e Executor) *FileOps {
	return &FileOps{Exec: e}
}

// b64 encodes a string for safe transport into a generated script.
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// runScript runs a helper via python3, feeding args as a JSON object on stdin and
// returning the raw stdout for the caller to unmarshal. It fails if the script
// exits non-zero.
func (f *FileOps) runScript(ctx context.Context, script string, args map[string]any) ([]byte, error) {
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal args: %w", ErrScriptFailed, err)
	}
	res, err := f.Exec.Execute(ctx, ExecRequest{
		Argv:  []string{pythonBin, "-c", script},
		Stdin: payload,
	})
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("%w: exit %d: %s", ErrScriptFailed, res.ExitCode, string(res.Stderr))
	}
	return res.Stdout, nil
}

// baseResult is embedded in every operation's JSON contract.
type baseResult struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error"`
}

// Ls lists the entries of a directory (non-recursive), sorted by name.
func (f *FileOps) Ls(ctx context.Context, path string) ([]DirEntry, error) {
	out, err := f.runScript(ctx, lsScript, map[string]any{"path": b64(path)})
	if err != nil {
		return nil, err
	}
	var r struct {
		baseResult
		Entries []DirEntry `json:"entries"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return nil, fmt.Errorf("%w: parse ls: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		return nil, fmt.Errorf("%w: ls %s: %s", ErrOpFailed, path, r.Error)
	}
	return r.Entries, nil
}

// Read returns a line-addressable slice of a text file. offsetLines is the
// 0-based first line to include; limitLines<=0 returns through end of file. The
// result is byte-capped at defaultReadMaxBytes.
func (f *FileOps) Read(ctx context.Context, path string, offsetLines, limitLines int) (string, error) {
	out, err := f.runScript(ctx, readScript, map[string]any{
		"path":      b64(path),
		"offset":    offsetLines,
		"limit":     limitLines,
		"max_bytes": defaultReadMaxBytes,
	})
	if err != nil {
		return "", err
	}
	var r struct {
		baseResult
		ContentB64 string `json:"content_b64"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return "", fmt.Errorf("%w: parse read: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		return "", fmt.Errorf("%w: read %s: %s", ErrOpFailed, path, r.Error)
	}
	content, err := base64.StdEncoding.DecodeString(r.ContentB64)
	if err != nil {
		return "", fmt.Errorf("%w: decode read: %w", ErrScriptFailed, err)
	}
	return string(content), nil
}

// Write creates or overwrites path with content, creating parent directories.
func (f *FileOps) Write(ctx context.Context, path, content string) error {
	out, err := f.runScript(ctx, writeScript, map[string]any{
		"path":    b64(path),
		"content": b64(content),
	})
	if err != nil {
		return err
	}
	var r baseResult
	if err := json.Unmarshal(out, &r); err != nil {
		return fmt.Errorf("%w: parse write: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		return fmt.Errorf("%w: write %s: %s", ErrOpFailed, path, r.Error)
	}
	return nil
}

// Edit performs an exact single find/replace of oldText with newText in path.
// It returns ErrOldTextNotFound if oldText is absent and ErrOldTextAmbiguous if
// it appears more than once (mirroring localtools.EditFile's single-replacement
// semantics, with the ambiguity guard added).
func (f *FileOps) Edit(ctx context.Context, path, oldText, newText string) error {
	out, err := f.runScript(ctx, editScript, map[string]any{
		"path": b64(path),
		"old":  b64(oldText),
		"new":  b64(newText),
	})
	if err != nil {
		return err
	}
	var r struct {
		baseResult
		Code string `json:"code"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return fmt.Errorf("%w: parse edit: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		switch r.Code {
		case "not_found":
			return ErrOldTextNotFound
		case "ambiguous":
			return ErrOldTextAmbiguous
		default:
			return fmt.Errorf("%w: edit %s: %s", ErrOpFailed, path, r.Error)
		}
	}
	return nil
}

// Delete removes a file (or recursively a directory) at path.
func (f *FileOps) Delete(ctx context.Context, path string) error {
	out, err := f.runScript(ctx, deleteScript, map[string]any{"path": b64(path)})
	if err != nil {
		return err
	}
	var r baseResult
	if err := json.Unmarshal(out, &r); err != nil {
		return fmt.Errorf("%w: parse delete: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		return fmt.Errorf("%w: delete %s: %s", ErrOpFailed, path, r.Error)
	}
	return nil
}

// Glob returns paths matching a shell glob pattern (recursive ** supported),
// sorted and capped at maxGlobResults.
func (f *FileOps) Glob(ctx context.Context, pattern string) ([]string, error) {
	out, err := f.runScript(ctx, globScript, map[string]any{
		"pattern":     b64(pattern),
		"max_results": maxGlobResults,
	})
	if err != nil {
		return nil, err
	}
	var r struct {
		baseResult
		Paths []string `json:"paths"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return nil, fmt.Errorf("%w: parse glob: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		return nil, fmt.Errorf("%w: glob %s: %s", ErrOpFailed, pattern, r.Error)
	}
	return r.Paths, nil
}

// Grep searches for a regex pattern under path (a file or a directory tree). It
// uses ripgrep when the sandbox has it and falls back to a python regex walk
// otherwise. Matches are capped at maxGrepMatches.
func (f *FileOps) Grep(ctx context.Context, pattern, path string) ([]GrepMatch, error) {
	out, err := f.runScript(ctx, grepScript, map[string]any{
		"pattern":     b64(pattern),
		"path":        b64(path),
		"max_matches": maxGrepMatches,
	})
	if err != nil {
		return nil, err
	}
	var r struct {
		baseResult
		Matches []struct {
			Path       string `json:"path"`
			LineNumber int    `json:"line_number"`
			Line       string `json:"line"`
		} `json:"matches"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return nil, fmt.Errorf("%w: parse grep: %w", ErrScriptFailed, err)
	}
	if !r.Ok {
		return nil, fmt.Errorf("%w: grep %s: %s", ErrOpFailed, pattern, r.Error)
	}
	matches := make([]GrepMatch, 0, len(r.Matches))
	for _, m := range r.Matches {
		matches = append(matches, GrepMatch{Path: m.Path, LineNumber: m.LineNumber, Line: m.Line})
	}
	return matches, nil
}
