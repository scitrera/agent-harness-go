package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

type LocalConfig struct {
	Workspace     *localtools.Workspace
	Python        string
	Exa           *localtools.ExaClient
	Timeout       time.Duration
	MaxOutput     int
	CommandPolicy CommandDecider
}

func RegisterLocal(reg *Registry, cfg LocalConfig) error {
	if cfg.Workspace == nil {
		return fmt.Errorf("%w: workspace required", ErrInvalidTool)
	}
	registrations := []struct {
		name    string
		handler Handler
	}{
		{"read_file", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return readFile(ctx, cfg, req) })},
		{"write_file", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return writeFile(ctx, cfg, req) })},
		{"edit_file", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return editFile(ctx, cfg, req) })},
		{"list_dir", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return listDir(ctx, cfg, req) })},
		{"inspect_file", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return inspectFile(ctx, cfg, req) })},
		{"shell", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return shell(ctx, cfg, req) })},
		{"python", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return python(ctx, cfg, req) })},
		// todo_write needs no workspace/cfg — it surfaces a todo part via the
		// per-turn PartEmitter on ctx (no-op when none is wired).
		{"todo_write", HandlerFunc(todoWrite)},
	}
	if cfg.Exa != nil {
		registrations = append(registrations, struct {
			name    string
			handler Handler
		}{"web_search", HandlerFunc(func(ctx context.Context, req Request) (Result, error) { return webSearch(ctx, cfg, req) })})
	}
	for _, item := range registrations {
		if err := reg.Register(item.name, item.handler); err != nil {
			return err
		}
	}
	descriptors := append(localDescriptors(), todoDescriptor())
	for _, d := range descriptors {
		if _, ok := reg.handlers[d.Name]; ok {
			reg.Describe(d)
		}
	}
	return nil
}

func localDescriptors() []Descriptor {
	schema := func(s string) json.RawMessage { return json.RawMessage(s) }
	return []Descriptor{
		{Name: "read_file", Description: "Read a UTF-8 text file relative to the active working directory, or by absolute path within the workspace/an explicitly granted directory. Optionally return only a 1-indexed inclusive line range via start_line/end_line; when a range is given the output is line-number prefixed.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path relative to the active working directory, or an absolute allowed path"},"max_bytes":{"type":"integer","description":"Optional max bytes to read"},"start_line":{"type":"integer","description":"Optional 1-indexed first line to return (inclusive); enables line-number-prefixed output"},"end_line":{"type":"integer","description":"Optional 1-indexed last line to return (inclusive); defaults to end of file"}},"required":["path"]}`), Concurrency: ConcurrencyParallelSafe},
		{Name: "write_file", Description: "Create or overwrite a file relative to the active working directory; writes remain confined to the launch workspace or an explicitly selected logical workspace.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path relative to the active working directory, or an absolute path inside an explicitly selected workspace"},"content":{"type":"string"}},"required":["path","content"]}`)},
		{Name: "edit_file", Description: "Replace an exact substring in a file relative to the active working directory (old_text must occur exactly once); writes remain confined to the launch workspace or an explicitly selected logical workspace.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path relative to the active working directory, or an absolute path inside an explicitly selected workspace"},"old_text":{"type":"string"},"new_text":{"type":"string"}},"required":["path","old_text","new_text"]}`)},
		{Name: "list_dir", Description: "List entries relative to the active working directory or in an absolute allowed directory.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Directory relative to the active working directory; empty means the active working directory"}}}`), Concurrency: ConcurrencyParallelSafe},
		{Name: "inspect_file", Description: "Return metadata (type, size, hash, image dimensions) and a workspace:// reference for a file relative to the active working directory or at an absolute allowed path without inlining its bytes.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path relative to the active working directory, or an absolute allowed path"}},"required":["path"]}`), Concurrency: ConcurrencyParallelSafe},
		{Name: "shell", Description: "Run a bounded command line in the active working directory or an explicitly selected cwd. Put a complete shell command line in command, or provide a single executable in command with a separate args array.", Parameters: schema(`{"type":"object","properties":{"command":{"type":"string","description":"Complete shell command line, or executable name when args is provided"},"args":{"type":"array","items":{"type":"string"}},"cwd":{"type":"string","description":"Optional directory relative to the active working directory, or an absolute allowed directory; omitted means the active working directory"},"timeout_ms":{"type":"integer"},"max_output":{"type":"integer"}},"required":["command"]}`)},
		{Name: "python", Description: "Execute a Python snippet in the active working directory or an explicitly selected cwd; returns stdout.", Parameters: schema(`{"type":"object","properties":{"code":{"type":"string"},"cwd":{"type":"string","description":"Optional directory relative to the active working directory, or an absolute allowed directory; omitted means the active working directory"},"timeout_ms":{"type":"integer"},"max_output":{"type":"integer"}},"required":["code"]}`)},
		{Name: "web_search", Description: "Search the web (Exa) and return matching results.", Parameters: schema(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}`), Concurrency: ConcurrencyParallelSafe},
	}
}

func readFile(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path      string `json:"path"`
		MaxBytes  int64  `json:"max_bytes"`
		StartLine int    `json:"start_line"`
		EndLine   int    `json:"end_line"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.Path = ResolveWorkingPath(ctx, args.Path)
	// A ctx-carried FileDelegate (e.g. the ACP client's fs/read_text_file) wins
	// over the local workspace when present.
	var text string
	var err error
	if d := FileDelegateFrom(ctx); d != nil {
		text, err = d.ReadFile(ctx, args.Path, args.MaxBytes)
	} else {
		text, err = cfg.Workspace.ReadFile(ctx, args.Path, args.MaxBytes)
	}
	if err != nil {
		return Result{}, err
	}
	// Optional 1-indexed inclusive line range. Applied AFTER the read (max_bytes
	// still bounds the bytes fetched), so a range past a max_bytes cap simply
	// yields fewer lines. When either bound is set, the slice is line-number
	// prefixed (cat -n style) so the model can reference exact lines; a full read
	// (no bounds) is returned raw, unchanged.
	if args.StartLine > 0 || args.EndLine > 0 {
		text = numberedLineSlice(text, args.StartLine, args.EndLine)
	}
	return marshalResult(req, struct {
		Text string `json:"text"`
	}{Text: text})
}

// numberedLineSlice returns lines [start, end] (1-indexed, inclusive) of text,
// each prefixed with its line number in "%6d\t" form. start <= 0 clamps to 1;
// end <= 0 or past EOF clamps to the last line. A trailing newline does not count
// as an extra line. Returns "" when the range starts past EOF.
func numberedLineSlice(text string, start, end int) string {
	lines := strings.Split(text, "\n")
	if n := len(lines); n > 0 && lines[n-1] == "" {
		lines = lines[:n-1] // drop the phantom line from a trailing newline
	}
	if start < 1 {
		start = 1
	}
	if end < 1 || end > len(lines) {
		end = len(lines)
	}
	if start > len(lines) || start > end {
		return ""
	}
	var b strings.Builder
	for i := start; i <= end; i++ {
		fmt.Fprintf(&b, "%6d\t%s\n", i, lines[i-1])
	}
	return strings.TrimRight(b.String(), "\n")
}

func writeFile(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.Path = ResolveWorkingPath(ctx, args.Path)
	// Delegate the write to a ctx-carried FileDelegate (ACP fs/write_text_file)
	// when present, else the local workspace. FileChanges metadata is preserved
	// either way.
	if d := FileDelegateFrom(ctx); d != nil {
		if err := d.WriteFile(ctx, args.Path, args.Content); err != nil {
			return Result{}, err
		}
	} else if err := cfg.Workspace.WriteFile(ctx, args.Path, args.Content); err != nil {
		return Result{}, err
	}
	result, err := okResult(req)
	if err != nil {
		return Result{}, err
	}
	result.Metadata.FileChanges = []FileChange{{Path: args.Path, Kind: "write"}}
	return result, nil
}

func editFile(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path    string `json:"path"`
		OldText string `json:"old_text"`
		NewText string `json:"new_text"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.Path = ResolveWorkingPath(ctx, args.Path)
	// Delegate the edit to a ctx-carried FileDelegate (ACP read+replace+write via
	// the client) when present, else the local workspace.
	if d := FileDelegateFrom(ctx); d != nil {
		if err := d.EditFile(ctx, args.Path, args.OldText, args.NewText); err != nil {
			return Result{}, err
		}
	} else if err := cfg.Workspace.EditFile(ctx, args.Path, args.OldText, args.NewText); err != nil {
		return Result{}, err
	}
	result, err := okResult(req)
	if err != nil {
		return Result{}, err
	}
	result.Metadata.FileChanges = []FileChange{{Path: args.Path, Kind: "edit"}}
	return result, nil
}

// list_dir + inspect_file stay local always: ACP v1 has no directory-listing or
// file-inspection client delegation, so there is nothing to route them through.
func listDir(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.Path = ResolveWorkingPath(ctx, args.Path)
	entries, err := cfg.Workspace.ListDir(ctx, args.Path)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, struct {
		Entries []localtools.DirEntry `json:"entries"`
	}{Entries: entries})
}

func inspectFile(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	args.Path = ResolveWorkingPath(ctx, args.Path)
	info, err := cfg.Workspace.InspectFile(ctx, args.Path)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, info)
}
