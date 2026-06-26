package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

type LocalConfig struct {
	Workspace *localtools.Workspace
	Python    string
	Exa       *localtools.ExaClient
	Timeout   time.Duration
	MaxOutput int
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
		{Name: "read_file", Description: "Read a UTF-8 text file from the workspace.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Workspace-relative path"},"max_bytes":{"type":"integer","description":"Optional max bytes to read"}},"required":["path"]}`)},
		{Name: "write_file", Description: "Create or overwrite a workspace file with the given content.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}`)},
		{Name: "edit_file", Description: "Replace an exact substring in a workspace file (old_text must occur exactly once).", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string"},"old_text":{"type":"string"},"new_text":{"type":"string"}},"required":["path","old_text","new_text"]}`)},
		{Name: "list_dir", Description: "List entries in a workspace directory.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Workspace-relative directory; empty for root"}}}`)},
		{Name: "inspect_file", Description: "Return metadata (type, size, hash, image dimensions) and a workspace:// reference for a file without inlining its bytes.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`)},
		{Name: "shell", Description: "Run a bounded shell command inside the sandbox workspace.", Parameters: schema(`{"type":"object","properties":{"command":{"type":"string"},"args":{"type":"array","items":{"type":"string"}},"cwd":{"type":"string"},"timeout_ms":{"type":"integer"},"max_output":{"type":"integer"}},"required":["command"]}`)},
		{Name: "python", Description: "Execute a Python snippet in the sandbox for calculation/analysis; returns stdout.", Parameters: schema(`{"type":"object","properties":{"code":{"type":"string"},"cwd":{"type":"string"},"timeout_ms":{"type":"integer"},"max_output":{"type":"integer"}},"required":["code"]}`)},
		{Name: "web_search", Description: "Search the web (Exa) and return matching results.", Parameters: schema(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}`)},
	}
}

func readFile(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path     string `json:"path"`
		MaxBytes int64  `json:"max_bytes"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	text, err := cfg.Workspace.ReadFile(ctx, args.Path, args.MaxBytes)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, struct {
		Text string `json:"text"`
	}{Text: text})
}

func writeFile(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
	if err := cfg.Workspace.WriteFile(ctx, args.Path, args.Content); err != nil {
		return Result{}, err
	}
	return okResult(req)
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
	if err := cfg.Workspace.EditFile(ctx, args.Path, args.OldText, args.NewText); err != nil {
		return Result{}, err
	}
	return okResult(req)
}

func listDir(ctx context.Context, cfg LocalConfig, req Request) (Result, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := decodeArgs(req, &args); err != nil {
		return Result{}, err
	}
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
	info, err := cfg.Workspace.InspectFile(ctx, args.Path)
	if err != nil {
		return Result{}, err
	}
	return marshalResult(req, info)
}
