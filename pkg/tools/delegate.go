package tools

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

// FileDelegate overrides the workspace file ops for a turn: when one is on ctx
// (WithFileDelegate), the read_file/write_file/edit_file handlers route through
// it instead of cfg.Workspace. The ACP channel wires one that proxies to the
// editor's fs/* client capability so edits land in the user's open buffers. No
// ACP import here — the seam is ctx-carried, so tools stays transport-agnostic.
type FileDelegate interface {
	ReadFile(ctx context.Context, path string, maxBytes int64) (string, error)
	WriteFile(ctx context.Context, path, content string) error
	EditFile(ctx context.Context, path, oldText, newText string) error
}

// CommandDelegate overrides RunCommand for a turn: when one is on ctx
// (WithCommandDelegate), the shell/python handlers run the command through it
// (e.g. the ACP terminal/* client capability) instead of cfg.Workspace. The
// command policy is still enforced first, on the handler side.
type CommandDelegate interface {
	RunCommand(ctx context.Context, spec localtools.CommandSpec) (localtools.CommandResult, error)
}

type fileDelegateKey struct{}
type commandDelegateKey struct{}

// WithFileDelegate carries a FileDelegate on ctx. nil is a no-op.
func WithFileDelegate(ctx context.Context, d FileDelegate) context.Context {
	if d == nil {
		return ctx
	}
	return context.WithValue(ctx, fileDelegateKey{}, d)
}

// FileDelegateFrom returns the FileDelegate carried by WithFileDelegate, or nil.
func FileDelegateFrom(ctx context.Context) FileDelegate {
	d, _ := ctx.Value(fileDelegateKey{}).(FileDelegate)
	return d
}

// WithCommandDelegate carries a CommandDelegate on ctx. nil is a no-op.
func WithCommandDelegate(ctx context.Context, d CommandDelegate) context.Context {
	if d == nil {
		return ctx
	}
	return context.WithValue(ctx, commandDelegateKey{}, d)
}

// CommandDelegateFrom returns the CommandDelegate carried by WithCommandDelegate,
// or nil.
func CommandDelegateFrom(ctx context.Context) CommandDelegate {
	d, _ := ctx.Value(commandDelegateKey{}).(CommandDelegate)
	return d
}
