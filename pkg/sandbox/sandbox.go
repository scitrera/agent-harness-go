// Package sandbox provides a provider-agnostic filesystem+command surface for
// agent sandboxes (Docker, gVisor, k8s, remote SSH, or the local host).
//
// # Design: execute() derives everything
//
// A sandbox backend only has to implement the three low-level primitives on the
// Executor interface — command execution, file upload, and file download (plus a
// stable ID). Every higher-level filesystem operation (ls, read, write, edit,
// grep, glob, delete) is DERIVED from those primitives by FileOps, which
// generates a minimal script and runs it through Executor.Execute. This mirrors
// langchain-deepagents' BaseSandbox: adding a new provider is cheap because you
// implement ~4 methods and get the full file-operation surface for free.
//
// The derived file operations shell out to python3 inside the sandbox (a tiny
// `python3 -c` program whose inputs are base64-encoded and whose output is a JSON
// contract on stdout). base64 sidesteps shell-escaping and encoding hazards, and
// JSON gives the Go side an unambiguous result to unmarshal. The one hard
// assumption is that python3 is on PATH inside the sandbox — true for essentially
// every agent container image; grep additionally prefers ripgrep (rg) when the
// sandbox has it and transparently falls back to python otherwise.
//
// # Adding a new provider
//
// Implement Executor for your backend:
//
//	type MyExecutor struct{ /* connection handle */ }
//	func (e *MyExecutor) Execute(ctx context.Context, req ExecRequest) (ExecResult, error) { ... }
//	func (e *MyExecutor) Upload(ctx context.Context, path string, content []byte) error   { ... }
//	func (e *MyExecutor) Download(ctx context.Context, path string) ([]byte, error)        { ... }
//	func (e *MyExecutor) ID() string                                                       { ... }
//
// Then wrap it: fops := sandbox.NewFileOps(myExecutor). No other code changes.
// Execute must run req.Argv WITHOUT a shell (argv-based, to avoid quoting bugs);
// callers that genuinely need a shell pass Argv{"sh", "-c", script}.
package sandbox

import "context"

// Executor is the minimal contract a sandbox backend implements. Everything else
// (see FileOps) is derived from these primitives.
type Executor interface {
	// Execute runs a command in the sandbox and returns the combined result.
	// Argv is executed directly (no shell). A completed process — even one that
	// exits non-zero — is reported via ExecResult.ExitCode with a nil error;
	// only failures to launch, timeouts, or I/O errors return a non-nil error.
	Execute(ctx context.Context, cmd ExecRequest) (ExecResult, error)
	// Upload writes raw bytes to an absolute path in the sandbox.
	Upload(ctx context.Context, path string, content []byte) error
	// Download reads raw bytes from an absolute path in the sandbox.
	Download(ctx context.Context, path string) ([]byte, error)
	// ID returns the stable sandbox identifier.
	ID() string
}

// ExecRequest describes a single command invocation. Argv[0] is the program and
// Argv[1:] its arguments; there is intentionally no shell string field so callers
// never have to quote. Zero-value fields are treated as unset/defaulted.
type ExecRequest struct {
	Argv          []string          // program + args, run without a shell
	Stdin         []byte            // optional standard input
	Cwd           string            // working directory (empty = backend default)
	Env           map[string]string // extra environment, layered over the backend's
	TimeoutMillis int               // wall-clock limit; <=0 uses the backend default
}

// ExecResult is the outcome of an ExecRequest. Stdout/Stderr may be truncated by
// the backend's output cap.
type ExecResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}
