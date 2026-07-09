package sandbox

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// defaultExecTimeout bounds a command that supplies no TimeoutMillis so a
// derived operation can't hang the harness indefinitely.
const defaultExecTimeout = 30 * time.Second

// defaultMaxOutputBytes caps each of stdout/stderr so a runaway command can't
// OOM the process. Generous enough to hold any legitimate file-op JSON payload.
const defaultMaxOutputBytes = 16 << 20 // 16 MiB

// LocalExecutor implements Executor against the HOST filesystem via os/exec. It
// is both the test vehicle for FileOps and a real "local shell" backend
// (analogous to deepagents' LocalShellBackend). Commands run without a shell,
// with a process group that is SIGKILL'd on timeout.
type LocalExecutor struct {
	id        string
	MaxOutput int           // per-stream output cap (<=0 uses default)
	Timeout   time.Duration // default per-command timeout (<=0 uses default)
}

// NewLocalExecutor returns a LocalExecutor whose ID is "local".
func NewLocalExecutor() *LocalExecutor {
	return &LocalExecutor{id: "local"}
}

func (l *LocalExecutor) ID() string { return l.id }

func (l *LocalExecutor) Execute(ctx context.Context, req ExecRequest) (ExecResult, error) {
	if len(req.Argv) == 0 {
		return ExecResult{}, ErrEmptyArgv
	}

	timeout := l.Timeout
	if req.TimeoutMillis > 0 {
		timeout = time.Duration(req.TimeoutMillis) * time.Millisecond
	}
	if timeout <= 0 {
		timeout = defaultExecTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, req.Argv[0], req.Argv[1:]...)
	if req.Cwd != "" {
		cmd.Dir = req.Cwd
	}
	cmd.Env = os.Environ()
	for k, v := range req.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if len(req.Stdin) > 0 {
		cmd.Stdin = bytes.NewReader(req.Stdin)
	}
	// Own process group so a timeout kill reaps the whole tree, not just argv[0].
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	limit := l.MaxOutput
	if limit <= 0 {
		limit = defaultMaxOutputBytes
	}
	var stdout, stderr capBuffer
	stdout.limit = limit
	stderr.limit = limit
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return ExecResult{}, err
	}
	// A non-nil Wait error is expected for a non-zero exit; the exit code is
	// read from ProcessState below, so the error itself is not needed.
	_ = cmd.Wait()
	result := ExecResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if runCtx.Err() == context.DeadlineExceeded {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		result.ExitCode = -1
		return result, context.DeadlineExceeded
	}
	// A completed process — even a non-zero exit — is a successful Execute; the
	// exit code is carried in the result, not surfaced as a Go error.
	result.ExitCode = cmd.ProcessState.ExitCode()
	return result, nil
}

// Upload writes raw bytes to an absolute host path, creating parent dirs.
func (l *LocalExecutor) Upload(ctx context.Context, path string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if dir := parentDir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, content, 0o644)
}

// Download reads raw bytes from an absolute host path.
func (l *LocalExecutor) Download(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func parentDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return ""
}

// capBuffer accumulates output up to limit bytes and silently drops the rest, so
// a runaway command can't exhaust memory. limit<=0 means unbounded.
type capBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *capBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return b.buf.Write(p)
	}
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *capBuffer) Bytes() []byte { return b.buf.Bytes() }
