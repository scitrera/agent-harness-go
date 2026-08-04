package localtools

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type CommandSpec struct {
	Name      string
	Args      []string
	CWD       string
	Env       []string
	Timeout   time.Duration
	MaxOutput int
}

type CommandResult struct {
	ExitCode        int
	Output          string
	OutputBytes     int
	OutputTruncated bool
	PID             int
}

func (w *Workspace) RunCommand(ctx context.Context, spec CommandSpec) (CommandResult, error) {
	if spec.Name == "" {
		return CommandResult{}, fmt.Errorf("%w: empty command", ErrCommandFailed)
	}
	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cwd := "."
	if spec.CWD != "" {
		cwd = spec.CWD
	}
	dir, err := w.resolveExisting(cwd)
	if err != nil {
		return CommandResult{}, err
	}

	cmd := exec.CommandContext(runCtx, spec.Name, spec.Args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), spec.Env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var out limitedBuffer
	out.limit = spec.MaxOutput
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		return CommandResult{}, fmt.Errorf("%w: start command: %w", ErrCommandFailed, err)
	}
	pid := cmd.Process.Pid
	err = cmd.Wait()
	if runCtx.Err() == context.DeadlineExceeded {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		return CommandResult{ExitCode: -1, Output: out.String(), OutputBytes: out.Total(), OutputTruncated: out.Truncated(), PID: pid}, fmt.Errorf("%w: timeout", ErrCommandFailed)
	}
	code := cmd.ProcessState.ExitCode()
	result := CommandResult{ExitCode: code, Output: out.String(), OutputBytes: out.Total(), OutputTruncated: out.Truncated(), PID: pid}
	if err != nil {
		return result, fmt.Errorf("%w: exit %d: %w", ErrCommandFailed, code, err)
	}
	return result, nil
}

func (w *Workspace) RunPython(ctx context.Context, pythonPath string, code string, spec CommandSpec) (CommandResult, error) {
	if pythonPath == "" {
		pythonPath = "python3"
	}
	spec.Name = pythonPath
	spec.Args = []string{"-c", code}
	return w.RunCommand(ctx, spec)
}

type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int
	total     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.total += len(p)
	if b.limit <= 0 {
		return b.buf.Write(p)
	}
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) String() string {
	return b.buf.String()
}

func (b *limitedBuffer) Total() int {
	return b.total
}

func (b *limitedBuffer) Truncated() bool {
	return b.truncated
}
