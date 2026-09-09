// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package localtools

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/procgroup"
)

type CommandSpec struct {
	Name      string
	Args      []string
	CWD       string
	Env       []string
	Timeout   time.Duration
	MaxOutput int
	// ArchiveLimit bounds the bytes retained BEYOND MaxOutput for the recovery
	// archive, so output the model never sees can still be written somewhere it
	// can read back. <=0 retains nothing (Archive stays empty). When the command
	// produces more than this, the retained text keeps a head and a tail window
	// with an explicit middle-gap marker — a build's error is usually at the end,
	// so a head-only archive would drop the one part worth keeping.
	ArchiveLimit int
}

type CommandResult struct {
	ExitCode        int
	Output          string
	OutputBytes     int
	OutputTruncated bool
	PID             int
	// Archive is the retained output for the recovery archive: up to
	// ArchiveLimit bytes as head+tail windows separated by a gap marker. Empty
	// when nothing was truncated or ArchiveLimit was <=0. It is a superset of
	// Output, not a replacement — callers write it to durable storage and hand
	// the model a read-back reference.
	Archive string
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
	cmd.SysProcAttr = procgroup.Attr()

	var out limitedBuffer
	out.limit = spec.MaxOutput
	out.archiveLimit = spec.ArchiveLimit
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		return CommandResult{}, fmt.Errorf("%w: start command: %w", ErrCommandFailed, err)
	}
	pid := cmd.Process.Pid
	err = cmd.Wait()
	if runCtx.Err() == context.DeadlineExceeded {
		_ = procgroup.Kill(cmd.Process)
		return CommandResult{ExitCode: -1, Output: out.String(), OutputBytes: out.Total(), OutputTruncated: out.Truncated(), Archive: out.Archive(), PID: pid}, fmt.Errorf("%w: timeout", ErrCommandFailed)
	}
	code := cmd.ProcessState.ExitCode()
	result := CommandResult{ExitCode: code, Output: out.String(), OutputBytes: out.Total(), OutputTruncated: out.Truncated(), Archive: out.Archive(), PID: pid}
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

	// archiveLimit/head/tail retain output past `limit` for the recovery
	// archive. head fills first; once it is full every later byte goes to the
	// tail ring, so the archive keeps the beginning and the end of a long run
	// and loses only the middle.
	archiveLimit int
	head         bytes.Buffer
	tail         []byte
	tailStart    int // ring write cursor once tail is at capacity
	tailFull     bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.total += len(p)
	b.archive(p)
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

// archive retains p across the head buffer and the tail ring, both capped at
// half of archiveLimit. It never allocates beyond archiveLimit regardless of how
// much the command writes.
func (b *limitedBuffer) archive(p []byte) {
	if b.archiveLimit <= 0 {
		return
	}
	half := b.archiveLimit / 2
	if half < 1 {
		half = 1
	}
	if room := half - b.head.Len(); room > 0 {
		if len(p) <= room {
			_, _ = b.head.Write(p)
			return
		}
		_, _ = b.head.Write(p[:room])
		p = p[room:]
	}
	if b.tail == nil {
		b.tail = make([]byte, 0, half)
	}
	// Only the last `half` bytes of p can survive in the ring.
	if len(p) > half {
		p = p[len(p)-half:]
	}
	for _, c := range p {
		if !b.tailFull {
			b.tail = append(b.tail, c)
			if len(b.tail) == half {
				b.tailFull = true
				b.tailStart = 0
			}
			continue
		}
		b.tail[b.tailStart] = c
		b.tailStart = (b.tailStart + 1) % half
	}
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

// Archive returns the retained head+tail text. When the middle was dropped the
// two windows are separated by an explicit marker, so a reader is never misled
// into thinking the archive is contiguous. Empty when nothing was truncated:
// the visible output is already the whole story.
func (b *limitedBuffer) Archive() string {
	if b.archiveLimit <= 0 || !b.truncated {
		return ""
	}
	head := b.head.String()
	tail := b.tailBytes()
	if len(tail) == 0 {
		return head
	}
	dropped := b.total - len(head) - len(tail)
	if dropped <= 0 {
		return head + string(tail)
	}
	return head + fmt.Sprintf("\n[... %d bytes of output dropped ...]\n", dropped) + string(tail)
}

// tailBytes unrolls the tail ring into write order.
func (b *limitedBuffer) tailBytes() []byte {
	if !b.tailFull {
		return b.tail
	}
	out := make([]byte, 0, len(b.tail))
	out = append(out, b.tail[b.tailStart:]...)
	return append(out, b.tail[:b.tailStart]...)
}
