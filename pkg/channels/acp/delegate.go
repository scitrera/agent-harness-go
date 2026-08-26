package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// This file wires ACP client-delegation: when the client advertised fs/terminal
// capabilities, TurnContext attaches per-session delegates so the harness's
// read_file/write_file/edit_file and shell/python tools route THROUGH the client
// (into the editor) instead of the local workspace. acp imports pkg/tools +
// pkg/localtools (neither imports acp, so no cycle).

// ─── outbound fs/terminal RPCs (one per method, for a given session) ─────

// readTextFile issues fs/read_text_file for an ABSOLUTE path and returns the
// client's content.
func (c *Channel) readTextFile(ctx context.Context, sessionID, path string, maxBytes int64) (string, error) {
	raw, err := c.conn.call(ctx, methodReadTextFile, readTextFileParams{SessionID: sessionID, Path: path})
	if err != nil {
		return "", err
	}
	var res readTextFileResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return "", fmt.Errorf("acp: decode read_text_file result: %w", err)
	}
	// The client returns the whole file; honor the tool's byte cap locally.
	if maxBytes > 0 && int64(len(res.Content)) > maxBytes {
		return res.Content[:maxBytes], nil
	}
	return res.Content, nil
}

// writeTextFile issues fs/write_text_file for an ABSOLUTE path (null result).
func (c *Channel) writeTextFile(ctx context.Context, sessionID, path, content string) error {
	_, err := c.conn.call(ctx, methodWriteTextFile, writeTextFileParams{SessionID: sessionID, Path: path, Content: content})
	return err
}

// terminalCreate issues terminal/create and returns the terminal id.
func (c *Channel) terminalCreate(ctx context.Context, sessionID string, p terminalCreateParams) (string, error) {
	p.SessionID = sessionID
	raw, err := c.conn.call(ctx, methodTerminalCreate, p)
	if err != nil {
		return "", err
	}
	var res terminalCreateResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return "", fmt.Errorf("acp: decode terminal/create result: %w", err)
	}
	return res.TerminalID, nil
}

// terminalWaitForExit blocks (via the client) until the terminal command exits.
func (c *Channel) terminalWaitForExit(ctx context.Context, sessionID, terminalID string) (terminalWaitForExitResult, error) {
	raw, err := c.conn.call(ctx, methodTerminalWaitExit, terminalRefParams{SessionID: sessionID, TerminalID: terminalID})
	if err != nil {
		return terminalWaitForExitResult{}, err
	}
	var res terminalWaitForExitResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return terminalWaitForExitResult{}, fmt.Errorf("acp: decode terminal/wait_for_exit result: %w", err)
	}
	return res, nil
}

// terminalOutput fetches the terminal's captured output (and exit status).
func (c *Channel) terminalOutput(ctx context.Context, sessionID, terminalID string) (terminalOutputResult, error) {
	raw, err := c.conn.call(ctx, methodTerminalOutput, terminalRefParams{SessionID: sessionID, TerminalID: terminalID})
	if err != nil {
		return terminalOutputResult{}, err
	}
	var res terminalOutputResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return terminalOutputResult{}, fmt.Errorf("acp: decode terminal/output result: %w", err)
	}
	return res, nil
}

// terminalRelease frees the terminal's client-side resources. Best-effort.
func (c *Channel) terminalRelease(ctx context.Context, sessionID, terminalID string) {
	_, _ = c.conn.call(ctx, methodTerminalRelease, terminalRefParams{SessionID: sessionID, TerminalID: terminalID})
}

// ─── tools.FileDelegate: route file tools through the client's fs/* ──────

// fileDelegate proxies read_file/write_file/edit_file to a session's ACP client.
// Paths from the tools are workspace-relative; ACP fs paths are ABSOLUTE, so a
// relative path is resolved against the session cwd first.
type fileDelegate struct {
	ch        *Channel
	sessionID string
	cwd       string
}

func (d *fileDelegate) resolve(path string) string {
	if filepath.IsAbs(path) || d.cwd == "" {
		return path
	}
	return filepath.Join(d.cwd, path)
}

func (d *fileDelegate) ReadFile(ctx context.Context, path string, maxBytes int64) (string, error) {
	return d.ch.readTextFile(ctx, d.sessionID, d.resolve(path), maxBytes)
}

func (d *fileDelegate) WriteFile(ctx context.Context, path, content string) error {
	return d.ch.writeTextFile(ctx, d.sessionID, d.resolve(path), content)
}

// EditFile mirrors localtools.EditFile over the client: read, require exactly
// one match, then write the replacement back.
func (d *fileDelegate) EditFile(ctx context.Context, path, oldText, newText string) error {
	if oldText == "" {
		return fmt.Errorf("%w: old text must not be empty", localtools.ErrInvalidFile)
	}
	abs := d.resolve(path)
	current, err := d.ch.readTextFile(ctx, d.sessionID, abs, 0)
	if err != nil {
		return err
	}
	switch strings.Count(current, oldText) {
	case 0:
		return localtools.ErrOldTextNotFound
	case 1:
	default:
		return localtools.ErrOldTextNotUnique
	}
	updated := strings.Replace(current, oldText, newText, 1)
	return d.ch.writeTextFile(ctx, d.sessionID, abs, updated)
}

// ─── tools.CommandDelegate: route shell/python through terminal/* ────────

// commandDelegate proxies shell/python to a session's ACP client terminal. The
// command policy is enforced by the tool handler before this is called.
type commandDelegate struct {
	ch        *Channel
	sessionID string
}

// RunCommand drives the terminal lifecycle: create -> wait_for_exit -> output ->
// release, adapting the result into a localtools.CommandResult. A transport
// error at any step surfaces as the returned error.
func (d *commandDelegate) RunCommand(ctx context.Context, spec localtools.CommandSpec) (localtools.CommandResult, error) {
	terminalID, err := d.ch.terminalCreate(ctx, d.sessionID, terminalCreateParams{
		Command:         spec.Name,
		Args:            spec.Args,
		Cwd:             spec.CWD,
		Env:             envVarsFrom(spec.Env),
		OutputByteLimit: spec.MaxOutput,
	})
	if err != nil {
		return localtools.CommandResult{}, err
	}
	defer d.ch.terminalRelease(context.WithoutCancel(ctx), d.sessionID, terminalID)

	exit, err := d.ch.terminalWaitForExit(ctx, d.sessionID, terminalID)
	if err != nil {
		return localtools.CommandResult{}, err
	}
	out, err := d.ch.terminalOutput(ctx, d.sessionID, terminalID)
	if err != nil {
		return localtools.CommandResult{}, err
	}
	code := 0
	if exit.ExitCode != nil {
		code = *exit.ExitCode
	} else if exit.Signal != "" {
		code = -1 // terminated by signal: no numeric code
	}
	return localtools.CommandResult{
		ExitCode:        code,
		Output:          out.Output,
		OutputBytes:     len(out.Output),
		OutputTruncated: out.Truncated,
	}, nil
}

// envVarsFrom converts "KEY=VALUE" env entries into ACP env pairs.
func envVarsFrom(env []string) []envVar {
	if len(env) == 0 {
		return nil
	}
	out := make([]envVar, 0, len(env))
	for _, e := range env {
		name, value, _ := strings.Cut(e, "=")
		out = append(out, envVar{Name: name, Value: value})
	}
	return out
}

// ─── the turn.Config.ContextDecorator seam ───────────────────────────────

// TurnContext decorates a turn's ctx with fs/terminal client delegates when the
// turn's thread maps to a known ACP session and the client advertised the
// matching capability. A sub-agent thread (not in the session index) is not
// found, so ctx is returned unchanged and sub-agents keep using the local
// workspace. Wired as turn.Config.ContextDecorator via buildRunner.
func (c *Channel) TurnContext(ctx context.Context, addr protocol.MessageAddress) context.Context {
	sess := c.sessionByThread(addr.ThreadID)
	if sess == nil {
		return ctx
	}
	c.mu.Lock()
	caps := c.caps
	c.mu.Unlock()
	if caps.Fs.ReadTextFile && caps.Fs.WriteTextFile {
		ctx = tools.WithFileDelegate(ctx, &fileDelegate{ch: c, sessionID: sess.id, cwd: sess.cwd})
	}
	if caps.Terminal {
		ctx = tools.WithCommandDelegate(ctx, &commandDelegate{ch: c, sessionID: sess.id})
	}
	return ctx
}

var (
	_ tools.FileDelegate    = (*fileDelegate)(nil)
	_ tools.CommandDelegate = (*commandDelegate)(nil)
)
