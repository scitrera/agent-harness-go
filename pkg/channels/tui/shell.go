package tui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	"github.com/scitrera/agent-harness-go/pkg/procgroup"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	"github.com/scitrera/agent-harness-go/pkg/steering"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

const shellOutputLimit = 1 << 20 // 1 MiB enters the transcript at most.

type shellRun struct {
	ID              string
	WorkspaceID     string
	ThreadID        string
	CWD             string
	Command         string
	Output          string
	OutputBytes     int
	OutputTruncated bool
	ExitCode        int
	Err             error
}

type shellRunnerFunc func(context.Context, shellRun) shellRun

func (m model) startShellCommand(command string) (tea.Model, tea.Cmd) {
	if command == "" {
		m.addSystem("usage: !<shell command>")
		return m, nil
	}
	id, err := ids.New("msg-shell-")
	if err != nil {
		m.addSystem("could not create shell message id: " + err.Error())
		return m, nil
	}
	run := shellRun{
		ID:          id,
		WorkspaceID: m.workspaceID,
		ThreadID:    m.threadID,
		CWD:         m.currentWorkingDirectory(),
		Command:     command,
		ExitCode:    -1,
	}
	if run.CWD == "" {
		run.CWD = m.workspaceRoot
	}
	m.inputHistory = append(m.inputHistory, "!"+command)
	m.rows = append(m.rows, chatRow{
		Kind: rowShell, ID: id,
		Text: fmt.Sprintf("$ %s\n[running in %s]", command, run.CWD),
	})
	m.refreshViewportToBottom()
	runner := m.runShell
	if runner == nil {
		runner = runShellCommand
	}
	return m, func() tea.Msg { return shellResultMsg{Run: runner(m.ctx, run)} }
}

func (m model) finishShellCommand(msg shellResultMsg) (tea.Model, tea.Cmd) {
	run := msg.Run
	resolution := m.resolveShellPreference(run.WorkspaceID, run.ThreadID)
	record := shellcontext.Record{
		Command:         run.Command,
		CWD:             run.CWD,
		Output:          run.Output,
		ExitCode:        run.ExitCode,
		OutputBytes:     run.OutputBytes,
		OutputTruncated: run.OutputTruncated,
		TriggerAgent:    resolution.Effective,
	}
	if run.Err != nil {
		record.Error = run.Err.Error()
	}
	if m.workspaceMatchesCurrent(run.WorkspaceID) && m.threadID == run.ThreadID {
		m.updateShellRow(run.ID, shellcontext.DisplayText(record))
		m.refreshViewportToBottom()
	}

	part, err := protocol.NewTextPart(shellcontext.ContextText(record))
	if err != nil {
		m.addSystem("could not encode shell context: " + err.Error())
		return m, nil
	}
	message := protocol.ChatMessage{
		SchemaVersion: "1.0",
		ID:            run.ID,
		Role:          protocol.RoleUser,
		Addr: protocol.MessageAddress{
			WorkspaceID: run.WorkspaceID,
			ThreadID:    run.ThreadID,
		},
		Content: []protocol.ContentPart{part},
	}
	if err := shellcontext.Put(&message, record); err != nil {
		m.addSystem("could not encode shell context: " + err.Error())
		return m, nil
	}

	// Completion during a live turn is a steering message: it owns no task and
	// is appended at the running turn's next input boundary.
	if m.hasActiveTurnFor(run.WorkspaceID, run.ThreadID) {
		message = steering.Mark(message)
		m.status = m.activeTurnStatus()
		return m, sendShellSteeringCmd(m.ctx, m.channel, m.index, m.initialWorkspaceID, run, message)
	}

	// Idle sends use the resolved policy. Both paths traverse normal ingress;
	// TriggerAgent=false is intercepted by the runner after persistence.
	addr := message.Addr
	scoped := m
	scoped.workspaceID = run.WorkspaceID
	scoped.cwd = run.CWD
	if err := scoped.scopeMessage(&addr, &message); err != nil {
		m.addSystem("could not bind shell context: " + err.Error())
		return m, nil
	}
	message.Addr.ThreadID = run.ThreadID
	pending := queuedMessage{
		Kind:        outboundShell,
		WorkspaceID: run.WorkspaceID,
		ThreadID:    run.ThreadID,
		Input:       "!" + run.Command,
		DisplayText: "!" + run.Command,
		Message:     message,
		Presented:   true,
	}
	return m, m.dispatchPreparedMessage(pending)
}

func (m *model) updateShellRow(id, text string) {
	for i := range m.rows {
		if m.rows[i].Kind == rowShell && m.rows[i].ID == id {
			m.rows[i].Text = text
			return
		}
	}
	m.rows = append(m.rows, chatRow{Kind: rowShell, ID: id, Text: text})
}

func (m model) resolveShellPreference(workspaceID, threadID string) ShellPreferenceResolution {
	if m.shellPreferences == nil {
		return ShellPreferenceResolution{Effective: m.shellTriggerAgent, Global: m.shellTriggerAgent, Source: "global"}
	}
	return m.shellPreferences.Resolve(m.userID, workspaceID, threadID, m.shellTriggerAgent)
}

func sendShellSteeringCmd(
	ctx context.Context,
	ch ChannelSurface,
	index threadindex.Store,
	initialWorkspaceID string,
	run shellRun,
	message protocol.ChatMessage,
) tea.Cmd {
	return func() tea.Msg {
		if err := touchWorkspaceThread(index, initialWorkspaceID, run.WorkspaceID, run.ThreadID, "!"+run.Command); err != nil {
			return sendResultMsg{WorkspaceID: run.WorkspaceID, Err: fmt.Errorf("touch thread: %w", err)}
		}
		if err := ch.Enqueue(ctx, channel.Inbound{Addr: message.Addr, Message: message}); err != nil {
			return sendResultMsg{WorkspaceID: run.WorkspaceID, Err: fmt.Errorf("steer shell context: %w", err)}
		}
		return sendResultMsg{WorkspaceID: run.WorkspaceID}
	}
}

func runShellCommand(ctx context.Context, run shellRun) shellRun {
	cmd := exec.CommandContext(ctx, "/bin/bash", "-lc", run.Command)
	cmd.Dir = run.CWD
	cmd.Env = os.Environ()
	cmd.SysProcAttr = procgroup.Attr()
	cmd.Cancel = func() error { return procgroup.Kill(cmd.Process) }
	var output boundedShellBuffer
	output.limit = shellOutputLimit
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	run.Output = output.String()
	run.OutputBytes = output.Total()
	run.OutputTruncated = output.Truncated()
	if cmd.ProcessState != nil {
		run.ExitCode = cmd.ProcessState.ExitCode()
	}
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			run.Err = ctx.Err()
		} else {
			run.Err = err
		}
	}
	return run
}

type boundedShellBuffer struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	limit     int
	total     int
	truncated bool
}

func (b *boundedShellBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total += len(p)
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		_, _ = b.buf.Write(p[:remaining])
	}
	if remaining < len(p) {
		b.truncated = true
	}
	return len(p), nil
}

func (b *boundedShellBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *boundedShellBuffer) Total() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total
}

func (b *boundedShellBuffer) Truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.truncated
}
