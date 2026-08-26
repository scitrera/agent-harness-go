package tui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	"github.com/scitrera/agent-harness-go/pkg/steering"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func shellTestModel(t *testing.T, trigger bool) (model, *Channel) {
	t.Helper()
	ctx := context.Background()
	ch := NewChannel()
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	m := model{
		ctx: ctx, channel: ch, index: index,
		workspaceID: "ws", initialWorkspaceID: "ws", threadID: "thread-1",
		workspaceRoot: root, cwd: root, shellTriggerAgent: trigger,
		viewport: viewport.New(), composer: newComposer(), turns: map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{}, tools: map[string]toolEntry{}, tailing: true,
	}
	m.resize(80, 20)
	return m, ch
}

func TestShellResultWhenIdleCanCommitContextWithoutAgentResponse(t *testing.T) {
	m, ch := shellTestModel(t, false)
	run := shellRun{ID: "msg-shell-1", WorkspaceID: "ws", ThreadID: "thread-1", CWD: m.cwd, Command: "pwd", Output: m.cwd + "\n", OutputBytes: len(m.cwd) + 1, ExitCode: 0}
	m.rows = append(m.rows, chatRow{Kind: rowShell, ID: run.ID, Text: "running"})

	next, send := m.finishShellCommand(shellResultMsg{Run: run})
	m = next.(model)
	if send == nil {
		t.Fatal("shell result did not dispatch context commit")
	}
	if result := send().(sendResultMsg); result.Err != nil {
		t.Fatal(result.Err)
	}
	inbound, err := ch.FetchTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !shellcontext.IsContextOnly(inbound.Message) {
		t.Fatalf("message should be context-only: %#v", inbound.Message)
	}
	if inbound.Addr.TaskID == "" {
		t.Fatal("context commit should have task identity for completion tracking")
	}
	if got := m.turns[inbound.Addr.TaskID].Phase; got != "saving context" {
		t.Fatalf("phase = %q", got)
	}
	if strings.Contains(m.renderRows(), "Thinking") {
		t.Fatalf("context-only shell showed model activity: %q", m.renderRows())
	}
}

func TestShellResultDuringTurnIsSteering(t *testing.T) {
	m, ch := shellTestModel(t, false)
	m.turns["task-active"] = turnActivity{WorkspaceID: "ws", ThreadID: "thread-1", Phase: "tool read_file"}
	run := shellRun{ID: "msg-shell-2", WorkspaceID: "ws", ThreadID: "thread-1", CWD: m.cwd, Command: "git status", Output: "clean\n", OutputBytes: 6, ExitCode: 0}
	m.rows = append(m.rows, chatRow{Kind: rowShell, ID: run.ID, Text: "running"})

	next, send := m.finishShellCommand(shellResultMsg{Run: run})
	_ = next.(model)
	if result := send().(sendResultMsg); result.Err != nil {
		t.Fatal(result.Err)
	}
	inbound, err := ch.FetchTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !steering.IsRequested(inbound.Message) {
		t.Fatalf("message was not steering: %#v", inbound.Message.Meta)
	}
	if inbound.Addr.TaskID != "" {
		t.Fatalf("steering should not own a task: %q", inbound.Addr.TaskID)
	}
	if !shellcontext.IsContextOnly(inbound.Message) {
		t.Fatal("idle preference should remain available for a missed-steering fallback")
	}
}

func TestRunShellCommandUsesBashAndWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	run := runShellCommand(context.Background(), shellRun{CWD: dir, Command: "printf '%s' \"$PWD\"; printf err >&2", ExitCode: -1})
	if run.Err != nil || run.ExitCode != 0 {
		t.Fatalf("run = %#v", run)
	}
	if !strings.Contains(run.Output, filepath.Clean(dir)) || !strings.Contains(run.Output, "err") {
		t.Fatalf("output = %q", run.Output)
	}
}

func TestBoundedShellBufferCapsTranscriptOutput(t *testing.T) {
	var buffer boundedShellBuffer
	buffer.limit = 4
	_, _ = buffer.Write([]byte("abcdef"))
	if got := buffer.String(); got != "abcd" {
		t.Fatalf("output = %q", got)
	}
	if !buffer.Truncated() || buffer.Total() != 6 {
		t.Fatalf("truncated=%v total=%d", buffer.Truncated(), buffer.Total())
	}
}

func TestShellContextCommitAcknowledgementDoesNotShowThinking(t *testing.T) {
	m, _ := shellTestModel(t, false)
	ack := protocol.ChatMessage{Role: protocol.RoleAssistant, Addr: protocol.MessageAddress{WorkspaceID: "ws", ThreadID: "thread-1", TaskID: "task-save"}}
	shellcontext.MarkCommitAck(&ack)
	m.applyEvent(channel.Event{Type: channel.EventMessageStarted, Addr: ack.Addr, Message: &ack})
	if got := m.turns["task-save"].Phase; got != "saving context" {
		t.Fatalf("phase = %q", got)
	}
	if m.hasThinking() {
		t.Fatal("context commit acknowledgement showed Thinking")
	}
}
