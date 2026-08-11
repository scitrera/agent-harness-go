package tui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/taskstate"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

type fakeApprovalResolver struct {
	taskID    string
	requestID string
	decision  approval.Decision
	ok        bool
}

func (f *fakeApprovalResolver) Resolve(taskID, requestID string, decision approval.Decision) bool {
	f.taskID = taskID
	f.requestID = requestID
	f.decision = decision
	return f.ok
}

func TestModelResolveApproval_whenRequestPending(t *testing.T) {
	// Given
	resolver := &fakeApprovalResolver{ok: true}
	m := model{
		approvals:        resolver,
		pendingApprovals: map[string]approvalRequest{"call-1": {RequestID: "call-1", TaskID: "task-1", Tool: "shell", Status: "pending"}},
		viewport:         viewport.New(),
	}

	// When
	m.resolveApproval([]string{"/approve", "call-1", "session"}, true)

	// Then
	if resolver.taskID != "task-1" || resolver.requestID != "call-1" || !resolver.decision.Granted || resolver.decision.Scope != "session" {
		t.Fatalf("decision mismatch: %+v", resolver)
	}
	if len(m.pendingApprovals) != 0 {
		t.Fatalf("approval not removed: %+v", m.pendingApprovals)
	}
}

func TestModelCommandsRoundTripWithoutConversationPlaceholder(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
	}{
		{input: "/model", want: "/model"},
		{input: "/models", want: "/model"},
		{input: "/schedules", want: "/schedules"},
		{input: "/runs --status failed --limit 10", want: "/runs --status failed --limit 10"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			ctx := context.Background()
			ch := NewChannel()
			index, err := threadindex.NewIndex(t.TempDir(), nil)
			if err != nil {
				t.Fatalf("new index: %v", err)
			}
			if err := index.Touch("thread-1", "existing thread"); err != nil {
				t.Fatalf("touch thread: %v", err)
			}
			m := model{
				ctx:      ctx,
				channel:  ch,
				index:    index,
				threadID: "thread-1",
				turns:    map[string]turnActivity{},
				viewport: viewport.New(),
			}

			next, cmd := m.handleSlash(tt.input)
			if cmd == nil {
				t.Fatal("model command should be sent to the remote runner")
			}
			updated := next.(model)
			if len(updated.rows) != 0 || updated.hasThinking() {
				t.Fatalf("model command rendered as a conversation turn: %+v", updated.rows)
			}
			result, ok := cmd().(sendResultMsg)
			if !ok || result.Err != nil {
				t.Fatalf("send result = %#v", result)
			}
			in, err := ch.FetchTask(ctx)
			if err != nil {
				t.Fatalf("fetch command: %v", err)
			}
			if got := messageText(in.Message); got != tt.want {
				t.Fatalf("command payload = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestModelHandlePermissions_approveAliasResolvesRequest(t *testing.T) {
	// Given
	resolver := &fakeApprovalResolver{ok: true}
	m := model{
		approvals:        resolver,
		pendingApprovals: map[string]approvalRequest{"call-1": {RequestID: "call-1", TaskID: "task-1", Tool: "shell", Status: "pending"}},
		viewport:         viewport.New(),
	}

	// When
	_, cmd := m.handlePermissions([]string{"/permissions", "approve", "call-1", "always"})

	// Then
	if cmd != nil {
		t.Fatal("permission approval should resolve synchronously")
	}
	if resolver.taskID != "task-1" || resolver.requestID != "call-1" || !resolver.decision.Granted || resolver.decision.Scope != "always" {
		t.Fatalf("decision mismatch: %+v", resolver)
	}
}

func TestModelHandleSlash_clearClearsCurrentThreadText(t *testing.T) {
	// Given
	ctx := context.Background()
	stateDir := t.TempDir()
	history := store.NewFileStore("", stateDir)
	index, err := threadindex.NewIndex(stateDir, nil)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	if err := index.Touch("thread-1", "old transcript"); err != nil {
		t.Fatalf("touch thread: %v", err)
	}
	part, err := protocol.NewTextPart("old visible line")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	seed := []protocol.ChatMessage{{
		ID:      "user-1",
		Role:    protocol.RoleUser,
		Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Content: []protocol.ContentPart{part},
	}}
	if err := history.SaveHistory(ctx, "thread-1", seed); err != nil {
		t.Fatalf("save history: %v", err)
	}
	m := model{
		ctx:      ctx,
		channel:  NewChannel(),
		store:    history,
		index:    index,
		threadID: "thread-1",
		threads:  index.List(),
		rows:     rowsFromHistory(seed),
		viewport: viewport.New(),
		tailing:  true,
	}
	m.viewport.SetWidth(80)
	m.viewport.SetHeight(4)
	m.refreshViewportToBottom()
	if !strings.Contains(m.viewport.View(), "old visible line") {
		t.Fatal("test setup should render old transcript text")
	}

	// When
	next, cmd := m.handleSlash("/clear")
	if cmd != nil {
		t.Fatal("clear should require confirmation before emitting a command")
	}
	updated, ok := next.(model)
	if !ok {
		t.Fatalf("expected model, got %T", next)
	}
	if !updated.confirmation.active() {
		t.Fatal("clear confirmation was not opened")
	}
	next, cmd = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: 'y', Text: "y"}))
	if cmd == nil {
		t.Fatal("expected clear command after confirmation")
	}
	msg := cmd()
	clearMsg, ok := msg.(clearThreadMsg)
	if !ok {
		t.Fatalf("expected clear thread message, got %T", msg)
	}
	updated, ok = next.(model)
	if !ok {
		t.Fatalf("expected model, got %T", next)
	}
	updated.applyClearThread(clearMsg)

	// Then
	got, err := history.LoadHistory(ctx, "thread-1")
	if err != nil {
		t.Fatalf("load history: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected persisted history cleared, got %d messages", len(got))
	}
	if strings.Contains(updated.viewport.View(), "old visible line") {
		t.Fatalf("viewport still contains cleared transcript: %q", updated.viewport.View())
	}
	if !strings.Contains(updated.viewport.View(), "cleared thread-1") {
		t.Fatalf("viewport missing clear confirmation: %q", updated.viewport.View())
	}
}

func TestClearDefersFirstTurnUntilClearResult(t *testing.T) {
	ctx := context.Background()
	stateDir := t.TempDir()
	history := store.NewFileStore("", stateDir)
	index, err := threadindex.NewIndex(stateDir, nil)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	if err := index.Touch("thread-1", "old transcript"); err != nil {
		t.Fatalf("touch thread: %v", err)
	}
	oldPart, err := protocol.NewTextPart("old visible line")
	if err != nil {
		t.Fatalf("old text part: %v", err)
	}
	oldRows := rowsFromHistory([]protocol.ChatMessage{{
		ID:      "user-old",
		Role:    protocol.RoleUser,
		Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-old"},
		Content: []protocol.ContentPart{oldPart},
	}})
	m := model{
		ctx:              ctx,
		channel:          NewChannel(),
		store:            history,
		index:            index,
		threadID:         "thread-1",
		threads:          index.List(),
		rows:             oldRows,
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		turns:            map[string]turnActivity{},
		renderedRows:     map[string]renderedRowCache{},
		clearingThreads:  map[string]struct{}{},
		viewport:         viewport.New(),
		composer:         newComposer(),
		tailing:          true,
	}
	m.resize(60, 20)

	next, _ := m.handleSlash("/clear")
	confirming := next.(model)
	next, clearCmd := confirming.updateKey(tea.KeyPressMsg(tea.Key{Code: 'y', Text: "y"}))
	if clearCmd == nil {
		t.Fatal("confirmed clear should emit a clear command")
	}
	clearing := next.(model)
	clearing.composer.SetValue("first question after clear")

	next, prematureSend := clearing.sendCurrent()
	if prematureSend != nil {
		t.Fatal("first turn should wait until clear persistence completes")
	}
	waiting := next.(model)
	if got := waiting.composer.Value(); got != "first question after clear" {
		t.Fatalf("deferred composer value = %q", got)
	}
	if waiting.deferredSendFor != "thread-1" {
		t.Fatalf("deferred send thread = %q", waiting.deferredSendFor)
	}

	clearResult := clearCmd().(clearThreadMsg)
	next, sendCmd := waiting.Update(clearResult)
	if sendCmd == nil {
		t.Fatal("successful clear should release the deferred first turn")
	}
	updated := next.(model)
	if len(updated.rows) != 3 ||
		updated.rows[0].Kind != rowSystem ||
		updated.rows[1].Kind != rowUser ||
		updated.rows[2].Kind != rowThinking {
		t.Fatalf("rows after clear + deferred send = %+v", updated.rows)
	}
	if got := updated.rows[1].Text; got != "first question after clear" {
		t.Fatalf("first user row = %q", got)
	}

	updated.applyEvent(channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      protocol.MessageAddress{ThreadID: "thread-1", TaskID: updated.lastTaskID},
		MessageID: "assistant-first",
		Index:     0,
		Delta:     "first answer",
	})
	if len(updated.rows) != 3 || updated.rows[1].Kind != rowUser || updated.rows[1].Text != "first question after clear" {
		t.Fatalf("assistant stream replaced first user row: %+v", updated.rows)
	}
	if updated.rows[2].Kind != rowAssistant || updated.rows[2].Text != "first answer" {
		t.Fatalf("assistant row = %+v", updated.rows[2])
	}
}

func TestModelHandleTasks_planCreateLoadsDrawer(t *testing.T) {
	// Given
	store := taskstate.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	m := model{ctx: context.Background(), taskStore: store, viewport: viewport.New()}

	// When
	_, cmd := m.handleTasks([]string{"/tasks", "plan", "create", "plan-1", "Build TUI"})
	if cmd == nil {
		t.Fatal("expected task command")
	}
	msg, ok := cmd().(drawerLoadedMsg)
	if !ok {
		t.Fatalf("unexpected message type %T", cmd())
	}

	// Then
	if msg.Err != nil {
		t.Fatalf("task command failed: %v", msg.Err)
	}
	if msg.Drawer != drawerTasks || !strings.Contains(msg.Text, "plan plan-1: Build TUI") {
		t.Fatalf("drawer message mismatch: %+v", msg)
	}
}
