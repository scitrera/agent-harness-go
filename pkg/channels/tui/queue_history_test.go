package tui

import (
	"context"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func historyTestModel(history ...string) model {
	m := model{
		viewport:     viewport.New(),
		composer:     newComposer(),
		inputHistory: append([]string(nil), history...),
		tailing:      true,
	}
	m.resize(80, 20)
	return m
}

func TestComposerArrowsNavigateMultilineBeforeOutgoingHistory(t *testing.T) {
	m := historyTestModel("older message", "newest message")
	m.composer.SetValue("draft line one\ndraft line two")
	m.composer.MoveToEnd()

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	updated := next.(model)
	if got := updated.composer.Value(); got != "draft line one\ndraft line two" {
		t.Fatalf("first Up replaced multiline draft: %q", got)
	}
	if got := updated.composer.Line(); got != 0 {
		t.Fatalf("first Up cursor line = %d, want first draft line", got)
	}

	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	updated = next.(model)
	if got := updated.composer.Value(); got != "newest message" {
		t.Fatalf("boundary Up recalled %q, want newest message", got)
	}

	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	updated = next.(model)
	if got := updated.composer.Value(); got != "draft line one\ndraft line two" {
		t.Fatalf("Down past newest history restored %q", got)
	}
}

func TestEmptyComposerUpRecallsLastOutgoingMessage(t *testing.T) {
	m := historyTestModel("first", "last outgoing")

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	updated := next.(model)
	if got := updated.composer.Value(); got != "last outgoing" {
		t.Fatalf("recalled value = %q", got)
	}

	next, _ = updated.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	updated = next.(model)
	if got := updated.composer.Value(); got != "first" {
		t.Fatalf("older recalled value = %q", got)
	}
}

func TestAdditionalMessagesStayLocalUntilActiveTurnFinishes(t *testing.T) {
	ctx := context.Background()
	ch := NewChannel()
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatal(err)
	}
	m := model{
		ctx: ctx, channel: ch, index: index, threadID: "thread-1",
		viewport: viewport.New(), composer: newComposer(), turns: map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{}, tools: map[string]toolEntry{}, tailing: true,
	}
	m.resize(80, 20)
	m.composer.SetValue("first")
	next, firstCmd := m.sendCurrent()
	if firstCmd == nil {
		t.Fatal("first message did not dispatch")
	}
	m = next.(model)
	firstTaskID := m.lastTaskID
	if result := firstCmd().(sendResultMsg); result.Err != nil {
		t.Fatal(result.Err)
	}

	m.composer.SetValue("second")
	next, secondCmd := m.sendCurrent()
	if secondCmd != nil {
		t.Fatal("second message dispatched while first turn was active")
	}
	m = next.(model)
	if m.lastTaskID != firstTaskID {
		t.Fatalf("active task changed from %q to %q", firstTaskID, m.lastTaskID)
	}
	if len(m.pendingMessages) != 1 || m.pendingMessages[0].Input != "second" {
		t.Fatalf("pending messages = %+v", m.pendingMessages)
	}
	if got := len(ch.inbox); got != 1 {
		t.Fatalf("enqueued tasks = %d, want only the active task", got)
	}
	if rendered := m.renderPendingMessages(); !strings.Contains(rendered, "queued> second") {
		t.Fatalf("pending surface = %q", rendered)
	}

	m.applyEvent(channel.Event{
		Type: channel.EventMessageFinal,
		Addr: protocol.MessageAddress{ThreadID: "thread-1", TaskID: firstTaskID},
	})
	nextCmd := m.dispatchNextPending()
	if nextCmd == nil {
		t.Fatal("terminal event did not release the next pending message")
	}
	if len(m.pendingMessages) != 0 {
		t.Fatalf("pending queue was not drained: %+v", m.pendingMessages)
	}
	if m.lastTaskID == firstTaskID {
		t.Fatal("next dispatch reused the completed task ID")
	}
	if result := nextCmd().(sendResultMsg); result.Err != nil {
		t.Fatal(result.Err)
	}
	if got := len(ch.inbox); got != 2 {
		t.Fatalf("enqueued tasks after completion = %d, want 2", got)
	}
}

func TestQueuedMessageCanBeRecalledEditedOrRemoved(t *testing.T) {
	m := historyTestModel("active")
	m.threadID = "thread-1"
	m.turns = map[string]turnActivity{"task-1": {ThreadID: "thread-1", Phase: "thinking"}}
	pending, err := m.prepareQueuedMessage(outboundConversation, "queued draft", nil)
	if err != nil {
		t.Fatal(err)
	}
	m.queuePreparedMessage(pending)

	next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	m = next.(model)
	if m.editingPendingID == 0 || m.composer.Value() != "queued draft" {
		t.Fatalf("queued message was not selected for edit: id=%d value=%q", m.editingPendingID, m.composer.Value())
	}
	m.composer.SetValue("edited draft")
	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if cmd != nil {
		t.Fatal("editing a pending message dispatched it through the active turn")
	}
	m = next.(model)
	if len(m.pendingMessages) != 1 || m.pendingMessages[0].Input != "edited draft" {
		t.Fatalf("edited queue = %+v", m.pendingMessages)
	}

	next, _ = m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyUp}))
	m = next.(model)
	m.composer.Reset()
	next, cmd = m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if cmd != nil {
		t.Fatal("removing a pending message emitted a command")
	}
	m = next.(model)
	if len(m.pendingMessages) != 0 {
		t.Fatalf("cleared queued message remained: %+v", m.pendingMessages)
	}
}

func TestQueuedSurfaceKeepsStatusAndCursorInsideTerminal(t *testing.T) {
	m := historyTestModel()
	m.threadID = "thread-1"
	m.turns = map[string]turnActivity{"task-1": {ThreadID: "thread-1", Phase: "thinking"}}
	for _, text := range []string{"queued one", "queued two"} {
		pending, err := m.prepareQueuedMessage(outboundConversation, text, nil)
		if err != nil {
			t.Fatal(err)
		}
		m.queuePreparedMessage(pending)
	}

	view := m.View()
	if got := lipgloss.Height(m.render()); got != m.height {
		t.Fatalf("rendered height = %d, want %d", got, m.height)
	}
	if view.Cursor == nil || view.Cursor.Y >= m.height-1 {
		t.Fatalf("composer cursor escaped input area: %+v", view.Cursor)
	}
	if rendered := m.renderPendingMessages(); !strings.Contains(rendered, "queued> queued one") || !strings.Contains(rendered, "queued> queued two") {
		t.Fatalf("pending surface = %q", rendered)
	}
}

func TestRepeatedEscapeDoesNotReportQueuedTaskAsNotCancellable(t *testing.T) {
	canceller := &recordingCanceller{ok: false}
	m := model{
		threadID: "thread-1", lastTaskID: "task-1", canceller: canceller,
		turns:    map[string]turnActivity{"task-1": {ThreadID: "thread-1", Phase: "queued"}},
		viewport: viewport.New(), composer: newComposer(),
	}
	m.addThinking("task-1")

	for range 5 {
		next, _ := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
		m = next.(model)
	}
	for _, row := range m.rows {
		if strings.Contains(row.Text, "task not cancellable") {
			t.Fatalf("repeated Escape emitted stale cancellation error: %+v", m.rows)
		}
	}
	if m.cancelPendingID != "task-1" {
		t.Fatalf("pending cancellation = %q", m.cancelPendingID)
	}
}

func TestPendingEscapeCancellationRetriesAndThenReleasesQueue(t *testing.T) {
	canceller := &recordingCanceller{ok: false}
	m := historyTestModel("active")
	m.threadID = "thread-1"
	m.lastTaskID = "task-1"
	m.canceller = canceller
	m.turns = map[string]turnActivity{"task-1": {ThreadID: "thread-1", Phase: "queued"}}
	pending, err := m.prepareQueuedMessage(outboundConversation, "next", nil)
	if err != nil {
		t.Fatal(err)
	}
	m.queuePreparedMessage(pending)

	if cmd := m.cancelActive(); cmd != nil {
		t.Fatal("unregistered cancellation should wait for a lifecycle signal")
	}
	canceller.ok = true
	cmd := m.retryPendingCancel("task-1")
	if cmd == nil {
		t.Fatal("successful cancellation did not release the next queued message")
	}
	if _, active := m.turns["task-1"]; active {
		t.Fatal("cancelled task remained active")
	}
	if len(m.pendingMessages) != 0 || m.lastTaskID == "task-1" {
		t.Fatalf("next message was not dispatched: pending=%+v last=%q", m.pendingMessages, m.lastTaskID)
	}
}
