package tui

import (
	"context"
	"testing"

	"charm.land/bubbles/v2/viewport"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func TestThinkingPlaceholderAnimatesUntilFirstResponseContent(t *testing.T) {
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatalf("touch thread: %v", err)
	}
	m := model{
		ctx:              context.Background(),
		channel:          NewChannel(),
		index:            index,
		threadID:         "thread-1",
		viewport:         viewport.New(),
		composer:         newComposer(),
		turns:            map[string]turnActivity{},
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		renderedRows:     map[string]renderedRowCache{},
		tailing:          true,
	}
	m.resize(60, 20)
	m.composer.SetValue("hello")

	next, sendCmd := m.sendCurrent()
	if sendCmd == nil {
		t.Fatal("send should enqueue a turn")
	}
	updated := next.(model)
	if len(updated.rows) != 2 || updated.rows[0].Kind != rowUser || updated.rows[1].Kind != rowThinking {
		t.Fatalf("pending turn rows = %+v", updated.rows)
	}
	initial := updated.rows[1].Text

	sendResult := sendCmd().(sendResultMsg)
	next, tickCmd := updated.Update(sendResult)
	updated = next.(model)
	if tickCmd == nil || !updated.thinkingTicking {
		t.Fatal("successful enqueue should start the thinking animation")
	}

	next, tickCmd = updated.Update(thinkingTickMsg{})
	updated = next.(model)
	if tickCmd == nil {
		t.Fatal("active placeholder should schedule another animation frame")
	}
	if updated.rows[1].Text == initial {
		t.Fatalf("thinking frame did not advance from %q", initial)
	}

	updated.applyEvent(channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      protocol.MessageAddress{ThreadID: "thread-1", TaskID: sendResult.TaskID},
		MessageID: "assistant-1",
		Index:     0,
		Delta:     "hello back",
	})
	if len(updated.rows) != 2 || updated.rows[0].Kind != rowUser || updated.rows[1].Kind != rowAssistant {
		t.Fatalf("first response did not replace placeholder: %+v", updated.rows)
	}
	if updated.rows[1].Text != "hello back" {
		t.Fatalf("assistant text = %q", updated.rows[1].Text)
	}
}

func TestRemoveThinkingOnlyRemovesMatchingTurn(t *testing.T) {
	m := model{}
	m.addThinking("task-1")
	m.addThinking("task-2")

	m.removeThinking("task-1")

	if len(m.rows) != 1 || m.rows[0].TaskID != "task-2" {
		t.Fatalf("remaining placeholders = %+v", m.rows)
	}
}
