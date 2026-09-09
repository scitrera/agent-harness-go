// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
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
	if !strings.Contains(initial, "Press Esc to Cancel") {
		t.Fatalf("thinking placeholder = %q, want cancel hint", initial)
	}

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

type recordingCanceller struct {
	taskID string
	ok     bool
}

func (c *recordingCanceller) Cancel(taskID string) bool {
	c.taskID = taskID
	return c.ok
}

func TestEscapeCancelsActiveTurn(t *testing.T) {
	canceller := &recordingCanceller{ok: true}
	m := model{
		threadID:   "thread-1",
		lastTaskID: "task-1",
		turns: map[string]turnActivity{
			"task-1": {ThreadID: "thread-1", Phase: "thinking"},
		},
		canceller: canceller,
		viewport:  viewport.New(),
	}
	m.addThinking("task-1")

	next, cmd := m.updateKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if cmd != nil {
		t.Fatal("escape cancellation should be synchronous")
	}
	updated := next.(model)
	if canceller.taskID != "task-1" {
		t.Fatalf("cancelled task = %q, want task-1", canceller.taskID)
	}
	if _, ok := updated.turns["task-1"]; ok {
		t.Fatal("cancelled turn remained active")
	}
	if updated.hasThinking() {
		t.Fatal("cancelled turn retained thinking placeholder")
	}
	if len(updated.rows) != 1 || !strings.Contains(updated.rows[0].Text, "cancelled task-1") {
		t.Fatalf("rows after cancel = %+v", updated.rows)
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

func TestThinkingPlaceholderReturnsBetweenToolCalls(t *testing.T) {
	m := model{
		threadID:         "thread-1",
		turns:            map[string]turnActivity{"task-1": {ThreadID: "thread-1", Phase: "thinking"}},
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		viewport:         viewport.New(),
	}
	m.addThinking("task-1")

	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "read_file"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	m.applyEvent(channel.Event{
		Type: channel.EventPartAppended,
		Addr: protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Part: &callPart,
	})
	m.applyEvent(toolLifecycleEvent(t, "task-1", "call-1", tools.ToolEventStarted))
	if m.hasThinking() {
		t.Fatal("thinking placeholder remained while tool was running")
	}

	m.applyEvent(toolLifecycleEvent(t, "task-1", "call-1", tools.ToolEventFinished))
	if !m.hasThinking() {
		t.Fatal("thinking placeholder did not return after tool finished")
	}

	resultPart, err := protocol.NewToolResultPart("call-1", "read_file", json.RawMessage(`{"content":"ok"}`), false)
	if err != nil {
		t.Fatalf("tool result part: %v", err)
	}
	m.applyEvent(channel.Event{
		Type: channel.EventPartAppended,
		Addr: protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Part: &resultPart,
	})
	if !m.hasThinking() {
		t.Fatal("tool result hid thinking before the next model response")
	}

	m.applyEvent(channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		MessageID: "assistant-1",
		Delta:     "done",
	})
	if m.hasThinking() {
		t.Fatal("assistant response did not replace thinking placeholder")
	}
}

func toolLifecycleEvent(t *testing.T, taskID, callID string, status tools.ToolEventStatus) channel.Event {
	t.Helper()
	payload, err := json.Marshal(tools.ToolEvent{CallID: callID, ToolName: "read_file", Status: status})
	if err != nil {
		t.Fatalf("marshal tool event: %v", err)
	}
	return channel.Event{
		Type:    channel.EventToolLifecycle,
		Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: taskID},
		Payload: payload,
	}
}
