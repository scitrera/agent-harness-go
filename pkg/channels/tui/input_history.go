// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
)

func inputHistoryFromMessages(messages []protocol.ChatMessage) []string {
	history := make([]string, 0, len(messages))
	for _, message := range messages {
		if message.Role != protocol.RoleUser {
			continue
		}
		if record, ok := shellcontext.FromMessage(message); ok {
			history = append(history, "!"+record.Command)
			continue
		}
		if text := strings.TrimSpace(messageText(message)); text != "" {
			history = append(history, text)
		}
	}
	return history
}

func cloneAttachments(attachments []pendingAttachment) []pendingAttachment {
	return append([]pendingAttachment(nil), attachments...)
}

func (m model) composerAtTop() bool {
	if m.composer.Line() != 0 {
		return false
	}
	return m.composer.LineInfo().RowOffset == 0
}

func (m model) composerAtBottom() bool {
	if m.composer.Line() != m.composer.LineCount()-1 {
		return false
	}
	info := m.composer.LineInfo()
	return info.RowOffset >= info.Height-1
}

func (m *model) resetHistoryNavigation() {
	m.historyIndex = 0
	m.historyDraft = ""
	m.historyDraftAtts = nil
	m.editingPendingID = 0
}

func (m *model) setComposerValue(text string, attachments []pendingAttachment) {
	m.composer.SetValue(text)
	m.attachments = cloneAttachments(attachments)
	m.updateAttachmentPlaceholder()
	m.refreshInputSurface()
}

func (m model) currentPendingIndexes() []int {
	indexes := make([]int, 0, len(m.pendingMessages))
	for i, pending := range m.pendingMessages {
		if m.workspaceMatchesCurrent(pending.WorkspaceID) && pending.ThreadID == m.threadID {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func (m model) pendingIndexByID(id uint64) int {
	for i := range m.pendingMessages {
		if m.pendingMessages[i].ID == id {
			return i
		}
	}
	return -1
}

func (m *model) loadPendingForEdit(index int) {
	if index < 0 || index >= len(m.pendingMessages) {
		return
	}
	pending := m.pendingMessages[index]
	m.historyIndex = 0
	m.editingPendingID = pending.ID
	m.setComposerValue(pending.Input, pending.Attachments)
	m.status = "editing queued message"
}

func (m *model) loadHistoryEntry(index int) {
	if index < 0 || index >= len(m.inputHistory) {
		return
	}
	m.editingPendingID = 0
	m.historyIndex = index + 1
	m.setComposerValue(m.inputHistory[index], nil)
}

func (m *model) beginHistoryNavigation() {
	m.historyDraft = m.composer.Value()
	m.historyDraftAtts = cloneAttachments(m.attachments)
	indexes := m.currentPendingIndexes()
	if len(indexes) > 0 {
		m.loadPendingForEdit(indexes[len(indexes)-1])
		return
	}
	if len(m.inputHistory) > 0 {
		m.loadHistoryEntry(len(m.inputHistory) - 1)
	}
}

func (m *model) restoreHistoryDraft() {
	draft := m.historyDraft
	attachments := cloneAttachments(m.historyDraftAtts)
	m.resetHistoryNavigation()
	m.setComposerValue(draft, attachments)
}

func (m *model) persistPendingEdit() error {
	index := m.pendingIndexByID(m.editingPendingID)
	if index < 0 {
		m.editingPendingID = 0
		return nil
	}
	text := strings.TrimSpace(m.composer.Value())
	if text == "" && len(m.attachments) == 0 {
		m.pendingMessages = append(m.pendingMessages[:index], m.pendingMessages[index+1:]...)
		m.editingPendingID = 0
		return nil
	}
	replacement, err := m.prepareQueuedMessage(m.pendingMessages[index].Kind, text, m.attachments)
	if err != nil {
		return err
	}
	replacement.ID = m.pendingMessages[index].ID
	m.pendingMessages[index] = replacement
	return nil
}

func (m *model) historyUp() bool {
	if m.editingPendingID != 0 {
		indexes := m.currentPendingIndexes()
		position := -1
		for i, index := range indexes {
			if m.pendingMessages[index].ID == m.editingPendingID {
				position = i
				break
			}
		}
		var targetID uint64
		if position > 0 {
			targetID = m.pendingMessages[indexes[position-1]].ID
		}
		if err := m.persistPendingEdit(); err != nil {
			m.addSystem("queued edit failed: " + err.Error())
			return true
		}
		if targetID != 0 {
			m.loadPendingForEdit(m.pendingIndexByID(targetID))
			return true
		}
		if len(m.inputHistory) > 0 {
			m.loadHistoryEntry(len(m.inputHistory) - 1)
		}
		return true
	}
	if m.historyIndex > 0 {
		if m.historyIndex > 1 {
			m.loadHistoryEntry(m.historyIndex - 2)
		}
		return true
	}
	if len(m.currentPendingIndexes()) == 0 && len(m.inputHistory) == 0 {
		return false
	}
	m.beginHistoryNavigation()
	return true
}

func (m *model) historyDown() bool {
	if m.editingPendingID != 0 {
		indexes := m.currentPendingIndexes()
		position := -1
		for i, index := range indexes {
			if m.pendingMessages[index].ID == m.editingPendingID {
				position = i
				break
			}
		}
		var targetID uint64
		if position >= 0 && position+1 < len(indexes) {
			targetID = m.pendingMessages[indexes[position+1]].ID
		}
		if err := m.persistPendingEdit(); err != nil {
			m.addSystem("queued edit failed: " + err.Error())
			return true
		}
		if targetID != 0 {
			m.loadPendingForEdit(m.pendingIndexByID(targetID))
			return true
		}
		m.restoreHistoryDraft()
		return true
	}
	if m.historyIndex > 0 {
		if m.historyIndex < len(m.inputHistory) {
			m.loadHistoryEntry(m.historyIndex)
			return true
		}
		indexes := m.currentPendingIndexes()
		if len(indexes) > 0 {
			m.loadPendingForEdit(indexes[0])
			return true
		}
		m.restoreHistoryDraft()
		return true
	}
	return false
}
