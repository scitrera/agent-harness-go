// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func contentForMessage(text string, attachments []pendingAttachment) ([]protocol.ContentPart, error) {
	content := make([]protocol.ContentPart, 0, 1+len(attachments))
	if text != "" {
		part, err := protocol.NewTextPart(text)
		if err != nil {
			return nil, err
		}
		content = append(content, part)
	}
	for _, attachment := range attachments {
		content = append(content, attachment.Part)
	}
	return content, nil
}

func (m model) prepareQueuedMessage(kind outboundKind, input string, attachments []pendingAttachment) (queuedMessage, error) {
	input = strings.TrimSpace(input)
	prepared := queuedMessage{
		Kind:        kind,
		WorkspaceID: m.workspaceID,
		ThreadID:    m.threadID,
		Input:       input,
		DisplayText: input,
		Attachments: cloneAttachments(attachments),
	}
	messageText := input
	var referencedImages []referencedImage
	switch kind {
	case outboundMetaCommand:
		if strings.HasPrefix(messageText, "/models") {
			messageText = "/model" + strings.TrimPrefix(messageText, "/models")
		}
		prepared.DisplayText = messageText
		attachments = nil
	case outboundConversation:
		withAttachments := m
		withAttachments.attachments = cloneAttachments(attachments)
		resolved, images, err := withAttachments.resolveAtReferences(messageText)
		if err != nil {
			return queuedMessage{}, fmt.Errorf("reference failed: %w", err)
		}
		prepared.DisplayText = resolved
		messageText = withAttachments.withWorkingDirectoryContext(resolved)
		referencedImages = images
	}
	content, err := contentForMessage(messageText, attachments)
	if err != nil {
		return queuedMessage{}, err
	}
	addr := protocol.MessageAddress{WorkspaceID: m.workspaceID, ThreadID: m.threadID}
	message := protocol.ChatMessage{Role: protocol.RoleUser, Addr: addr, Content: content}
	if err := m.scopeMessage(&addr, &message); err != nil {
		return queuedMessage{}, err
	}
	message.Addr.ThreadID = m.threadID
	prepared.Message = message
	prepared.ReferencedImages = referencedImages
	return prepared, nil
}

func (m model) hasActiveTurnFor(workspaceID, threadID string) bool {
	for _, activity := range m.turns {
		workspaceMatches := workspaceID == "" || activity.WorkspaceID == "" || activity.WorkspaceID == workspaceID
		if workspaceMatches && (activity.ThreadID == "" || activity.ThreadID == threadID) {
			return true
		}
	}
	return false
}

func (m *model) queuePreparedMessage(message queuedMessage) {
	m.nextPendingID++
	message.ID = m.nextPendingID
	m.pendingMessages = append(m.pendingMessages, message)
	m.status = m.activeTurnStatus()
	m.reflowSurfaces()
	m.refreshViewport()
}

func (m *model) dispatchPreparedMessage(pending queuedMessage) tea.Cmd {
	taskID, err := threadindex.NewID("task-")
	if err != nil {
		m.addSystem("could not create task id: " + err.Error())
		return nil
	}
	addr := pending.Message.Addr
	addr.ThreadID = pending.ThreadID
	addr.TaskID = taskID
	message := pending.Message
	if message.ID == "" {
		message.ID = "user-" + taskID
	}
	message.Addr = addr
	if pending.Presented {
		for i := range m.rows {
			if m.rows[i].ID == message.ID {
				m.rows[i].TaskID = taskID
				break
			}
		}
	}
	phase := "queued"
	if shellcontext.IsContextOnly(message) {
		phase = "saving context"
	}
	m.markTurnAt(taskID, pending.WorkspaceID, pending.ThreadID, phase)
	if m.workspaceMatchesCurrent(pending.WorkspaceID) && pending.ThreadID == m.threadID {
		m.lastTaskID = taskID
		if !pending.Presented {
			m.inputHistory = append(m.inputHistory, pending.Input)
		}
		if pending.Kind != outboundMetaCommand && !pending.Presented {
			m.rows = append(m.rows, chatRow{Kind: rowUser, ID: message.ID, TaskID: taskID, Text: messageText(message)})
			if !shellcontext.IsContextOnly(message) {
				m.addThinking(taskID)
			}
			m.refreshViewportToBottom()
		} else {
			if pending.Presented && !shellcontext.IsContextOnly(message) {
				m.addThinking(taskID)
			}
			m.refreshViewport()
		}
	}
	if pending.Kind == outboundMetaCommand {
		return sendMetaCommandCmd(m.ctx, m.channel, pending.WorkspaceID, addr, message)
	}
	return sendMessageCmd(
		m.ctx, m.channel, m.index, m.initialWorkspaceID, pending.WorkspaceID,
		addr, message, pending.DisplayText, pending.ReferencedImages,
	)
}

func (m model) submitPreparedMessage(pending queuedMessage) (tea.Model, tea.Cmd) {
	if m.hasActiveTurnFor(pending.WorkspaceID, pending.ThreadID) {
		m.queuePreparedMessage(pending)
		return m, nil
	}
	return m, m.dispatchPreparedMessage(pending)
}

func (m *model) dispatchNextPending() tea.Cmd {
	for i, pending := range m.pendingMessages {
		if pending.ID == m.editingPendingID || m.hasActiveTurnFor(pending.WorkspaceID, pending.ThreadID) {
			continue
		}
		m.pendingMessages = append(m.pendingMessages[:i], m.pendingMessages[i+1:]...)
		m.reflowSurfaces()
		return m.dispatchPreparedMessage(pending)
	}
	m.status = m.activeTurnStatus()
	m.reflowSurfaces()
	m.refreshViewport()
	return nil
}

func (m model) commitPendingEdit() (tea.Model, tea.Cmd) {
	if err := m.persistPendingEdit(); err != nil {
		m.addSystem("queued edit failed: " + err.Error())
		return m, nil
	}
	m.composer.Reset()
	m.attachments = nil
	m.updateAttachmentPlaceholder()
	m.resetHistoryNavigation()
	m.refreshInputSurface()
	return m, m.dispatchNextPending()
}

func sendMessageCmd(
	ctx context.Context,
	ch ChannelSurface,
	index threadindex.Store,
	initialWorkspaceID string,
	storageWorkspaceID string,
	addr protocol.MessageAddress,
	message protocol.ChatMessage,
	text string,
	referencedImages []referencedImage,
) tea.Cmd {
	return func() tea.Msg {
		var err error
		message, err = appendReferencedImages(ctx, message, referencedImages)
		if err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("load image reference: %w", err)}
		}
		if err := touchWorkspaceThread(index, initialWorkspaceID, storageWorkspaceID, addr.ThreadID, text); err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("touch thread: %w", err)}
		}
		if err := ch.Enqueue(ctx, channel.Inbound{Addr: addr, Message: message}); err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("enqueue: %w", err)}
		}
		return sendResultMsg{
			WorkspaceID: storageWorkspaceID,
			Session:     threadindex.Session{ID: addr.ThreadID},
			TaskID:      addr.TaskID,
		}
	}
}

func sendMetaCommandCmd(ctx context.Context, ch ChannelSurface, storageWorkspaceID string, addr protocol.MessageAddress, message protocol.ChatMessage) tea.Cmd {
	return func() tea.Msg {
		if err := ch.Enqueue(ctx, channel.Inbound{Addr: addr, Message: message}); err != nil {
			return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID, Err: fmt.Errorf("enqueue: %w", err)}
		}
		return sendResultMsg{WorkspaceID: storageWorkspaceID, TaskID: addr.TaskID}
	}
}
