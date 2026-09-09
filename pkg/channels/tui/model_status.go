// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// observeMessageModel caches worker-owned model state for one addressed thread.
// eventAddr is the transport-resolved address and therefore fills any address
// fields omitted from the finalized message itself.
func (m *model) observeMessageModel(eventAddr protocol.MessageAddress, message protocol.ChatMessage) {
	name := modelpkg.ActiveModelFromMessage(message)
	if name == "" {
		return
	}
	workspaceID := message.Addr.WorkspaceID
	if workspaceID == "" {
		workspaceID = eventAddr.WorkspaceID
	}
	threadID := message.Addr.ThreadID
	if threadID == "" {
		threadID = eventAddr.ThreadID
	}
	if threadID == "" {
		return
	}
	if m.observedModels == nil {
		m.observedModels = map[string]string{}
	}
	m.observedModels[workspaceKey(workspaceID, threadID)] = name
	// An unscoped/legacy TUI accepts events from its configured default
	// workspace. Mirror that observation under its lookup key as well.
	if m.workspaceID == "" {
		m.observedModels[workspaceKey("", threadID)] = name
	}
}

// observeModelHistory restores the latest model observation while initializing
// or switching threads. The explicit load target wins over message addresses,
// which may be absent in legacy history.
func (m *model) observeModelHistory(workspaceID, threadID string, messages []protocol.ChatMessage) {
	for i := len(messages) - 1; i >= 0; i-- {
		name := modelpkg.ActiveModelFromMessage(messages[i])
		if name == "" {
			continue
		}
		if m.observedModels == nil {
			m.observedModels = map[string]string{}
		}
		m.observedModels[workspaceKey(workspaceID, threadID)] = name
		return
	}
}
