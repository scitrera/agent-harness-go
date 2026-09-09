// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

type streamEventMsg struct {
	Event channel.Event
}

type sendResultMsg struct {
	Session     threadindex.Session
	WorkspaceID string
	TaskID      string
	Err         error
}

type shellResultMsg struct {
	Run shellRun
}

type historyLoadedMsg struct {
	WorkspaceID string
	ThreadID    string
	Messages    []protocol.ChatMessage
	Err         error
}

type threadCreatedMsg struct {
	WorkspaceID string
	Session     threadindex.Session
	Err         error
}

type threadDeletedMsg struct {
	WorkspaceID string
	DeletedID   string
	NextID      string
	Err         error
}

type threadRenamedMsg struct {
	WorkspaceID string
	ThreadID    string
	Err         error
}

type clearThreadMsg struct {
	WorkspaceID string
	ThreadID    string
	Err         error
}

type workspaceLoadedMsg struct {
	WorkspaceID string
	CWD         string
	Session     threadindex.Session
	Threads     []threadindex.Session
	Messages    []protocol.ChatMessage
	Err         error
}

type quitMsg struct{}

type drawerLoadedMsg struct {
	Drawer drawerMode
	Text   string
	Status string
	Err    error
}

type attachmentLoadedMsg struct {
	Attachment pendingAttachment
	Err        error
}

type thinkingTickMsg struct{}

type quitConfirmationExpiredMsg struct {
	Token uint64
}
