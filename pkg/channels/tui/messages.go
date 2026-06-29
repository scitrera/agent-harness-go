package tui

import (
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

type streamEventMsg struct {
	Event channel.Event
}

type sendResultMsg struct {
	Session threadindex.Session
	Err     error
}

type historyLoadedMsg struct {
	ThreadID string
	Messages []protocol.ChatMessage
	Err      error
}

type threadCreatedMsg struct {
	Session threadindex.Session
	Err     error
}

type threadDeletedMsg struct {
	DeletedID string
	NextID    string
	Err       error
}

type threadRenamedMsg struct {
	ThreadID string
	Err      error
}

type clearThreadMsg struct {
	ThreadID string
	Err      error
}

type tickMsg struct {
	At time.Time
}

type quitMsg struct{}

type drawerLoadedMsg struct {
	Drawer drawerMode
	Text   string
	Status string
	Err    error
}
