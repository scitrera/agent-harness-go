package tui

import (
	"context"
	"os"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/taskstate"
	"github.com/scitrera/agent-harness-go/pkg/team"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

type HistoryStore interface {
	LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error)
	SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error
	DeleteHistory(ctx context.Context, threadID string) error
}

// ChannelSurface is what the UI needs from its transport: submit a turn, read
// the resulting stream events, and report what was shed under load. The
// in-process Channel satisfies it, and so does a remote transport whose other
// half runs in another process — which is how the same UI drives either a
// local runner or an agent reached over Aether.
type ChannelSurface interface {
	Enqueue(ctx context.Context, in channel.Inbound) error
	Events() <-chan channel.Event
	DroppedEvents() int64
}

// ApprovalResolver settles a pending tool-approval prompt. Locally that is the
// approval broker; over a remote transport it sends an approve/deny control to
// the agent holding the prompt.
type ApprovalResolver interface {
	Resolve(taskID, requestID string, d approval.Decision) bool
}

// Canceller aborts an in-flight turn — the local turn canceller, or a control
// message to the agent running it.
type Canceller interface {
	Cancel(taskID string) bool
}

type ModelStatus interface {
	ActiveModelName(threadID string) string
}

type CommandProvider interface {
	AvailableCommands() []commands.Command
}

type TaskStore interface {
	taskstate.Store
}

type TeamStore interface {
	ListAgents(ctx context.Context) ([]team.AgentNode, error)
	RegisterAgent(ctx context.Context, node team.AgentNode) error
	AddChild(ctx context.Context, req team.AddChildRequest) error
	UpdateAgentStatus(ctx context.Context, id team.AgentID, status team.AgentStatus) (team.AgentNode, error)
	DescendantsBFS(ctx context.Context, parentID team.AgentID) ([]team.AgentNode, error)
}

type AgentCatalog interface {
	List(ctx context.Context) ([]subagent.Definition, error)
	Get(ctx context.Context, typ subagent.AgentType) (subagent.Definition, error)
}

// DirectoryAccess grants model tools access to an explicit user-selected
// working directory outside the original workspace.
type DirectoryAccess interface {
	GrantWorkingDirectory(dir string) error
}

type Config struct {
	Channel         ChannelSurface
	Store           HistoryStore
	Index           *threadindex.Index
	Approvals       ApprovalResolver
	Canceller       Canceller
	ModelStatus     ModelStatus
	Commands        CommandProvider
	TaskStore       TaskStore
	TeamStore       TeamStore
	AgentCatalog    AgentCatalog
	DirectoryAccess DirectoryAccess
	InitialThreadID string
	WorkspaceRoot   string
}

func Run(ctx context.Context, cfg Config) error {
	defer func() { _, _ = os.Stdout.WriteString(alternateScrollModeOff) }()
	model, err := newModel(ctx, cfg)
	if err != nil {
		return err
	}
	program := tea.NewProgram(model, tea.WithContext(ctx))
	if _, err := program.Run(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
