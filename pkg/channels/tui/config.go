package tui

import (
	"context"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/taskstate"
	"github.com/scitrera/agent-harness-go/pkg/team"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

type HistoryStore interface {
	LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error)
	SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error
	DeleteHistory(ctx context.Context, threadID string) error
}

type ModelStatus interface {
	ActiveModelName(threadID string) string
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

type Config struct {
	Channel         *Channel
	Store           HistoryStore
	Index           *threadindex.Index
	Approvals       *approval.Broker
	Canceller       *turncancel.Canceller
	ModelStatus     ModelStatus
	TaskStore       TaskStore
	TeamStore       TeamStore
	AgentCatalog    AgentCatalog
	InitialThreadID string
}

func Run(ctx context.Context, cfg Config) error {
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
