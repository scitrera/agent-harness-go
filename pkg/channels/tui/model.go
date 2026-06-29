package tui

import (
	"context"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

const composerHeight = 3

type model struct {
	ctx          context.Context
	channel      *Channel
	events       <-chan channel.Event
	store        HistoryStore
	index        *threadindex.Index
	approvals    approvalResolver
	canceller    canceler
	modelStatus  ModelStatus
	taskStore    TaskStore
	teamStore    TeamStore
	agentCatalog AgentCatalog

	threadID string
	threads  []threadindex.Session
	rows     []chatRow

	pendingApprovals map[string]approvalRequest
	tools            map[string]toolEntry
	lastTaskID       string
	status           string
	drawer           drawerMode
	drawerContent    string
	tailing          bool

	viewport viewport.Model
	composer textarea.Model
	width    int
	height   int
}

type approvalResolver interface {
	Resolve(taskID, requestID string, d approval.Decision) bool
}

type canceler interface {
	Cancel(taskID string) bool
}

func newModel(ctx context.Context, cfg Config) (model, error) {
	if cfg.Channel == nil {
		return model{}, fmt.Errorf("tui channel is required")
	}
	if cfg.Store == nil {
		return model{}, fmt.Errorf("history store is required")
	}
	if cfg.Index == nil {
		return model{}, fmt.Errorf("thread index is required")
	}
	threadID, threads, err := selectInitialThread(cfg.Index, cfg.InitialThreadID)
	if err != nil {
		return model{}, err
	}
	messages, err := cfg.Store.LoadHistory(ctx, threadID)
	if err != nil {
		return model{}, fmt.Errorf("load history: %w", err)
	}
	m := model{
		ctx:              ctx,
		channel:          cfg.Channel,
		events:           cfg.Channel.Events(),
		store:            cfg.Store,
		index:            cfg.Index,
		approvals:        cfg.Approvals,
		canceller:        cfg.Canceller,
		modelStatus:      cfg.ModelStatus,
		taskStore:        cfg.TaskStore,
		teamStore:        cfg.TeamStore,
		agentCatalog:     cfg.AgentCatalog,
		threadID:         threadID,
		threads:          threads,
		rows:             rowsFromHistory(messages),
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		tailing:          true,
		viewport:         viewport.New(),
		composer:         newComposer(),
	}
	m.status = "ready"
	m.refreshViewport()
	return m, nil
}

func selectInitialThread(index *threadindex.Index, requested string) (string, []threadindex.Session, error) {
	requested = strings.TrimSpace(requested)
	if requested != "" {
		if err := index.Touch(requested, ""); err != nil {
			return "", nil, fmt.Errorf("touch thread: %w", err)
		}
		return requested, index.List(), nil
	}
	threads := index.List()
	if len(threads) > 0 {
		return threads[0].ID, threads, nil
	}
	session, err := index.Create()
	if err != nil {
		return "", nil, fmt.Errorf("create thread: %w", err)
	}
	return session.ID, index.List(), nil
}

func newComposer() textarea.Model {
	composer := textarea.New()
	composer.Prompt = "> "
	composer.Placeholder = "Message or /command"
	composer.ShowLineNumbers = false
	composer.SetHeight(composerHeight)
	composer.SetWidth(80)
	composer.SetVirtualCursor(false)
	composer.Focus()
	return composer
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.composer.Focus(), waitEvent(m.ctx, m.events), tick(), setTerminalTitleCmd(m.terminalTitle()))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.resize(msg.Width, msg.Height)
		return m, nil
	case tea.KeyPressMsg:
		return m.updateKey(msg)
	case streamEventMsg:
		m.applyEvent(msg.Event)
		return m, waitEvent(m.ctx, m.events)
	case sendResultMsg:
		m.applySendResult(msg)
		return m, setTerminalTitleCmd(m.terminalTitle())
	case historyLoadedMsg:
		m.applyHistoryLoaded(msg)
		return m, setTerminalTitleCmd(m.terminalTitle())
	case threadCreatedMsg:
		m.applyThreadCreated(msg)
		return m, setTerminalTitleCmd(m.terminalTitle())
	case threadDeletedMsg:
		wasCurrent := msg.DeletedID == m.threadID
		m.applyThreadDeleted(msg)
		if msg.Err == nil && wasCurrent {
			if msg.NextID != "" {
				return m, loadHistoryCmd(m.ctx, m.store, msg.NextID)
			}
			return m, createThreadCmd(m.index, "")
		}
		return m, nil
	case threadRenamedMsg:
		m.applyThreadRenamed(msg)
		return m, setTerminalTitleCmd(m.terminalTitle())
	case clearThreadMsg:
		m.applyClearThread(msg)
		return m, nil
	case drawerLoadedMsg:
		m.applyDrawerLoaded(msg)
		return m, nil
	case tickMsg:
		m.refreshViewport()
		return m, tick()
	case quitMsg:
		return m, tea.Quit
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	if _, ok := msg.(tea.MouseWheelMsg); ok {
		m.tailing = m.viewport.AtBottom()
	}
	var inputCmd tea.Cmd
	m.composer, inputCmd = m.composer.Update(msg)
	return m, tea.Batch(cmd, inputCmd)
}
