package tui

import (
	"context"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

const (
	minComposerHeight        = 1
	maxComposerHeight        = 3
	maxComposerContentHeight = 1000
	// DEC private mode 1007 asks compatible terminals to translate wheel input
	// into cursor Up/Down while the alternate screen is active. Unlike mouse
	// reporting modes 1000/1002, it leaves drag selection owned by the terminal.
	alternateScrollModeOn  = "\x1b[?1007h"
	alternateScrollModeOff = "\x1b[?1007l"
)

type model struct {
	ctx             context.Context
	channel         ChannelSurface
	events          <-chan channel.Event
	store           HistoryStore
	index           *threadindex.Index
	approvals       ApprovalResolver
	canceller       Canceller
	modelStatus     ModelStatus
	commandSource   CommandProvider
	taskStore       TaskStore
	teamStore       TeamStore
	agentCatalog    AgentCatalog
	directoryAccess DirectoryAccess
	workspaceRoot   string
	cwd             string

	threadID string
	threads  []threadindex.Session
	rows     []chatRow

	pendingApprovals map[string]approvalRequest
	tools            map[string]toolEntry
	subagents        map[string]subagentActivity
	turns            map[string]turnActivity
	renderedRows     map[string]renderedRowCache
	attachments      []pendingAttachment
	thinkingFrame    int
	thinkingTicking  bool
	clearingThreads  map[string]struct{}
	deferredSendFor  string
	lastTaskID       string
	status           string
	drawer           drawerMode
	drawerContent    string
	tailing          bool
	selector         selectionState
	confirmation     pendingConfirmation
	quitConfirmation pendingQuitConfirmation
	quitToken        uint64
	toolActiveOnly   bool
	toolDetailID     string

	viewport viewport.Model
	composer textarea.Model
	width    int
	height   int
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
	workspaceRoot, err := canonicalWorkspaceRoot(cfg.WorkspaceRoot)
	if err != nil {
		return model{}, fmt.Errorf("resolve workspace root: %w", err)
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
		commandSource:    cfg.Commands,
		taskStore:        cfg.TaskStore,
		teamStore:        cfg.TeamStore,
		agentCatalog:     cfg.AgentCatalog,
		directoryAccess:  cfg.DirectoryAccess,
		workspaceRoot:    workspaceRoot,
		cwd:              workspaceRoot,
		threadID:         threadID,
		threads:          threads,
		rows:             rowsFromHistory(messages),
		pendingApprovals: map[string]approvalRequest{},
		tools:            map[string]toolEntry{},
		subagents:        map[string]subagentActivity{},
		turns:            map[string]turnActivity{},
		renderedRows:     map[string]renderedRowCache{},
		clearingThreads:  map[string]struct{}{},
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
	composer.Placeholder = defaultComposerPlaceholder
	composer.ShowLineNumbers = false
	composer.MinHeight = minComposerHeight
	composer.MaxHeight = maxComposerHeight
	composer.MaxContentHeight = maxComposerContentHeight
	composer.DynamicHeight = true
	composer.KeyMap.InsertNewline = key.NewBinding(
		key.WithKeys("shift+enter", "ctrl+j", "alt+enter"),
		key.WithHelp("shift+enter", "newline"),
	)
	composer.KeyMap.WordBackward.SetKeys("alt+left", "alt+b", "ctrl+left")
	composer.KeyMap.WordForward.SetKeys("alt+right", "alt+f", "ctrl+right")
	composer.SetHeight(minComposerHeight)
	composer.SetWidth(80)
	composer.SetVirtualCursor(false)
	composer.Focus()
	return composer
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.composer.Focus(), waitEvent(m.ctx, m.events), setTerminalTitleCmd(m.terminalTitle()), tea.Raw(alternateScrollModeOn))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.resize(msg.Width, msg.Height)
		return m, nil
	case tea.KeyPressMsg:
		updated, cmd := m.updateKey(msg)
		if msg.String() == "enter" {
			return updated, repaint(cmd)
		}
		return updated, cmd
	case tea.PasteMsg:
		return m.updatePaste(msg)
	case streamEventMsg:
		rowStructure := rowStructureKey(m.rows)
		m.applyEvent(msg.Event)
		next := waitEvent(m.ctx, m.events)
		if rowStructureKey(m.rows) != rowStructure || msg.Event.Type == channel.EventMessageFinal {
			next = repaint(next)
		}
		return m, m.ensureThinkingTick(next)
	case sendResultMsg:
		m.applySendResult(msg)
		return m, m.ensureThinkingTick(setTerminalTitleCmd(m.terminalTitle()))
	case thinkingTickMsg:
		if !m.advanceThinking() {
			m.thinkingTicking = false
			return m, nil
		}
		return m, thinkingTickCmd()
	case quitConfirmationExpiredMsg:
		m.expireQuitConfirmation(msg)
		return m, nil
	case historyLoadedMsg:
		m.applyHistoryLoaded(msg)
		return m, repaint(setTerminalTitleCmd(m.terminalTitle()))
	case threadCreatedMsg:
		m.applyThreadCreated(msg)
		return m, repaint(setTerminalTitleCmd(m.terminalTitle()))
	case threadDeletedMsg:
		wasCurrent := msg.DeletedID == m.threadID
		m.applyThreadDeleted(msg)
		if msg.Err == nil && wasCurrent {
			if msg.NextID != "" {
				return m, repaint(loadHistoryCmd(m.ctx, m.store, msg.NextID))
			}
			return m, repaint(createThreadCmd(m.index, ""))
		}
		return m, repaint(nil)
	case threadRenamedMsg:
		m.applyThreadRenamed(msg)
		return m, setTerminalTitleCmd(m.terminalTitle())
	case clearThreadMsg:
		sendAfterClear := msg.Err == nil &&
			m.deferredSendFor == msg.ThreadID &&
			m.threadID == msg.ThreadID
		if m.deferredSendFor == msg.ThreadID {
			m.deferredSendFor = ""
		}
		m.applyClearThread(msg)
		if sendAfterClear {
			updated, cmd := m.sendCurrent()
			return updated, repaint(cmd)
		}
		return m, repaint(nil)
	case drawerLoadedMsg:
		m.applyDrawerLoaded(msg)
		return m, nil
	case attachmentLoadedMsg:
		m.applyAttachmentLoaded(msg)
		return m, nil
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

func repaint(cmd tea.Cmd) tea.Cmd {
	return tea.Batch(cmd, tea.ClearScreen)
}
