package turn

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/dailynotes"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type Provider interface {
	Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error)
}

// StreamingProvider is an optional capability: providers that implement it can
// stream text tokens (delivered to onDelta) while assembling the final message.
type StreamingProvider interface {
	ChatStream(ctx context.Context, req provider.ChatRequest, onDelta provider.DeltaFunc) (provider.ChatResponse, error)
}

// MemoryService is the optional MemoryLayer integration: recall for the
// auto-injection hook, and append for the auto-commit write path. Workspace +
// thread are passed per call (not baked into the client) so it is safe to share
// across concurrent turns.
type MemoryService interface {
	Recall(ctx context.Context, auth tools.MemoryAuthority, workspace, query string, limit int) ([]tools.MemoryHit, error)
	AppendThreadMessages(ctx context.Context, auth tools.MemoryAuthority, workspace, threadID string, messages []protocol.ChatMessage) error
}

type BootstrapLoader interface {
	LoadBootstrap(ctx context.Context) ([]bootstrap.File, error)
}

// ContextManager assembles the model-visible context for a turn from the
// bootstrap files and thread history. Implementations may compact, summarize, or
// augment (e.g. MemoryLayer-backed RAG) however they like. The default is
// contextpack.Assembler, which satisfies this interface as-is.
type ContextManager interface {
	Build(ctx context.Context, bootstrap []bootstrap.File, history []protocol.ChatMessage) ([]protocol.ChatMessage, error)
}

// EventPublisher is the egress half of the transport seam (channel.Publisher).
type EventPublisher = channel.Publisher

type Runner struct {
	store             harness.HistoryStore
	loader            BootstrapLoader
	registry          *tools.Registry
	provider          Provider
	publisher         EventPublisher
	ctxMgr            ContextManager
	model             string
	maxToolIterations int
	streaming         bool
	toolSpecs         []provider.ToolSpec

	memory                MemoryService
	memAutoCommit         bool
	memAutoCommitAsstOnly bool
	memAutoCommitAsync    bool
	memAutoRecall         bool
	memRecallLimit        int
	memRecallWithInput    bool

	dailyNotes     bool
	dailyNotesDir  string
	dailyNotesDays int
	now            func() time.Time

	commands          *commands.Registry
	approvers         []hooks.ToolApprover
	observers         []hooks.ToolObserver
	authorityFn       AuthorityFunc
	dedupTrailingUser bool
}

// AuthorityFunc derives a turn's OBO authority from the inbound address+message.
// Core leaves it unset (zero authority → memory uses its default); the Scitrera
// distribution wires one that reads the inbound grant.
type AuthorityFunc func(addr protocol.MessageAddress, user protocol.ChatMessage) tools.MemoryAuthority

type Config struct {
	Store     harness.HistoryStore
	Loader    BootstrapLoader
	Registry  *tools.Registry
	Provider  Provider
	Publisher EventPublisher
	// Assembler is the default context manager. ContextManager, when set,
	// overrides it (e.g. a MemoryLayer-backed strategy).
	Assembler         contextpack.Assembler
	ContextManager    ContextManager
	Model             string
	MaxToolIterations int
	Streaming         bool

	// Memory integration (optional). When Memory is set: MemoryAutoCommit appends
	// the turn's user+assistant messages to the thread; MemoryAutoRecall injects
	// thread-scoped recalled memories into the turn context.
	Memory           MemoryService
	MemoryAutoCommit bool
	// MemoryAutoCommitAssistantOnly commits ONLY the assistant message on
	// auto-commit. Use it when the host already persists the user message (e.g.
	// the workclaw platform-server commits the inbound user turn before
	// dispatching the chat task); committing it again here would duplicate the
	// turn. Default false: commit user + assistant.
	MemoryAutoCommitAssistantOnly bool
	// MemoryAutoCommitAsync runs the post-turn commit in a detached goroutine so
	// it never blocks turn completion. Use it when the commit transport may stall
	// (e.g. a MemoryLayer ProxyHttp whose response does not yet route back through
	// the sandbox relay — the write still lands, but the response wait would
	// otherwise delay the turn). Only safe for long-lived runtimes (a CLI process
	// could exit before the goroutine finishes). Default false: synchronous.
	MemoryAutoCommitAsync    bool
	MemoryAutoRecall         bool
	MemoryRecallLimit        int
	MemoryRecallIncludeInput bool

	// Daily-notes startup context. When DailyNotes is set, the first turn of a
	// thread injects recent <DailyNotesDir>/memory/YYYY-MM-DD*.md as untrusted
	// background. Now supplies the clock (defaults to time.Now).
	DailyNotes     bool
	DailyNotesDir  string
	DailyNotesDays int
	Now            func() time.Time

	// Commands holds discovered workspace slash commands. Reserved built-ins
	// (/help, /commands, /clear) are always available; a nil registry just means
	// no workspace commands.
	Commands *commands.Registry

	// Approvers gate tool calls (first denial wins); Observers watch the tool
	// lifecycle (no veto). Both are in-process today but the interfaces are
	// transport-agnostic so an external (e.g. Aether-ACL) approver or an OTel
	// observer can be plugged in later.
	Approvers []hooks.ToolApprover
	Observers []hooks.ToolObserver

	// Authority derives each turn's OBO authority from the inbound message. Core
	// leaves it nil; the distribution wires the grant extractor.
	Authority AuthorityFunc

	// DedupTrailingUserTurn drops a trailing user message from the loaded history
	// when it matches the incoming turn (same id, else same non-empty text) before
	// appending it. Use it when the host commits the inbound user turn to the store
	// out-of-band before dispatching (so the fetched history can already end with
	// it); the incoming turn is then kept as the canonical copy. Default false.
	DedupTrailingUserTurn bool
}

func NewRunner(cfg Config) (*Runner, error) {
	if cfg.Store == nil {
		return nil, ErrMissingStore
	}
	if cfg.Loader == nil {
		return nil, ErrMissingLoader
	}
	if cfg.Provider == nil {
		return nil, ErrMissingProvider
	}
	if cfg.Registry == nil {
		cfg.Registry = tools.NewRegistry()
	}
	if cfg.MaxToolIterations <= 0 {
		cfg.MaxToolIterations = defaultMaxToolIterations
	}
	ctxMgr := cfg.ContextManager
	if ctxMgr == nil {
		ctxMgr = cfg.Assembler
	}
	return &Runner{
		store:                 cfg.Store,
		loader:                cfg.Loader,
		registry:              cfg.Registry,
		provider:              cfg.Provider,
		publisher:             cfg.Publisher,
		ctxMgr:                ctxMgr,
		model:                 cfg.Model,
		maxToolIterations:     cfg.MaxToolIterations,
		streaming:             cfg.Streaming,
		toolSpecs:             toolSpecsFrom(cfg.Registry),
		memory:                cfg.Memory,
		memAutoCommit:         cfg.MemoryAutoCommit,
		memAutoCommitAsstOnly: cfg.MemoryAutoCommitAssistantOnly,
		memAutoCommitAsync:    cfg.MemoryAutoCommitAsync,
		memAutoRecall:         cfg.MemoryAutoRecall,
		memRecallLimit:        recallLimitOrDefault(cfg.MemoryRecallLimit),
		memRecallWithInput:    cfg.MemoryRecallIncludeInput,
		dailyNotes:            cfg.DailyNotes,
		dailyNotesDir:         cfg.DailyNotesDir,
		dailyNotesDays:        cfg.DailyNotesDays,
		now:                   cfg.Now,
		commands:              cfg.Commands,
		approvers:             cfg.Approvers,
		observers:             cfg.Observers,
		authorityFn:           cfg.Authority,
		dedupTrailingUser:     cfg.DedupTrailingUserTurn,
	}, nil
}

func recallLimitOrDefault(limit int) int {
	if limit <= 0 {
		return 5
	}
	return limit
}

func textOf(m protocol.ChatMessage) string {
	var b strings.Builder
	for _, part := range m.Content {
		if tp, ok := part.AsText(); ok {
			b.WriteString(tp.Text)
		}
	}
	return b.String()
}

func recalledMemoryMessage(hits []tools.MemoryHit, addr protocol.MessageAddress) (protocol.ChatMessage, bool) {
	if len(hits) == 0 {
		return protocol.ChatMessage{}, false
	}
	var b strings.Builder
	b.WriteString("Relevant memories (auto-recalled; use memory_search/memory_get to dig deeper):\n")
	for _, h := range hits {
		b.WriteString("- ")
		b.WriteString(h.Content)
		if h.ID != "" {
			b.WriteString(" [id: ")
			b.WriteString(h.ID)
			b.WriteString("]")
		}
		b.WriteString("\n")
	}
	part, err := protocol.NewTextPart(strings.TrimRight(b.String(), "\n"))
	if err != nil {
		return protocol.ChatMessage{}, false
	}
	return protocol.ChatMessage{
		SchemaVersion: "1.0",
		ID:            "recalled-memories",
		Role:          protocol.RoleSystem,
		Addr:          addr,
		Content:       []protocol.ContentPart{part},
	}, true
}

// toolSpecsFrom converts the registry's tool descriptors into provider tool
// specs exposed to the model via the tool-calling API.
func toolSpecsFrom(reg *tools.Registry) []provider.ToolSpec {
	descriptors := reg.Descriptors()
	specs := make([]provider.ToolSpec, 0, len(descriptors))
	for _, d := range descriptors {
		specs = append(specs, provider.ToolSpec{
			Name:        d.Name,
			Description: d.Description,
			Parameters:  d.Parameters,
		})
	}
	return specs
}

func (r *Runner) Run(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (_ protocol.ChatMessage, err error) {
	// Link to the upstream trace (if the inbound address carries one) and open
	// the per-turn span.
	ctx = telemetry.LinkUpstream(ctx, addr)
	ctx, span := telemetry.StartTurn(ctx, addr)
	defer telemetry.Finish(span, &err)

	// Intercept OpenClaw-style slash commands. Built-ins short-circuit (reply
	// without calling the model); workspace commands rewrite the user message
	// into an expanded prompt and may override the model for this turn.
	rewritten, reply, done, modelOverride, allowedTools, err := r.resolveCommand(ctx, addr, user)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	if done {
		return reply, nil
	}
	user = rewritten
	model := r.model
	if modelOverride != "" {
		model = modelOverride
	}
	// A command's allowed-tools frontmatter gates this turn's tool calls.
	var perTurnApprovers []hooks.ToolApprover
	if len(allowedTools) > 0 {
		perTurnApprovers = append(perTurnApprovers, hooks.NewAllowList(allowedTools, "tool not permitted by the active command's allowed-tools"))
	}
	// The turn's OBO authority is derived by the configured Authority hook (the
	// distribution reads the inbound grant; core leaves it zero so memory uses
	// its default authority). Per-turn, never a stale foundational grant.
	var auth tools.MemoryAuthority
	if r.authorityFn != nil {
		auth = r.authorityFn(addr, user)
	}
	session, err := harness.NewSession(ctx, addr, r.store, r.registry, auth)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("start session: %w", err)
	}
	// The host may commit the inbound user turn to the store out-of-band before
	// dispatching this task (e.g. workclaw's platform-server), so a freshly-loaded
	// history can already end with it. Drop that copy before appending the
	// incoming turn so it isn't doubled — done before the newThread check so a
	// thread whose only message is the pre-committed turn still bootstraps.
	if r.dedupTrailingUser {
		if session.DropTrailingUserDuplicate(user) {
			slog.InfoContext(ctx, "history: dropped duplicate trailing user message before append (host pre-commit)", slog.String("thread", addr.ThreadID))
		}
	}
	newThread := len(session.History()) == 0
	if err := session.Append(ctx, user); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("append user message: %w", err)
	}
	bootstrap, err := r.loader.LoadBootstrap(ctx)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("load bootstrap: %w", err)
	}
	var injected []protocol.ChatMessage
	if newThread {
		if dn, ok := r.dailyNotesMessage(ctx, addr); ok {
			injected = append(injected, dn)
		}
	}
	if rm := r.recallForTurn(ctx, auth, addr, user); rm != nil {
		injected = append(injected, *rm)
	}
	streamer := newTurnStreamer(r.publisher, addr, streamMessageID(addr), r.now)
	if err := streamer.start(ctx); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_started: %w", err)
	}
	assistant, err := r.runProviderLoop(ctx, session, addr, bootstrap, streamer, injected, model, perTurnApprovers)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	if err := streamer.finalize(ctx, assistant); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_finalized: %w", err)
	}
	if r.memAutoCommitAsync {
		// Detach: never block turn completion on the commit. context.WithoutCancel
		// keeps trace values but drops the turn's cancellation/deadline so the
		// write isn't aborted when Run returns (the commit transport applies its
		// own timeout).
		go r.commitToMemory(context.WithoutCancel(ctx), auth, addr, user, assistant)
	} else {
		r.commitToMemory(ctx, auth, addr, user, assistant)
	}
	return assistant, nil
}

// recallForTurn auto-recalls thread-scoped memories and returns a system message
// to inject into the turn context, or nil. Best-effort: errors are logged.
func (r *Runner) recallForTurn(ctx context.Context, auth tools.MemoryAuthority, addr protocol.MessageAddress, user protocol.ChatMessage) *protocol.ChatMessage {
	if !r.memAutoRecall || r.memory == nil {
		return nil
	}
	query := ""
	if r.memRecallWithInput {
		query = textOf(user)
	}
	hits, err := r.memory.Recall(ctx, auth, addr.WorkspaceID, query, r.memRecallLimit)
	if err != nil {
		slog.WarnContext(ctx, "memory auto-recall failed", slog.Any("err", err))
		return nil
	}
	msg, ok := recalledMemoryMessage(hits, addr)
	if !ok {
		return nil
	}
	return &msg
}

// dailyNotesMessage loads recent workspace daily notes as an untrusted
// background-context system message (first turn of a thread only).
func (r *Runner) dailyNotesMessage(ctx context.Context, addr protocol.MessageAddress) (protocol.ChatMessage, bool) {
	if !r.dailyNotes || r.dailyNotesDir == "" {
		return protocol.ChatMessage{}, false
	}
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	text, err := dailynotes.Load(r.dailyNotesDir, now, r.dailyNotesDays)
	if err != nil {
		slog.WarnContext(ctx, "daily notes load failed", slog.Any("err", err))
		return protocol.ChatMessage{}, false
	}
	if text == "" {
		return protocol.ChatMessage{}, false
	}
	part, err := protocol.NewTextPart(text)
	if err != nil {
		return protocol.ChatMessage{}, false
	}
	return protocol.ChatMessage{
		SchemaVersion: "1.0",
		ID:            "daily-notes",
		Role:          protocol.RoleSystem,
		Addr:          addr,
		Content:       []protocol.ContentPart{part},
	}, true
}

// commitToMemory appends the turn to the thread — user + assistant, or
// assistant-only when MemoryAutoCommitAssistantOnly is set (the host persists
// the user message). Best-effort: errors are logged, never returned.
func (r *Runner) commitToMemory(ctx context.Context, auth tools.MemoryAuthority, addr protocol.MessageAddress, user, assistant protocol.ChatMessage) {
	if !r.memAutoCommit || r.memory == nil {
		return
	}
	msgs := []protocol.ChatMessage{user, assistant}
	if r.memAutoCommitAsstOnly {
		// Host owns the user-message persistence; commit only the assistant to
		// avoid duplicating the user turn in the thread.
		msgs = []protocol.ChatMessage{assistant}
	}
	if err := r.memory.AppendThreadMessages(ctx, auth, addr.WorkspaceID, addr.ThreadID, msgs); err != nil {
		slog.WarnContext(ctx, "memory auto-commit failed", slog.Any("err", err))
	}
}
