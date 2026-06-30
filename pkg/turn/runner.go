package turn

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/commands"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/dailynotes"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
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
	modelRegistry     *modelpkg.Registry
	modelSelector     modelpkg.Selector
	// threadModels holds the per-thread model pinned via /model <name>
	// (runner-lifetime; reset when the runner is rebuilt or the process restarts).
	// Guarded by threadModelsMu.
	threadModels   map[string]string
	threadModelsMu sync.Mutex

	maxToolIterations   int
	maxModelAttempts    int
	maxTransientRetries int
	retryBackoff        func(attempt int) time.Duration
	streaming           bool
	streamFlush         time.Duration
	toolSpecs           []provider.ToolSpec

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

	dynamicTools    DynamicToolProvider
	staticToolNames map[string]struct{}
	attachments     AttachmentResolver

	approvals       approval.Awaiter
	approvalGranter ApprovalGranter
	approvalTimeout time.Duration
	approvalScopes  []string
	grantStore      tools.GrantStore
}

// ApprovalGranter records tool authorizations the user grants via the approval
// flow. Implemented by the distribution's dynamic policy (tools.DynamicPolicy).
type ApprovalGranter interface {
	GrantSession(workspaceID, tool string)
	GrantAlways(ctx context.Context, workspaceID, tool string) error
}

// AuthorityFunc derives a turn's OBO authority from the inbound address+message.
// Core leaves it unset (zero authority → memory uses its default); the Scitrera
// distribution wires one that reads the inbound grant.
type AuthorityFunc func(addr protocol.MessageAddress, user protocol.ChatMessage) tools.MemoryAuthority

// DynamicToolProvider supplies tools discovered per-turn from an external source
// (e.g. the platform-bridge tool registry) rather than the static registry. The
// runner calls Discover once per turn to assemble the model-visible tool list,
// then routes invocations of any discovered tool (one not in the static
// registry) to Invoke. Discover is best-effort: an error degrades the turn to
// the static tool set. The per-turn OBO authority is on ctx
// (tools.MemoryAuthorityFrom) and on req.Authority.
type DynamicToolProvider interface {
	Discover(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) ([]tools.Descriptor, error)
	Invoke(ctx context.Context, req tools.Request) (tools.Result, error)
}

// turnTools is the per-turn model-visible tool set: the static specs plus any
// dynamically-discovered specs, and the set of dynamic tool names so the invoke
// path can route them to the DynamicToolProvider (static tools win on collision).
type turnTools struct {
	specs        []provider.ToolSpec
	dynamicNames map[string]struct{}
}

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
	// ModelRegistry is the set of available models + default. When set, the runner
	// selects the per-turn model from it (capability-matched), honoring an
	// explicit /command override first. nil → the single Model is always used.
	ModelRegistry *modelpkg.Registry
	// ModelSelector picks the per-turn model from ModelRegistry on the auto path
	// (an explicit override wins). nil with a non-nil ModelRegistry →
	// model.CapabilityDefault. The distribution plugs in a cost/complexity router
	// here (policy stays out of oss).
	ModelSelector     modelpkg.Selector
	// MaxModelAttempts bounds how many distinct models a single turn may try
	// before surfacing the error (the initial model plus cross-model fallbacks).
	// Only relevant with a ModelSelector that opts into fallback — oss's
	// CapabilityDefault never does. <=0 → default 3.
	MaxModelAttempts int
	// MaxTransientRetries bounds same-model backoff retries for a transient
	// provider failure (rate_limit/server/overloaded/timeout/network) before
	// escalating to a fallback model or surfacing the error. 0 → default 2;
	// negative → disabled (surface immediately, the pre-safety-net behavior).
	MaxTransientRetries int
	// RetryBackoff returns the wait before transient retry attempt n (1-based).
	// nil → an exponential default (250ms, doubling, capped at 8s). The wait is
	// context-aware (a cancelled ctx aborts it).
	RetryBackoff      func(attempt int) time.Duration
	MaxToolIterations int
	Streaming         bool
	// StreamFlushInterval coalesces streamed token deltas: deltas are buffered and
	// emitted as one token_delta at most once per interval (always on the first
	// delta; flushed before any other stream event and at finalize). This bounds
	// the per-turn message rate so a fast stream stays under the gateway's
	// per-identity quota. 0 (default) emits every delta immediately.
	StreamFlushInterval time.Duration

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

	// Approval flow (human-in-the-loop). When Approvals is set, a tool the policy
	// gates with "requires approval" emits an approval_request part and blocks for
	// an approve/deny control instead of erroring. ApprovalGranter records
	// session/always grants; ApprovalTimeout bounds the wait (0 = until ctx);
	// ApprovalScopes advertises offered scopes (default once/session/always).
	Approvals       approval.Awaiter
	ApprovalGranter ApprovalGranter
	ApprovalTimeout time.Duration
	ApprovalScopes  []string

	// GrantStore lets the approval slow-path consult durable "always" grants
	// before prompting: when a tool is already durably authorized for the
	// workspace, the runner records a session grant and runs it without
	// emitting an approval_request. Optional; nil → prompt every time (today's
	// behavior). Typically the same store backing ApprovalGranter's
	// GrantAlways persistence.
	GrantStore tools.GrantStore

	// DynamicTools, when set, supplies per-turn tools discovered from an external
	// source (e.g. the platform-bridge registry, queried with the user's message
	// for the top-N relevant tools). The runner merges them into the turn's
	// model-visible tool set and routes their invocations to the provider.
	// Optional; nil → static tools only. Independent of any always-on meta-tools
	// the distribution may register statically (e.g. search_tools/call_tool), so
	// auto-discovery can be disabled while leaving explicit discovery in place.
	DynamicTools DynamicToolProvider

	// Attachments, when set, resolves multimodal parts the provider cannot fetch
	// itself (vfs_ref-only image/file parts) into a model-deliverable carrier,
	// applied to the assembled request just before each provider call. Optional;
	// nil → NoopAttachmentResolver (logs undeliverable attachments, resolves
	// nothing). The sahara distribution wires a data-connectors-backed resolver.
	Attachments AttachmentResolver
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
	staticSpecs := toolSpecsFrom(cfg.Registry)
	staticNames := make(map[string]struct{}, len(staticSpecs))
	for _, s := range staticSpecs {
		staticNames[s.Name] = struct{}{}
	}
	attachments := cfg.Attachments
	if attachments == nil {
		attachments = NoopAttachmentResolver{}
	}
	// Per-turn model selection: with a registry but no explicit selector, use the
	// neutral capability-matching default. No registry → single configured model.
	modelSelector := cfg.ModelSelector
	if modelSelector == nil && cfg.ModelRegistry != nil {
		modelSelector = modelpkg.CapabilityDefault{}
	}
	maxModelAttempts := cfg.MaxModelAttempts
	if maxModelAttempts <= 0 {
		maxModelAttempts = 3
	}
	maxTransientRetries := cfg.MaxTransientRetries
	switch {
	case maxTransientRetries == 0:
		maxTransientRetries = 2 // default on (bounded); closes the documented transient-retry gap
	case maxTransientRetries < 0:
		maxTransientRetries = 0 // explicitly disabled
	}
	retryBackoff := cfg.RetryBackoff
	if retryBackoff == nil {
		retryBackoff = defaultRetryBackoff
	}
	return &Runner{
		store:                 cfg.Store,
		loader:                cfg.Loader,
		registry:              cfg.Registry,
		provider:              cfg.Provider,
		publisher:             cfg.Publisher,
		ctxMgr:                ctxMgr,
		model:                 cfg.Model,
		modelRegistry:         cfg.ModelRegistry,
		modelSelector:         modelSelector,
		threadModels:          map[string]string{},
		maxToolIterations:     cfg.MaxToolIterations,
		streaming:             cfg.Streaming,
		streamFlush:           cfg.StreamFlushInterval,
		maxModelAttempts:      maxModelAttempts,
		maxTransientRetries:   maxTransientRetries,
		retryBackoff:          retryBackoff,
		toolSpecs:             staticSpecs,
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
		approvals:             cfg.Approvals,
		approvalGranter:       cfg.ApprovalGranter,
		approvalTimeout:       cfg.ApprovalTimeout,
		approvalScopes:        cfg.ApprovalScopes,
		grantStore:            cfg.GrantStore,
		dynamicTools:          cfg.DynamicTools,
		staticToolNames:       staticNames,
		attachments:           attachments,
	}, nil
}

// assembleTurnTools builds the model-visible tool set for a turn: the static
// specs plus any tools the DynamicToolProvider surfaces for this address/user
// (e.g. the platform-bridge registry's top-N matches for the user's message).
// Best-effort — a discovery error degrades to the static set. Static tools win
// on name collision so a remote tool can never shadow a built-in.
func (r *Runner) assembleTurnTools(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) turnTools {
	tt := turnTools{specs: r.toolSpecs}
	if r.dynamicTools == nil {
		return tt
	}
	descs, err := r.dynamicTools.Discover(ctx, addr, user)
	if err != nil {
		slog.WarnContext(ctx, "dynamic tool discovery failed; using static tools only", slog.Any("err", err))
		return tt
	}
	if len(descs) == 0 {
		return tt
	}
	specs := make([]provider.ToolSpec, len(r.toolSpecs), len(r.toolSpecs)+len(descs))
	copy(specs, r.toolSpecs)
	names := make(map[string]struct{}, len(descs))
	for _, d := range descs {
		if _, isStatic := r.staticToolNames[d.Name]; isStatic {
			continue // a built-in tool always wins the name
		}
		if _, dup := names[d.Name]; dup {
			continue
		}
		names[d.Name] = struct{}{}
		specs = append(specs, provider.ToolSpec{Name: d.Name, Description: d.Description, Parameters: d.Parameters})
	}
	tt.specs = specs
	tt.dynamicNames = names
	return tt
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
	// Log inbound multimodal sources at turn entry. Attachments arrive on the user
	// message as image/file parts carrying a vfs_ref, uri, or inline data_uri; the
	// harness has no VFS resolver, so a vfs_ref-only attachment never reaches the
	// model. Surfacing the carrier breakdown here makes that diagnosable.
	if mm := summarizeMultimodal([]protocol.ChatMessage{user}); mm.any() {
		slog.InfoContext(ctx, "turn: inbound multimodal attachments",
			slog.String("thread", addr.ThreadID),
			slog.Int("images", mm.images),
			slog.Int("files", mm.files),
			slog.Int("data_uri", mm.dataURI),
			slog.Int("uri", mm.uri),
			slog.Int("vfs_ref", mm.vfsRef),
			slog.Int("unresolved", mm.unresolved),
		)
	}
	model := r.resolveTurnModel(ctx, addr, user, modelOverride)
	slog.InfoContext(ctx, "turn: model selected",
		slog.String("model", model),
		slog.Bool("override", modelOverride != ""),
	)
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
	// Carry the per-turn OBO on the turn ctx so ctx-based consumers (the durable
	// tool-grant store, an authority-aware history store) act under the user's
	// grant. Tools also receive it via req.Authority on the session; this covers
	// the ctx path.
	ctx = tools.WithMemoryAuthority(ctx, auth)
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
	streamer := newTurnStreamer(r.publisher, addr, streamMessageID(addr), r.now, r.streamFlush)
	if err := streamer.start(ctx); err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_started: %w", err)
	}
	// Per-turn part emitter: lets tools (e.g. todo_write) surface a content part
	// on this message's stream and have it folded into the finalized message.
	emitter := newTurnPartEmitter(streamer)
	ctx = tools.WithPartEmitter(ctx, emitter)
	// Per-turn tool set: static tools plus any dynamically-discovered ones (the
	// DynamicToolProvider is queried with the user's message for relevant tools).
	tt := r.assembleTurnTools(ctx, addr, user)
	assistant, err := r.runProviderLoop(ctx, session, addr, user, bootstrap, streamer, injected, model, perTurnApprovers, tt)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	// Fold tool-emitted durable parts (e.g. the latest todo checklist) into the
	// finalized + committed message so they persist and reload with history.
	// finalize reconstructs the full turn (text + tool_call/tool_result +
	// image/file + todo …) from the streamed events and returns the canonical
	// message; use it for the commit + return so persisted history matches what
	// the user saw stream in (id-bearing durable parts already streamed are
	// deduped by id, so this append is a no-op on the streaming path).
	assistant.Content = append(assistant.Content, emitter.durableParts()...)
	finalized, err := streamer.finalize(ctx, assistant)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("publish message_finalized: %w", err)
	}
	assistant = finalized
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

// resolveTurnModel picks the model for this turn: an explicit command/override
// wins; otherwise the configured ModelSelector chooses from the registry
// (capability-matched). With no registry/selector the single configured model is
// used (unchanged single-model behavior).
func (r *Runner) resolveTurnModel(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, override string) string {
	if override != "" {
		return override
	}
	req := requiredCapabilities(user)
	// A model pinned via /model <name> wins for the thread, as long as it can
	// handle this turn's modality; otherwise fall through to auto-routing so an
	// image (etc.) still reaches a capable model.
	if sticky := r.stickyModel(addr.ThreadID); sticky != "" {
		if m, ok := r.modelRegistry.Get(sticky); ok && m.Capabilities.Satisfies(req) {
			return sticky
		}
		slog.InfoContext(ctx, "pinned model cannot satisfy this turn; auto-routing",
			slog.String("pinned", sticky))
	}
	if r.modelSelector == nil || r.modelRegistry == nil {
		return r.model
	}
	sel, err := r.modelSelector.SelectModel(ctx, modelpkg.SelectInput{
		Addr:     addr,
		User:     user,
		Required: req,
		Registry: r.modelRegistry,
		Default:  r.model,
	})
	if err != nil {
		slog.WarnContext(ctx, "model selection failed; using default",
			slog.String("default", r.model), slog.Any("err", err))
		return r.model
	}
	if sel == "" {
		return r.model
	}
	return sel
}

// requiredCapabilities derives the capabilities this turn needs: Tools is always
// required (the harness runs a tool loop); Vision when the message carries images.
func requiredCapabilities(user protocol.ChatMessage) modelpkg.Capabilities {
	req := modelpkg.Capabilities{Tools: true}
	for _, p := range user.Content {
		if p.Type() == protocol.ContentImage {
			req.Vision = true
		}
	}
	return req
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
