package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/goal"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/memorylayer"
	"github.com/scitrera/agent-harness-go/pkg/promptnotes"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/refinement"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// stores bundles where a mode keeps conversations. Both the turn runner and the
// UI take them from here, so the two cannot end up reading different
// transcripts — the failure that makes a UI look like it lost history.
//
// files is always the filesystem store: workspace bootstrap documents
// (SOUL/AGENTS/…) live on disk regardless of where transcripts go.
type stores struct {
	history historyStore
	threads threadindex.Store
	files   *store.FileStore
	// memory is the semantic-recall service, set only when MemoryLayer is
	// configured. nil leaves the turn runner's memory hooks inert.
	memory turn.MemoryService
	// catalogs is the optional workspace-aware remote catalog source. The runner
	// combines it with filesystem skills and caches compiled entries per logical
	// workspace; nil preserves filesystem-only behavior.
	catalogs catalog.WorkspaceProvider
	// promptNotes is the one explicitly selected authoritative source. nil means
	// disabled; no runtime path combines or falls back between providers.
	promptNotes promptnotes.WorkspaceProvider
	// agentCatalog is the one explicitly selected reusable subagent-definition
	// authority. A provider-backed catalog resolves the turn's logical workspace
	// at invocation time; nil preserves generic unnamed subagents.
	agentCatalog subagent.Catalog
	// refinements coordinates the selected immutable audit authority with the
	// independently selected target-resource authorities.
	refinements *refinement.Service
	// Lifecycle stores use local files by default. Aether worker modes replace
	// them with CAS-backed implementations over the same interfaces.
	subagents subagent.Registry
	goals     goal.Store
	// continuations is the append-only explanation and admission ledger for
	// host-owned bounded goal follow-ups. Aether modes replace the local file
	// ledger with CAS alongside the lifecycle projections.
	continuations goal.ContinuationLedger
	turns         turnjournal.Store
	// workspaceViews publishes client/worker-observed local checkout identities
	// to MemoryLayer when it is the configured authority. nil preserves the
	// standalone, local-only path.
	workspaceViews *memorylayer.WorkspaceViewPublisher
	// remote reports whether transcripts live outside this process, which is
	// what makes them visible to other clients.
	remote         bool
	historyBackend string
}

// withCASLifecycle replaces the local single-writer lifecycle projections with
// distributed CAS stores. Aether's stable agent identity is exclusive, so the
// subagent registry defers startup interruption recovery until the first KV
// operation after the channel has connected successfully.
func withCASLifecycle(st stores, blobs casblob.Store) (stores, error) {
	subagents, err := subagent.NewCASRegistry(subagent.CASRegistryConfig{Blobs: blobs, DeferRecovery: true})
	if err != nil {
		return stores{}, err
	}
	goals, err := goal.NewCASStore(goal.CASStoreConfig{Blobs: blobs})
	if err != nil {
		return stores{}, err
	}
	st.subagents = subagents
	st.goals = goals
	continuations, err := goal.NewCASLedger(goal.CASLedgerConfig{Blobs: blobs})
	if err != nil {
		return stores{}, err
	}
	st.continuations = continuations
	turns, err := turnjournal.NewCASStore(turnjournal.CASStoreConfig{Blobs: blobs})
	if err != nil {
		return stores{}, err
	}
	st.turns = turns
	return st, nil
}

// historyStore is the transcript surface the modes need: the runner's
// load/save plus the delete a "clear" performs.
type historyStore interface {
	harness.HistoryStore
	DeleteHistory(ctx context.Context, threadID string) error
}

type workspaceHistoryDeleter interface {
	DeleteWorkspaceHistory(ctx context.Context, workspaceID, threadID string) error
}

// boundHistoryStore lets legacy UI surfaces address the process-selected
// workspace while still allowing the turn runner to pass an explicit workspace
// through the additive harness.WorkspaceHistoryStore capability.
type boundHistoryStore struct {
	base               historyStore
	workspaceID        string
	backendWorkspaceID string
}

func bindHistory(base historyStore, workspaceID string) historyStore {
	return bindHistoryBackend(base, workspaceID, workspaceID)
}

func bindHistoryBackend(base historyStore, workspaceID, backendWorkspaceID string) historyStore {
	if workspaceID == "" {
		return base
	}
	return &boundHistoryStore{base: base, workspaceID: workspaceID, backendWorkspaceID: backendWorkspaceID}
}

func (s *boundHistoryStore) LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error) {
	return s.LoadWorkspaceHistory(ctx, s.workspaceID, threadID)
}

func (s *boundHistoryStore) SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error {
	return s.SaveWorkspaceHistory(ctx, s.workspaceID, threadID, messages)
}

func (s *boundHistoryStore) DeleteHistory(ctx context.Context, threadID string) error {
	return s.DeleteWorkspaceHistory(ctx, s.workspaceID, threadID)
}

func (s *boundHistoryStore) LoadWorkspaceHistory(ctx context.Context, workspaceID, threadID string) ([]protocol.ChatMessage, error) {
	if scoped, ok := s.base.(harness.WorkspaceHistoryStore); ok {
		return scoped.LoadWorkspaceHistory(ctx, s.backendWorkspace(workspaceID), threadID)
	}
	if !s.accepts(workspaceID) {
		return nil, fmt.Errorf("history backend is bound to workspace %q, cannot load %q", s.workspaceID, workspaceID)
	}
	return s.base.LoadHistory(ctx, threadID)
}

func (s *boundHistoryStore) SaveWorkspaceHistory(ctx context.Context, workspaceID, threadID string, messages []protocol.ChatMessage) error {
	if scoped, ok := s.base.(harness.WorkspaceHistoryStore); ok {
		return scoped.SaveWorkspaceHistory(ctx, s.backendWorkspace(workspaceID), threadID, messages)
	}
	if !s.accepts(workspaceID) {
		return fmt.Errorf("history backend is bound to workspace %q, cannot save %q", s.workspaceID, workspaceID)
	}
	return s.base.SaveHistory(ctx, threadID, messages)
}

func (s *boundHistoryStore) DeleteWorkspaceHistory(ctx context.Context, workspaceID, threadID string) error {
	if scoped, ok := s.base.(workspaceHistoryDeleter); ok {
		return scoped.DeleteWorkspaceHistory(ctx, s.backendWorkspace(workspaceID), threadID)
	}
	if !s.accepts(workspaceID) {
		return fmt.Errorf("history backend is bound to workspace %q, cannot delete %q", s.workspaceID, workspaceID)
	}
	return s.base.DeleteHistory(ctx, threadID)
}

func (s *boundHistoryStore) accepts(workspaceID string) bool {
	return workspaceID == s.workspaceID || workspaceID == s.backendWorkspaceID
}

func (s *boundHistoryStore) backendWorkspace(workspaceID string) string {
	if workspaceID == s.workspaceID {
		return s.backendWorkspaceID
	}
	return workspaceID
}

type boundMemoryService struct {
	base               turn.MemoryService
	workspaceID        string
	backendWorkspaceID string
}

func bindMemory(base turn.MemoryService, workspaceID, backendWorkspaceID string) turn.MemoryService {
	if base == nil || workspaceID == "" || workspaceID == backendWorkspaceID {
		return base
	}
	bound := &boundMemoryService{base: base, workspaceID: workspaceID, backendWorkspaceID: backendWorkspaceID}
	if registrar, ok := base.(turn.ThreadRegistrar); ok {
		return &boundMemoryRegistrar{boundMemoryService: bound, registrar: registrar}
	}
	return bound
}

func (s *boundMemoryService) backendWorkspace(workspaceID string) string {
	if workspaceID == "" || workspaceID == s.workspaceID {
		return s.backendWorkspaceID
	}
	return workspaceID
}

func (s *boundMemoryService) Recall(ctx context.Context, auth tools.MemoryAuthority, workspaceID, query string, limit int) ([]tools.MemoryHit, error) {
	return s.base.Recall(ctx, auth, s.backendWorkspace(workspaceID), query, limit)
}

func (s *boundMemoryService) AppendThreadMessages(ctx context.Context, auth tools.MemoryAuthority, workspaceID, threadID, ownership string, messages []protocol.ChatMessage) error {
	return s.base.AppendThreadMessages(ctx, auth, s.backendWorkspace(workspaceID), threadID, ownership, messages)
}

// boundMemoryRegistrar preserves the optional ThreadRegistrar capability while
// translating the logical project workspace to its configured MemoryLayer
// workspace. Keeping it separate prevents a non-registrar MemoryService from
// falsely advertising thread creation support.
type boundMemoryRegistrar struct {
	*boundMemoryService
	registrar turn.ThreadRegistrar
}

func (s *boundMemoryRegistrar) EnsureThread(ctx context.Context, auth tools.MemoryAuthority, spec turn.ThreadSpec) (string, error) {
	spec.WorkspaceID = s.backendWorkspace(spec.WorkspaceID)
	return s.registrar.EnsureThread(ctx, auth, spec)
}

func workspaceStateDir(cfg appConfig) string {
	return workspacepkg.StateDir(cfg.stateDir, cfg.workspaceID)
}

func deleteAddressHistory(ctx context.Context, history historyStore, addr protocol.MessageAddress) error {
	if scoped, ok := history.(workspaceHistoryDeleter); ok && addr.WorkspaceID != "" {
		return scoped.DeleteWorkspaceHistory(ctx, addr.WorkspaceID, addr.ThreadID)
	}
	return history.DeleteHistory(ctx, addr.ThreadID)
}

// historyLabel describes where transcripts live, for the startup banner.
func historyLabel(st stores) string {
	if st.historyBackend != "" {
		return st.historyBackend
	}
	return "local files"
}

// openStores resolves the history and thread registry for a mode: MemoryLayer
// when configured, otherwise local files.
func openStores(ctx context.Context, cfg appConfig) (stores, error) {
	files := store.NewFileStore(cfg.workspaceRoot, cfg.stateDir)
	var ml *memorylayer.Store
	if memoryLayerConfigured(cfg.memorylayerMode) {
		var err error
		ml, err = memorylayer.New(memoryLayerClientConfig(cfg))
		if err != nil {
			return stores{}, err
		}
		// Refresh doubles as the auto-discovery probe and populates the thread
		// list the UI renders from cache. Only proven service absence may fall
		// back; every other transport/protocol/server error remains fatal.
		if err := ml.Refresh(ctx); err != nil {
			if cfg.memorylayerMode == memoryLayerModeAuto && !cfg.memorylayerRequired && isAbsentMemoryLayerService(err) {
				slog.InfoContext(ctx, "MemoryLayer service not present on Aether; using local history",
					slog.String("target", cfg.memorylayerTarget))
				ml = nil
				cfg.memorylayerMode = memoryLayerModeOff
				cfg.memorylayerTransport = nil
			} else {
				return stores{}, fmt.Errorf("memorylayer: load threads: %w", err)
			}
		}
	}
	promptNoteProvider, err := openPromptNoteProvider(cfg)
	if err != nil {
		return stores{}, err
	}
	agentCatalog, err := openAgentSpecificationCatalog(cfg)
	if err != nil {
		return stores{}, err
	}
	refinements, err := openRefinementService(cfg)
	if err != nil {
		return stores{}, err
	}
	subagents, err := subagent.NewFileRegistry(cfg.stateDir)
	if err != nil {
		return stores{}, err
	}
	goals, err := goal.NewFileStore(cfg.stateDir)
	if err != nil {
		return stores{}, err
	}
	continuations, err := goal.NewFileLedger(cfg.stateDir, time.Now)
	if err != nil {
		return stores{}, err
	}
	turns, err := turnjournal.NewFileStore(cfg.stateDir)
	if err != nil {
		return stores{}, err
	}
	if ml == nil {
		index, err := threadindex.NewIndex(workspaceStateDir(cfg), time.Now)
		if err != nil {
			return stores{}, fmt.Errorf("threads: %w", err)
		}
		return stores{
			history:        bindHistory(files, cfg.workspaceID),
			threads:        index,
			files:          files,
			promptNotes:    promptNoteProvider,
			agentCatalog:   agentCatalog,
			refinements:    refinements,
			subagents:      subagents,
			goals:          goals,
			continuations:  continuations,
			turns:          turns,
			historyBackend: "local files",
		}, nil
	}

	recaller, err := memorylayer.NewRecaller(memoryLayerClientConfig(cfg))
	if err != nil {
		return stores{}, err
	}
	catalogProvider, err := memorylayer.NewCatalogProvider(memoryLayerClientConfig(cfg))
	if err != nil {
		return stores{}, err
	}
	workspaceViews, err := memorylayer.NewWorkspaceViewPublisher(memoryLayerClientConfig(cfg), ml)
	if err != nil {
		return stores{}, err
	}
	return stores{
		history:        bindHistoryBackend(ml, cfg.workspaceID, cfg.memorylayerWorkspace),
		threads:        ml,
		files:          files,
		memory:         bindMemory(recaller, cfg.workspaceID, cfg.memorylayerWorkspace),
		catalogs:       catalog.BindWorkspaceProvider(catalogProvider, cfg.workspaceID, cfg.memorylayerWorkspace),
		promptNotes:    promptNoteProvider,
		agentCatalog:   agentCatalog,
		refinements:    refinements,
		subagents:      subagents,
		goals:          goals,
		continuations:  continuations,
		turns:          turns,
		workspaceViews: workspaceViews,
		remote:         true,
		historyBackend: "memorylayer " + memoryLayerLocation(cfg),
	}, nil
}

func openRefinementService(cfg appConfig) (*refinement.Service, error) {
	hasMemoryLayer := memoryLayerConfigured(cfg.memorylayerMode)
	authority, err := normalizeRefinementAuthority(cfg.refinementAuthority, hasMemoryLayer)
	if err != nil {
		return nil, err
	}
	if authority == refinementAuthorityOff {
		return nil, nil
	}
	promptAuthority, err := normalizePromptNotesAuthority(cfg.promptNotesAuthority, hasMemoryLayer)
	if err != nil {
		return nil, err
	}
	agentAuthority, err := normalizeAgentSpecificationsAuthority(cfg.agentSpecificationsAuthority, hasMemoryLayer)
	if err != nil {
		return nil, err
	}
	var audit refinement.Store
	switch authority {
	case refinementAuthorityLocal:
		audit, err = refinement.NewFileStore(cfg.stateDir, time.Now)
	case refinementAuthorityMemoryLayer:
		audit, err = memorylayer.NewRefinementRecordStore(memoryLayerClientConfig(cfg))
		if err == nil {
			audit = refinement.BindStore(audit, cfg.workspaceID, cfg.memorylayerWorkspace)
		}
	}
	if err != nil {
		return nil, err
	}
	editors := map[refinement.ResourceKind]refinement.ResourceEditor{}
	if promptAuthority == promptNotesAuthorityLocal {
		localEditor, editorErr := promptnotes.NewFileEditor(cfg.stateDir)
		if editorErr != nil {
			return nil, editorErr
		}
		editors[refinement.ResourcePromptNote] = localEditor
	}
	if promptAuthority == promptNotesAuthorityMemoryLayer || agentAuthority == agentSpecificationsAuthorityMemoryLayer {
		editor, editorErr := memorylayer.NewRefinementResourceEditor(memoryLayerClientConfig(cfg))
		if editorErr != nil {
			return nil, editorErr
		}
		bound := refinement.BindResourceEditor(editor, cfg.workspaceID, cfg.memorylayerWorkspace)
		if promptAuthority == promptNotesAuthorityMemoryLayer {
			editors[refinement.ResourcePromptNote] = bound
		}
		if agentAuthority == agentSpecificationsAuthorityMemoryLayer {
			editors[refinement.ResourceAgentSpecification] = bound
		}
	}
	return &refinement.Service{
		Store: audit, Editors: editors,
		Policy: refinement.Policy{AllowSessionLowRiskWithoutApproval: cfg.refinementSessionAutoApply},
	}, nil
}

func openAgentSpecificationCatalog(cfg appConfig) (subagent.Catalog, error) {
	authority, err := normalizeAgentSpecificationsAuthority(cfg.agentSpecificationsAuthority, memoryLayerConfigured(cfg.memorylayerMode))
	if err != nil {
		return nil, err
	}
	switch authority {
	case agentSpecificationsAuthorityOff:
		return nil, nil
	case agentSpecificationsAuthorityLocal:
		return agentCatalogForWorkspace(cfg.workspaceRoot)
	case agentSpecificationsAuthorityMemoryLayer:
		provider, err := memorylayer.NewAgentSpecificationProvider(memoryLayerClientConfig(cfg))
		if err != nil {
			return nil, err
		}
		return subagent.NewProviderCatalog(provider, cfg.workspaceID, cfg.memorylayerWorkspace)
	default:
		panic("unreachable agent-specification authority")
	}
}

func openPromptNoteProvider(cfg appConfig) (promptnotes.WorkspaceProvider, error) {
	authority, err := normalizePromptNotesAuthority(cfg.promptNotesAuthority, memoryLayerConfigured(cfg.memorylayerMode))
	if err != nil {
		return nil, err
	}
	switch authority {
	case promptNotesAuthorityOff:
		return nil, nil
	case promptNotesAuthorityLocal:
		return promptnotes.NewFileProvider(cfg.stateDir)
	case promptNotesAuthorityMemoryLayer:
		provider, err := memorylayer.NewPromptNoteProvider(memoryLayerClientConfig(cfg))
		if err != nil {
			return nil, err
		}
		return promptnotes.BindWorkspaceProvider(provider, cfg.workspaceID, cfg.memorylayerWorkspace), nil
	default:
		panic("unreachable prompt-note authority")
	}
}
