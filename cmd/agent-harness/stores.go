package main

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/memorylayer"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
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
	// remote reports whether transcripts live outside this process, which is
	// what makes them visible to other clients.
	remote bool
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
	return &boundMemoryService{base: base, workspaceID: workspaceID, backendWorkspaceID: backendWorkspaceID}
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
func historyLabel(cfg appConfig) string {
	if cfg.memorylayerURL != "" {
		return "memorylayer " + cfg.memorylayerURL
	}
	return "local files"
}

// openStores resolves the history and thread registry for a mode: MemoryLayer
// when configured, otherwise local files.
func openStores(ctx context.Context, cfg appConfig) (stores, error) {
	files := store.NewFileStore(cfg.workspaceRoot, cfg.stateDir)
	if cfg.memorylayerURL == "" {
		index, err := threadindex.NewIndex(workspaceStateDir(cfg), time.Now)
		if err != nil {
			return stores{}, fmt.Errorf("threads: %w", err)
		}
		return stores{history: bindHistory(files, cfg.workspaceID), threads: index, files: files}, nil
	}

	ml, err := memorylayer.New(memorylayer.Config{
		BaseURL:   cfg.memorylayerURL,
		APIKey:    cfg.memorylayerKey,
		Workspace: cfg.memorylayerWorkspace,
	})
	if err != nil {
		return stores{}, err
	}
	// Populate the thread list once up front: List() is served from cache
	// because the UI calls it while rendering.
	if err := ml.Refresh(ctx); err != nil {
		return stores{}, fmt.Errorf("memorylayer: load threads: %w", err)
	}
	recaller, err := memorylayer.NewRecaller(memorylayer.Config{
		BaseURL:   cfg.memorylayerURL,
		APIKey:    cfg.memorylayerKey,
		Workspace: cfg.memorylayerWorkspace,
	})
	if err != nil {
		return stores{}, err
	}
	return stores{
		history: bindHistoryBackend(ml, cfg.workspaceID, cfg.memorylayerWorkspace),
		threads: ml,
		files:   files,
		memory:  bindMemory(recaller, cfg.workspaceID, cfg.memorylayerWorkspace),
		remote:  true,
	}, nil
}
