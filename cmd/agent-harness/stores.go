package main

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/memorylayer"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turn"
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
		index, err := threadindex.NewIndex(cfg.stateDir, time.Now)
		if err != nil {
			return stores{}, fmt.Errorf("threads: %w", err)
		}
		return stores{history: files, threads: index, files: files}, nil
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
	return stores{history: ml, threads: ml, files: files, memory: recaller, remote: true}, nil
}
