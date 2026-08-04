package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	tui "github.com/scitrera/agent-harness-go/pkg/channels/tui"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/team"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

func runTUI(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := approval.New()
	tc := tui.NewChannel()
	runner, fsStore, workspace, err := buildRunner(cfg, tc, broker, nil)
	if err != nil {
		return err
	}
	index, err := threadindex.NewIndex(cfg.stateDir, time.Now)
	if err != nil {
		return fmt.Errorf("threads: %w", err)
	}
	agentCatalog, err := agentCatalogForWorkspace(cfg.workspaceRoot)
	if err != nil {
		return err
	}
	taskStore := store.NewTaskStateStore(cfg.stateDir)
	teamStore := team.NewFileGraphStore(filepath.Join(cfg.stateDir, "team", "graph.json"))
	canceller := turncancel.New()
	rt, err := runtime.NewRunner(tc, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "run loop stopped", slog.Any("err", err))
		}
	}()

	slog.InfoContext(ctx, "agent-harness TUI ready",
		slog.String("model", cfg.model),
		slog.String("endpoint", cfg.baseURL),
		slog.String("thread", cfg.thread),
	)
	runErr := tui.Run(ctx, tui.Config{
		Channel:         tc,
		Store:           fsStore,
		Index:           index,
		Approvals:       broker,
		Canceller:       canceller,
		ModelStatus:     runner,
		Commands:        runner,
		TaskStore:       taskStore,
		TeamStore:       teamStore,
		AgentCatalog:    agentCatalog,
		DirectoryAccess: workspace,
		InitialThreadID: cfg.thread,
		WorkspaceRoot:   cfg.workspaceRoot,
	})
	stop()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		slog.Warn("shutdown: run loop did not drain in-flight turn within timeout", slog.Duration("timeout", shutdownTimeout))
	}
	return runErr
}
