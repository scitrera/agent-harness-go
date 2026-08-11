package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/sessionlog"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// runAetherStandalone runs both halves in one process: the agent worker in the
// background and the terminal UI in the foreground, talking to each other
// through the gateway rather than through a Go channel.
//
// It is the same split deployment as `--serve` plus `--tui --aether`, minus the
// second terminal — useful to see the real transport in the loop without
// standing up two processes. The two halves still exchange every turn over
// Aether, so nothing about the path is special-cased.
func runAetherStandalone(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A per-process specifier keeps two standalone runs on one gateway from
	// answering each other's turns.
	if cfg.aetherSpecifier == "" {
		specifier, err := ids.New("standalone-")
		if err != nil {
			return fmt.Errorf("specifier: %w", err)
		}
		cfg.aetherSpecifier = specifier
	}

	wireWorkspaceID := effectiveWorkspace("", cfg.workspaceID)
	workspaceResolver, err := newSessionWorkspaceResolver(wireWorkspaceID, cfg.visibleWorkspaces)
	if err != nil {
		return err
	}
	broker := approval.New()
	worker, err := aetherchan.New(aetherchan.Config{
		ServerAddr:             cfg.aetherAddr,
		Workspace:              cfg.aetherWorkspace,
		SessionWorkspace:       wireWorkspaceID,
		WorkspaceResolver:      workspaceResolver,
		Specifier:              cfg.aetherSpecifier,
		SourceAgent:            cfg.sourceAgent(),
		APIKey:                 os.Getenv("AETHER_API_KEY"),
		Token:                  os.Getenv("AETHER_TOKEN"),
		Tenant:                 os.Getenv("AETHER_TENANT"),
		TLSEnabled:             cfg.aetherTLS,
		TLSInsecureSkipVerify:  cfg.aetherTLSInsecure,
		PreferTaskMessageLanes: cfg.aetherTaskMessageLanes,
	})
	if err != nil {
		return err
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = worker.Close() }()
	cfg = withMemoryLayerAetherTransport(cfg, worker)
	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	if st.workspaceViews != nil {
		worker.SetExecutionBindingAuthorizer(st.workspaceViews)
	}
	workerToolHost, err := configureAetherWorkerViews(ctx, worker, cfg, st.workspaceViews)
	if err != nil {
		return err
	}
	blobs, err := worker.BlobStore(0)
	if err != nil {
		return err
	}
	st, err = withCASLifecycle(st, blobs)
	if err != nil {
		return err
	}
	eventLog, err := sessionlog.NewCASEventLog(sessionlog.CASEventLogConfig{Blobs: blobs})
	if err != nil {
		return err
	}
	sessionTransport, err := newSessionTransport(
		cfg.stateDir, st, eventLog, workspaceResolver, wireWorkspaceID,
		hasAdditionalVisibleWorkspace(wireWorkspaceID, cfg.visibleWorkspaces), worker, worker,
	)
	if err != nil {
		return err
	}
	worker.SetSessionService(sessionTransport.Coordinator)
	subagentTasks, err := aetherSubagentTaskBackend(worker, cfg)
	if err != nil {
		return err
	}
	authorityHandoff := authhandoff.New()
	if err := enableAetherScheduledTurns(ctx, worker, workerToolHost, authorityHandoff, cfg); err != nil {
		return err
	}
	continuationBackend, err := worker.EnableGoalContinuations(st.continuations, authorityHandoff, 0)
	if err != nil {
		return err
	}
	scheduledOperations, err := buildAetherScheduledOperations(worker, st)
	if err != nil {
		return err
	}
	runner, _, err := buildRunner(cfg, st, sessionTransport.Publisher, broker, subagentTasks, worker, continuationBackend, authorityHandoff, worker.TurnContext, scheduledOperations)
	if err != nil {
		return err
	}
	if err := enableAetherSubagentExecutor(worker, runner, st.agentCatalog, cfg); err != nil {
		return err
	}
	canceller := turncancel.New()
	worker.SetCanceller(canceller)
	worker.SetApprovalBroker(broker)
	worker.SetThreadClearer(func(addr protocol.MessageAddress) error {
		if _, err := sessionTransport.Coordinator.ResetSession(context.Background(), addr.WorkspaceID, addr.ThreadID); err != nil {
			return err
		}
		return deleteAddressHistory(context.Background(), st.history, addr)
	})
	rt, err := runtime.NewRunner(worker, withAetherTaskLifecycle(worker, runner))
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)
	if err := worker.ReconcileGoalContinuations(ctx); err != nil {
		return fmt.Errorf("goal continuation recovery: %w", err)
	}
	if err := worker.ReconcileScheduledTurns(ctx); err != nil {
		return fmt.Errorf("scheduled turn recovery: %w", err)
	}
	if err := startAetherTurnRecovery(ctx, worker, runner); err != nil {
		return err
	}

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "run loop: %v\n", err)
		}
	}()

	uiErr := runTUIClient(cfg)

	stop()
	select {
	case <-workerDone:
	case <-time.After(shutdownTimeout):
		fmt.Fprintln(os.Stderr, "shutdown: worker did not drain in-flight turn within timeout")
	}
	return uiErr
}
