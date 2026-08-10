package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
	"github.com/scitrera/agent-harness-go/pkg/sessionlog"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// runServe runs the harness as a headless agent worker reachable over Aether:
// turns arrive on the agent topic and stream events go back to whichever client
// sent them. There is no local UI — frontends connect over the same gateway.
func runServe(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wireWorkspaceID := effectiveWorkspace("", cfg.workspaceID)
	workspaceResolver, err := newSessionWorkspaceResolver(wireWorkspaceID, cfg.visibleWorkspaces)
	if err != nil {
		return err
	}
	broker := approval.New()
	ch, err := aetherchan.New(aetherchan.Config{
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
	// Connect before opening stores so auto mode can probe MemoryLayer through
	// this authenticated Aether session. The channel's bounded inbox safely
	// retains any turn arriving during the short remainder of initialization.
	if err := ch.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	cfg = withMemoryLayerAetherTransport(cfg, ch)

	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	if st.workspaceViews != nil {
		ch.SetExecutionBindingAuthorizer(st.workspaceViews)
	}
	workerToolHost, err := configureAetherWorkerViews(ctx, ch, cfg, st.workspaceViews)
	if err != nil {
		return err
	}
	blobs, err := ch.BlobStore(0)
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
		hasAdditionalVisibleWorkspace(wireWorkspaceID, cfg.visibleWorkspaces), ch, ch,
	)
	if err != nil {
		return err
	}
	ch.SetSessionService(sessionTransport.Coordinator)
	subagentTasks, err := aetherSubagentTaskBackend(ch, cfg)
	if err != nil {
		return err
	}
	authorityHandoff := authhandoff.New()
	if err := enableAetherScheduledTurns(ctx, ch, workerToolHost, authorityHandoff, cfg); err != nil {
		return err
	}
	continuationBackend, err := ch.EnableGoalContinuations(st.continuations, authorityHandoff, 0)
	if err != nil {
		return err
	}
	runner, _, err := buildRunner(cfg, st, sessionTransport.Publisher, broker, subagentTasks, ch, continuationBackend, authorityHandoff, ch.TurnContext)
	if err != nil {
		return err
	}
	if err := enableAetherSubagentExecutor(ch, runner, st.agentCatalog, cfg); err != nil {
		return err
	}
	canceller := turncancel.New()
	ch.SetCanceller(canceller)
	ch.SetApprovalBroker(broker)
	// A `clear` control drops the thread's persisted transcript, so a client-side
	// clear is not resurrected from local history on the next turn.
	ch.SetThreadClearer(func(addr protocol.MessageAddress) error {
		if _, err := sessionTransport.Coordinator.ResetSession(context.Background(), addr.WorkspaceID, addr.ThreadID); err != nil {
			return err
		}
		return deleteAddressHistory(context.Background(), st.history, addr)
	})

	rt, err := runtime.NewRunner(ch, withAetherTaskLifecycle(ch, runner))
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)

	if err := ch.ReconcileGoalContinuations(ctx); err != nil {
		return fmt.Errorf("goal continuation recovery: %w", err)
	}
	if err := ch.ReconcileScheduledTurns(ctx); err != nil {
		return fmt.Errorf("scheduled turn recovery: %w", err)
	}
	if err := startAetherTurnRecovery(ctx, ch, runner); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s - serving on aether %s as %s (history: %s%s)\n",
		cfg.model, cfg.baseURL, cfg.aetherAddr, ch.Topic(), historyLabel(st), externalSubagentLabel(cfg))

	if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
		return fmt.Errorf("run loop: %w", err)
	}
	return nil
}
