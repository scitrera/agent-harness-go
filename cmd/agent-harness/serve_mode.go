package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/runtime"
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

	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	sessionTransport, err := newSessionTransport(
		st.history, workspaceResolver, wireWorkspaceID,
		hasAdditionalVisibleWorkspace(wireWorkspaceID, cfg.visibleWorkspaces), ch, ch,
	)
	if err != nil {
		return err
	}
	ch.SetSessionService(sessionTransport.Coordinator)
	runner, _, err := buildRunner(cfg, st, sessionTransport.Publisher, broker, nil)
	if err != nil {
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

	rt, err := runtime.NewRunner(ch, runner)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	rt.SetCanceller(canceller)

	if err := ch.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	fmt.Fprintf(os.Stderr, "agent-harness | model=%s endpoint=%s - serving on aether %s as %s (history: %s)\n",
		cfg.model, cfg.baseURL, cfg.aetherAddr, ch.Topic(), historyLabel(cfg))

	if _, err := rt.RunLoop(ctx, runtime.LoopConfig{Concurrency: 8}); err != nil && ctx.Err() == nil {
		return fmt.Errorf("run loop: %w", err)
	}
	return nil
}
