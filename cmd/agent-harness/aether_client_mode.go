package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/channels/tui"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/team"
)

// remoteModelStatus reports the model for the status line when there is no local
// runner to ask. The agent picks the real model; this is the client's best
// guess, which is why it is only a label.
type remoteModelStatus struct{ model string }

func (r remoteModelStatus) ActiveModelName(string) string { return r.model }

// runTUIClient runs the terminal UI against an agent reached over Aether. The
// UI is unchanged: it talks to the same channel surface, only the other half of
// that channel is a gateway hop away instead of an in-process runner.
func runTUIClient(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr:            cfg.aetherAddr,
		Workspace:             cfg.aetherWorkspace,
		AgentSpecifier:        cfg.aetherSpecifier,
		UserID:                cfg.aetherUser,
		WindowID:              cfg.aetherWindow,
		APIKey:                os.Getenv("AETHER_API_KEY"),
		Token:                 os.Getenv("AETHER_TOKEN"),
		Tenant:                os.Getenv("AETHER_TENANT"),
		TLSEnabled:            cfg.aetherTLS,
		TLSInsecureSkipVerify: cfg.aetherTLSInsecure,
	})
	if err != nil {
		return err
	}

	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	// With MemoryLayer the agent and every client read the same transcript, so
	// the client needs no projection of its own. On local files it does: the
	// agent owns the authoritative copy, and without this a restart or a thread
	// switch renders a blank conversation.
	if !st.remote {
		client.SetHistoryProjection(st.history)
	}
	agentCatalog, err := agentCatalogForWorkspace(cfg.workspaceRoot)
	if err != nil {
		return err
	}

	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	modeStateDir := workspaceStateDir(cfg)

	fmt.Fprintf(os.Stderr, "agent-harness | connected to %s via aether %s (history: %s)\n",
		client.AgentTopic(), cfg.aetherAddr, historyLabel(cfg))

	return tui.Run(ctx, tui.Config{
		Channel:   client,
		Store:     st.history,
		Index:     st.threads,
		Approvals: client,
		Canceller: client,
		// No local runner to ask, so the status line shows the configured model
		// and the command palette offers only the UI's own commands.
		ModelStatus:     remoteModelStatus{model: cfg.model},
		TaskStore:       store.NewTaskStateStore(modeStateDir),
		TeamStore:       team.NewFileGraphStore(filepath.Join(modeStateDir, "team", "graph.json")),
		AgentCatalog:    agentCatalog,
		InitialThreadID: cfg.thread,
		WorkspaceRoot:   cfg.workspaceRoot,
	})
}
