// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

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

// remoteModelStatus supplies the status-line fallback before this client has
// observed worker-owned model metadata for the current thread. Live finals and
// loaded history replace this estimate inside the TUI.
type remoteModelStatus struct{ model string }

func (r remoteModelStatus) ActiveModelName(string) string { return r.model }

// runTUIClient runs the terminal UI against an agent reached over Aether. The
// UI is unchanged: it talks to the same channel surface, only the other half of
// that channel is a gateway hop away instead of an in-process runner.
func runTUIClient(cfg appConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Before the first worker-authored message is available, prefer the shared
	// registry default over the unrelated CLI fallback. A malformed or
	// unavailable client-side registry must not prevent attachment. Once present,
	// worker-stamped per-thread model metadata is authoritative.
	statusModel := cfg.model
	registry := cfg.modelRegistry
	var registryErr error
	if registry == nil {
		registry, registryErr = loadAppModelRegistry(cfg.workspaceRoot, "")
	}
	if registryErr != nil {
		fmt.Fprintln(os.Stderr, "warning: client model status registry:", registryErr)
	} else {
		statusModel = resolveAppModel(statusModel, registry)
	}

	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr:            cfg.aetherAddr,
		Workspace:             cfg.aetherWorkspace,
		SessionWorkspace:      effectiveWorkspace("", cfg.workspaceID),
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
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	cfg = withMemoryLayerAetherTransport(cfg, client)

	st, err := openStores(ctx, cfg)
	if err != nil {
		return err
	}
	// With MemoryLayer the agent and every client read the same transcript, so
	// the client needs no projection of its own. On local files it does: the
	// agent owns the authoritative copy, and without this a restart or a thread
	// switch renders a blank conversation.
	if !st.remote {
		if cfg.dynamicWorkspaces {
			projection, ok := st.history.(aetherchan.WorkspaceHistoryProjection)
			if !ok {
				return fmt.Errorf("dynamic workspace history projection is not workspace-aware")
			}
			client.SetWorkspaceHistoryProjection(projection)
		} else {
			client.SetHistoryProjection(st.history)
		}
	}
	toolHost, err := aetherchan.NewClientToolHost(ctx, aetherchan.ClientToolHostConfig{
		WorkspaceID:   effectiveWorkspace("", cfg.workspaceID),
		WorkspaceRoot: cfg.workspaceRoot,
		StateDir:      cfg.workspaceIndexDir,
		ToolHostID:    client.ToolHostID(),
		AgentTopic:    client.AgentTopic(),
		Publisher:     st.workspaceViews,
		Python:        "python3",
	})
	if err != nil {
		return fmt.Errorf("client workspace tool host: %w", err)
	}
	client.SetToolHost(toolHost)
	go renewAetherWorkspaceViews(ctx, "client", toolHost)

	modeStateDir := workspaceStateDir(cfg)
	shellPreferences, err := openTUIShellPreferences(cfg)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "agent-harness | connected to %s via aether %s (history: %s)\n",
		client.AgentTopic(), cfg.aetherAddr, historyLabel(st))

	return tui.Run(ctx, tui.Config{
		Channel:   client,
		Store:     st.history,
		Index:     st.threads,
		Approvals: client,
		Canceller: client,
		// No local runner to ask, so this is only the footer's initial fallback.
		// Worker-stamped final messages update the authoritative per-thread value.
		ModelStatus:       remoteModelStatus{model: statusModel},
		TaskStore:         store.NewTaskStateStore(modeStateDir),
		TeamStore:         team.NewFileGraphStore(filepath.Join(modeStateDir, "team", "graph.json")),
		AgentCatalog:      st.agentCatalog,
		DirectoryAccess:   toolHost,
		ExecutionBindings: toolHost,
		WorkspaceResolver: func() tui.DirectoryWorkspaceResolver {
			if cfg.dynamicWorkspaces {
				return toolHost
			}
			return nil
		}(),
		InitialThreadID:    cfg.thread,
		InitialWorkspaceID: cfg.workspaceID,
		WorkspaceRoot:      cfg.workspaceRoot,
		RetainReasoning:    cfg.tuiRetainReasoning,
		UserID:             cfg.aetherUser,
		ShellTriggerAgent:  cfg.tuiShellTriggerAgent,
		ShellPreferences:   shellPreferences,
	})
}
