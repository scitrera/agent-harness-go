package main

import (
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/sessionlog"
)

type sessionTransport struct {
	Coordinator *sessionlog.Coordinator
	Publisher   *sessionlog.RecordingPublisher
}

func hasAdditionalVisibleWorkspace(defaultWorkspace string, visible []string) bool {
	for _, workspaceID := range visible {
		if workspaceID != "" && workspaceID != defaultWorkspace {
			return true
		}
	}
	return false
}

func newSessionWorkspaceResolver(defaultWorkspace string, visible []string) (*sessionlog.StaticWorkspaceResolver, error) {
	resolver, err := sessionlog.NewStaticWorkspaceResolver(defaultWorkspace, visible)
	if err != nil {
		return nil, fmt.Errorf("session workspace: %w", err)
	}
	return resolver, nil
}

func newSessionTransport(
	stateDir string,
	st stores,
	workspaces sessionlog.WorkspaceResolver,
	defaultWorkspace string,
	multiWorkspace bool,
	next channel.Publisher,
	live sessionlog.SessionEventPublisher,
) (*sessionTransport, error) {
	attachHistory, err := sessionlog.BindDefaultHistory(st.history, defaultWorkspace)
	if err != nil {
		return nil, fmt.Errorf("session history: %w", err)
	}
	eventLog, err := sessionlog.NewFileEventLog(sessionlog.FileEventLogConfig{StateDir: stateDir})
	if err != nil {
		return nil, fmt.Errorf("session events: %w", err)
	}
	state, err := sessionlog.NewLifecycleStateProvider(sessionlog.LifecycleStateProviderConfig{
		Subagents: st.subagents,
		Goals:     st.goals,
	})
	if err != nil {
		return nil, fmt.Errorf("session lifecycle state: %w", err)
	}
	capabilities := spec.DefaultSessionCapabilities()
	capabilities = append(capabilities, state.Capabilities()...)
	if multiWorkspace {
		capabilities = append(capabilities, spec.SessionCapabilityMultiWorkspace)
	}
	coordinator, err := sessionlog.NewCoordinator(sessionlog.CoordinatorConfig{
		Workspaces:   workspaces,
		History:      attachHistory,
		Events:       eventLog,
		State:        state,
		Capabilities: capabilities,
	})
	if err != nil {
		return nil, fmt.Errorf("session coordinator: %w", err)
	}
	publisher, err := sessionlog.NewRecordingPublisher(sessionlog.RecordingPublisherConfig{
		Events:           eventLog,
		Next:             next,
		SessionEvents:    live,
		DefaultWorkspace: defaultWorkspace,
	})
	if err != nil {
		return nil, fmt.Errorf("session publisher: %w", err)
	}
	return &sessionTransport{Coordinator: coordinator, Publisher: publisher}, nil
}
