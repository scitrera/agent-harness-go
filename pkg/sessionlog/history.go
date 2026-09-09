// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// DefaultHistoryStore gives a legacy single-workspace HistoryStore a resolved
// wire identity without moving its on-disk keys. If Base also implements
// harness.WorkspaceHistoryStore, non-default workspaces pass through unchanged.
type DefaultHistoryStore struct {
	base      harness.HistoryStore
	defaultID string
}

// BindDefaultHistory maps defaultID to Base's compatibility Load/Save methods.
// This lets legacy single-workspace deployments answer spec attach requests as
// a resolved "default" workspace while retaining their existing storage layout.
func BindDefaultHistory(base harness.HistoryStore, defaultID string) (*DefaultHistoryStore, error) {
	if base == nil {
		return nil, errors.New("sessionlog: history store is required")
	}
	defaultID = strings.TrimSpace(defaultID)
	if defaultID == "" {
		return nil, fmt.Errorf("%w: default workspace is required", ErrWorkspaceUnavailable)
	}
	return &DefaultHistoryStore{base: base, defaultID: defaultID}, nil
}

func (s *DefaultHistoryStore) LoadHistory(ctx context.Context, sessionID string) ([]protocol.ChatMessage, error) {
	return s.base.LoadHistory(ctx, sessionID)
}

func (s *DefaultHistoryStore) SaveHistory(ctx context.Context, sessionID string, messages []protocol.ChatMessage) error {
	return s.base.SaveHistory(ctx, sessionID, messages)
}

func (s *DefaultHistoryStore) LoadWorkspaceHistory(ctx context.Context, workspaceID, sessionID string) ([]protocol.ChatMessage, error) {
	if workspaceID == s.defaultID {
		return s.base.LoadHistory(ctx, sessionID)
	}
	if scoped, ok := s.base.(harness.WorkspaceHistoryStore); ok {
		return scoped.LoadWorkspaceHistory(ctx, workspaceID, sessionID)
	}
	return nil, fmt.Errorf("%w: %q", ErrWorkspaceUnavailable, workspaceID)
}

func (s *DefaultHistoryStore) SaveWorkspaceHistory(ctx context.Context, workspaceID, sessionID string, messages []protocol.ChatMessage) error {
	if workspaceID == s.defaultID {
		return s.base.SaveHistory(ctx, sessionID, messages)
	}
	if scoped, ok := s.base.(harness.WorkspaceHistoryStore); ok {
		return scoped.SaveWorkspaceHistory(ctx, workspaceID, sessionID, messages)
	}
	return fmt.Errorf("%w: %q", ErrWorkspaceUnavailable, workspaceID)
}

var _ WorkspaceHistoryStore = (*DefaultHistoryStore)(nil)
