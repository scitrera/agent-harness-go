// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestDefaultHistoryStoreMapsResolvedDefaultToLegacyKeys(t *testing.T) {
	ctx := context.Background()
	base := &legacyHistory{threads: map[string][]protocol.ChatMessage{}}
	bound, err := BindDefaultHistory(base, "default")
	if err != nil {
		t.Fatal(err)
	}
	message := testMessage("message-1", protocol.RoleUser, "", "session-1", "hello")
	if err := bound.SaveWorkspaceHistory(ctx, "default", "session-1", []protocol.ChatMessage{message}); err != nil {
		t.Fatal(err)
	}
	if len(base.threads["session-1"]) != 1 {
		t.Fatalf("legacy history = %#v", base.threads)
	}
	loaded, err := bound.LoadWorkspaceHistory(ctx, "default", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].ID != message.ID {
		t.Fatalf("loaded = %#v", messageIDs(loaded))
	}
	if _, err := bound.LoadWorkspaceHistory(ctx, "project-b", "session-1"); !errors.Is(err, ErrWorkspaceUnavailable) {
		t.Fatalf("other workspace error = %v", err)
	}
}

func TestDefaultHistoryStorePassesAdditionalScopedWorkspacesThrough(t *testing.T) {
	ctx := context.Background()
	base := harness.NewMemoryStore()
	bound, err := BindDefaultHistory(base, "project-a")
	if err != nil {
		t.Fatal(err)
	}
	message := testMessage("message-b", protocol.RoleUser, "project-b", "shared", "hello B")
	if err := bound.SaveWorkspaceHistory(ctx, "project-b", "shared", []protocol.ChatMessage{message}); err != nil {
		t.Fatal(err)
	}
	loaded, err := base.LoadWorkspaceHistory(ctx, "project-b", "shared")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].ID != message.ID {
		t.Fatalf("scoped history = %#v", messageIDs(loaded))
	}
}

type legacyHistory struct {
	threads map[string][]protocol.ChatMessage
}

func (s *legacyHistory) LoadHistory(_ context.Context, sessionID string) ([]protocol.ChatMessage, error) {
	return append([]protocol.ChatMessage(nil), s.threads[sessionID]...), nil
}

func (s *legacyHistory) SaveHistory(_ context.Context, sessionID string, messages []protocol.ChatMessage) error {
	s.threads[sessionID] = append([]protocol.ChatMessage(nil), messages...)
	return nil
}
