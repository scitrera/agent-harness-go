// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func listWorkspaceThreads(index threadindex.Store, initialWorkspaceID, workspaceID string) []threadindex.Session {
	if scoped, ok := index.(threadindex.WorkspaceStore); ok {
		return scoped.ListWorkspace(workspaceID)
	}
	if workspaceID == initialWorkspaceID {
		return index.List()
	}
	return nil
}

func refreshWorkspaceThreads(ctx context.Context, index threadindex.Store, initialWorkspaceID, workspaceID string) error {
	if scoped, ok := index.(threadindex.WorkspaceStore); ok {
		return scoped.RefreshWorkspace(ctx, workspaceID)
	}
	if workspaceID != initialWorkspaceID {
		return fmt.Errorf("thread index does not support workspace %q", workspaceID)
	}
	return nil
}

func createWorkspaceThread(index threadindex.Store, initialWorkspaceID, workspaceID string) (threadindex.Session, error) {
	if scoped, ok := index.(threadindex.WorkspaceStore); ok {
		return scoped.CreateWorkspaceThread(workspaceID)
	}
	if workspaceID != initialWorkspaceID {
		return threadindex.Session{}, fmt.Errorf("thread index does not support workspace %q", workspaceID)
	}
	return index.Create()
}

func touchWorkspaceThread(index threadindex.Store, initialWorkspaceID, workspaceID, id, text string) error {
	if scoped, ok := index.(threadindex.WorkspaceStore); ok {
		return scoped.TouchWorkspaceThread(workspaceID, id, text)
	}
	if workspaceID != initialWorkspaceID {
		return fmt.Errorf("thread index does not support workspace %q", workspaceID)
	}
	return index.Touch(id, text)
}

func renameWorkspaceThread(index threadindex.Store, initialWorkspaceID, workspaceID, id, title string) error {
	if scoped, ok := index.(threadindex.WorkspaceStore); ok {
		return scoped.RenameWorkspaceThread(workspaceID, id, title)
	}
	if workspaceID != initialWorkspaceID {
		return fmt.Errorf("thread index does not support workspace %q", workspaceID)
	}
	return index.Rename(id, title)
}

func deleteWorkspaceThread(index threadindex.Store, initialWorkspaceID, workspaceID, id string) error {
	if scoped, ok := index.(threadindex.WorkspaceStore); ok {
		return scoped.DeleteWorkspaceThread(workspaceID, id)
	}
	if workspaceID != initialWorkspaceID {
		return fmt.Errorf("thread index does not support workspace %q", workspaceID)
	}
	return index.Delete(id)
}

func loadWorkspaceHistory(ctx context.Context, store HistoryStore, initialWorkspaceID, workspaceID, threadID string) ([]protocol.ChatMessage, error) {
	if workspaceID == "" && initialWorkspaceID == "" {
		return store.LoadHistory(ctx, threadID)
	}
	if scoped, ok := store.(WorkspaceHistoryStore); ok {
		return scoped.LoadWorkspaceHistory(ctx, workspaceID, threadID)
	}
	if workspaceID != initialWorkspaceID {
		return nil, fmt.Errorf("history store does not support workspace %q", workspaceID)
	}
	return store.LoadHistory(ctx, threadID)
}

func deleteWorkspaceHistory(ctx context.Context, store HistoryStore, initialWorkspaceID, workspaceID, threadID string) error {
	if workspaceID == "" && initialWorkspaceID == "" {
		return store.DeleteHistory(ctx, threadID)
	}
	if scoped, ok := store.(WorkspaceHistoryStore); ok {
		return scoped.DeleteWorkspaceHistory(ctx, workspaceID, threadID)
	}
	if workspaceID != initialWorkspaceID {
		return fmt.Errorf("history store does not support workspace %q", workspaceID)
	}
	return store.DeleteHistory(ctx, threadID)
}

func workspaceKey(workspaceID, threadID string) string {
	if strings.TrimSpace(workspaceID) == "" {
		return threadID
	}
	return strings.TrimSpace(workspaceID) + "\x00" + threadID
}

func (m model) listThreads() []threadindex.Session {
	return listWorkspaceThreads(m.index, m.initialWorkspaceID, m.workspaceID)
}

func (m model) workspaceMatchesCurrent(workspaceID string) bool {
	return m.workspaceID == "" || workspaceID == "" || workspaceID == m.workspaceID
}
