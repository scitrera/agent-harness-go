// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package threadindex

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

var _ WorkspaceStore = (*WorkspaceIndex)(nil)

// WorkspaceIndex keeps one filesystem Index per logical workspace. The
// selected workspace retains the Store surface, while explicit workspace
// operations lazily open the isolated state directory for that workspace.
type WorkspaceIndex struct {
	stateDir           string
	defaultWorkspaceID string
	now                func() time.Time

	mu      sync.Mutex
	indexes map[string]*Index
}

// NewWorkspaceIndex creates a workspace-aware filesystem thread registry.
func NewWorkspaceIndex(stateDir, defaultWorkspaceID string, now func() time.Time) (*WorkspaceIndex, error) {
	if now == nil {
		now = time.Now
	}
	x := &WorkspaceIndex{
		stateDir:           stateDir,
		defaultWorkspaceID: strings.TrimSpace(defaultWorkspaceID),
		now:                now,
		indexes:            map[string]*Index{},
	}
	if _, err := x.index(x.defaultWorkspaceID); err != nil {
		return nil, err
	}
	return x, nil
}

func (x *WorkspaceIndex) index(workspaceID string) (*Index, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	x.mu.Lock()
	defer x.mu.Unlock()
	if index := x.indexes[workspaceID]; index != nil {
		return index, nil
	}
	index, err := NewIndex(workspacepkg.StateDir(x.stateDir, workspaceID), x.now)
	if err != nil {
		return nil, fmt.Errorf("open workspace %q thread index: %w", workspaceID, err)
	}
	x.indexes[workspaceID] = index
	return index, nil
}

func (x *WorkspaceIndex) List() []Session {
	return x.ListWorkspace(x.defaultWorkspaceID)
}

func (x *WorkspaceIndex) Create() (Session, error) {
	return x.CreateWorkspaceThread(x.defaultWorkspaceID)
}

func (x *WorkspaceIndex) Touch(id, firstUserText string) error {
	return x.TouchWorkspaceThread(x.defaultWorkspaceID, id, firstUserText)
}

func (x *WorkspaceIndex) Rename(id, titleText string) error {
	return x.RenameWorkspaceThread(x.defaultWorkspaceID, id, titleText)
}

func (x *WorkspaceIndex) Delete(id string) error {
	return x.DeleteWorkspaceThread(x.defaultWorkspaceID, id)
}

// RefreshWorkspace loads the workspace's durable index into this process.
func (x *WorkspaceIndex) RefreshWorkspace(_ context.Context, workspaceID string) error {
	_, err := x.index(workspaceID)
	return err
}

func (x *WorkspaceIndex) ListWorkspace(workspaceID string) []Session {
	index, err := x.index(workspaceID)
	if err != nil {
		return nil
	}
	return index.List()
}

func (x *WorkspaceIndex) CreateWorkspaceThread(workspaceID string) (Session, error) {
	index, err := x.index(workspaceID)
	if err != nil {
		return Session{}, err
	}
	return index.Create()
}

func (x *WorkspaceIndex) TouchWorkspaceThread(workspaceID, id, firstUserText string) error {
	index, err := x.index(workspaceID)
	if err != nil {
		return err
	}
	return index.Touch(id, firstUserText)
}

func (x *WorkspaceIndex) RenameWorkspaceThread(workspaceID, id, titleText string) error {
	index, err := x.index(workspaceID)
	if err != nil {
		return err
	}
	return index.Rename(id, titleText)
}

func (x *WorkspaceIndex) DeleteWorkspaceThread(workspaceID, id string) error {
	index, err := x.index(workspaceID)
	if err != nil {
		return err
	}
	return index.Delete(id)
}
