// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/store"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type recordingDirectoryAccess struct {
	dirs []string
	err  error
}

type fixedExecutionBindingProvider struct {
	binding protocol.ExecutionBinding
}

type mappedDirectoryWorkspaceResolver map[string]string

func (r mappedDirectoryWorkspaceResolver) ResolveWorkspaceForDirectory(_ context.Context, dir string) (string, error) {
	workspaceID := r[dir]
	if workspaceID == "" {
		return "", fmt.Errorf("unmapped directory %s", dir)
	}
	return workspaceID, nil
}

func (p fixedExecutionBindingProvider) ExecutionBindingForDirectory(_ context.Context, _ string) (protocol.ExecutionBinding, error) {
	return p.binding, nil
}

func (r *recordingDirectoryAccess) GrantWorkingDirectory(dir string) error {
	r.dirs = append(r.dirs, dir)
	return r.err
}

func (r *recordingDirectoryAccess) GrantWorkspaceDirectory(dir string) error {
	return r.GrantWorkingDirectory(dir)
}

func TestWorkingDirectoryCommandsStayInsideWorkspace(t *testing.T) {
	root := t.TempDir()
	subdirectory := filepath.Join(root, "src")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	processCWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get process cwd: %v", err)
	}
	m := model{
		workspaceRoot: root,
		cwd:           root,
		viewport:      viewport.New(),
		composer:      newComposer(),
		tailing:       true,
	}

	next, cmd := m.handleSlash("/cd src")
	if cmd != nil {
		t.Fatal("/cd should be local")
	}
	updated := next.(model)
	if updated.cwd != subdirectory {
		t.Fatalf("cwd = %q, want %q", updated.cwd, subdirectory)
	}
	if current, getErr := os.Getwd(); getErr != nil || current != processCWD {
		t.Fatalf("process cwd changed: got %q error %v, want %q", current, getErr, processCWD)
	}

	next, cmd = updated.handleSlash("/pwd")
	if cmd != nil {
		t.Fatal("/pwd should be local")
	}
	updated = next.(model)
	if got := updated.rows[len(updated.rows)-1].Text; got != subdirectory {
		t.Fatalf("/pwd output = %q, want %q", got, subdirectory)
	}

	next, _ = updated.handleSlash("/cd ..")
	updated = next.(model)
	if updated.cwd != root {
		t.Fatalf("cwd after .. = %q, want workspace root %q", updated.cwd, root)
	}
}

func TestWorkingDirectoryCommandAllowsExplicitExternalDirectory(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	access := &recordingDirectoryAccess{}
	file := filepath.Join(root, "file.txt")
	if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	m := model{
		workspaceRoot:   root,
		cwd:             root,
		directoryAccess: access,
		viewport:        viewport.New(),
		composer:        newComposer(),
		tailing:         true,
	}

	next, _ := m.handleSlash("/cd " + outside)
	updated := next.(model)
	if updated.cwd != outside {
		t.Fatalf("external /cd cwd = %q, want %q", updated.cwd, outside)
	}
	if len(access.dirs) != 1 || access.dirs[0] != outside {
		t.Fatalf("granted directories = %+v", access.dirs)
	}

	next, _ = m.handleSlash("/cd file.txt")
	updated = next.(model)
	if updated.cwd != root {
		t.Fatalf("file /cd changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "not a directory") {
		t.Fatalf("file /cd error = %q", got)
	}
}

func TestWorkingDirectoryAllowsExternalSymlinkTarget(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	access := &recordingDirectoryAccess{}
	link := filepath.Join(root, "outside-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	m := model{
		workspaceRoot:   root,
		cwd:             root,
		directoryAccess: access,
		viewport:        viewport.New(),
		composer:        newComposer(),
		tailing:         true,
	}

	next, _ := m.handleSlash("/cd outside-link")
	updated := next.(model)
	if updated.cwd != outside {
		t.Fatalf("symlink target cwd = %q, want %q", updated.cwd, outside)
	}
	if len(access.dirs) != 1 || access.dirs[0] != outside {
		t.Fatalf("symlink target grant = %+v", access.dirs)
	}
}

func TestWorkingDirectoryKeepsPriorCWDWhenExternalGrantFails(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	m := model{
		workspaceRoot:   root,
		cwd:             root,
		directoryAccess: &recordingDirectoryAccess{err: fmt.Errorf("denied")},
		viewport:        viewport.New(),
		composer:        newComposer(),
		tailing:         true,
	}

	next, _ := m.handleSlash("/cd " + outside)
	updated := next.(model)
	if updated.cwd != root {
		t.Fatalf("failed grant changed cwd to %q", updated.cwd)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "denied") {
		t.Fatalf("grant failure = %q", got)
	}
}

func TestRemoteWorkingDirectoryUsesLogicalBindingWithoutAbsolutePathMetadata(t *testing.T) {
	root := t.TempDir()
	subdir := filepath.Join(root, "src")
	if err := os.Mkdir(subdir, 0o700); err != nil {
		t.Fatal(err)
	}
	index, err := threadindex.NewIndex(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Touch("thread-1", ""); err != nil {
		t.Fatal(err)
	}
	binding := spec.NewExecutionBinding("project-a", "view-a", "us::drew::w1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-a"
	binding.RelativeDirectory = "src"
	channel := NewChannel()
	m := model{
		ctx: context.Background(), channel: channel, index: index,
		workspaceRoot: root, cwd: subdir, threadID: "thread-1",
		executionBindings: fixedExecutionBindingProvider{binding: binding},
		viewport:          viewport.New(), composer: newComposer(),
		turns: map[string]turnActivity{}, pendingApprovals: map[string]approvalRequest{},
		tools: map[string]toolEntry{}, tailing: true,
	}
	m.composer.SetValue("inspect the project")
	_, cmd := m.sendCurrent()
	if cmd == nil {
		t.Fatal("message was not enqueued")
	}
	if result := cmd(); result.(sendResultMsg).Err != nil {
		t.Fatal(result)
	}
	inbound, err := channel.FetchTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := spec.GetExecutionBinding(inbound.Message)
	if err != nil || got == nil || got.ViewID != "view-a" || got.RelativeDirectory != "src" {
		t.Fatalf("execution binding = %+v, %v", got, err)
	}
	if _, ok := tools.MessageWorkingDirectory(inbound.Message); ok {
		t.Fatal("remote message leaked absolute working_directory metadata")
	}
	text, _ := inbound.Message.Content[0].AsText()
	if strings.Contains(text.Text, root) {
		t.Fatalf("remote prompt leaked client root: %q", text.Text)
	}
}

func TestWorkingDirectorySwitchesWorkspaceThreadAndHistoryPartition(t *testing.T) {
	ctx := context.Background()
	rootA := t.TempDir()
	rootB := t.TempDir()
	stateDir := t.TempDir()
	index, err := threadindex.NewWorkspaceIndex(stateDir, "project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.TouchWorkspaceThread("project-a", "shared", "thread A"); err != nil {
		t.Fatal(err)
	}
	if err := index.TouchWorkspaceThread("project-b", "shared", "thread B"); err != nil {
		t.Fatal(err)
	}
	history := store.NewFileStore("", stateDir)
	partA, _ := protocol.NewTextPart("history A")
	partB, _ := protocol.NewTextPart("history B")
	if err := history.SaveWorkspaceHistory(ctx, "project-a", "shared", []protocol.ChatMessage{{
		ID: "a", Role: protocol.RoleUser, Addr: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "shared"}, Content: []protocol.ContentPart{partA},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := history.SaveWorkspaceHistory(ctx, "project-b", "shared", []protocol.ChatMessage{{
		ID: "b", Role: protocol.RoleUser, Addr: protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "shared"}, Content: []protocol.ContentPart{partB},
	}}); err != nil {
		t.Fatal(err)
	}
	m, err := newModel(ctx, Config{
		Channel: NewChannel(), Store: history, Index: index,
		DirectoryAccess:    &recordingDirectoryAccess{},
		WorkspaceResolver:  mappedDirectoryWorkspaceResolver{rootA: "project-a", rootB: "project-b"},
		InitialWorkspaceID: "project-a", InitialThreadID: "shared", WorkspaceRoot: rootA,
	})
	if err != nil {
		t.Fatal(err)
	}

	next, cmd := m.handleSlash("/cd " + rootB)
	if cmd == nil {
		t.Fatal("workspace /cd did not start a load")
	}
	switching := next.(model)
	if !switching.workspaceSwitching || switching.workspaceID != "project-a" || switching.cwd != rootA {
		t.Fatalf("workspace changed before load completed: workspace=%q cwd=%q switching=%v", switching.workspaceID, switching.cwd, switching.workspaceSwitching)
	}
	next, _ = switching.Update(cmd())
	updated := next.(model)
	if updated.workspaceID != "project-b" || updated.cwd != rootB || updated.threadID != "shared" {
		t.Fatalf("workspace selection = workspace %q cwd %q thread %q", updated.workspaceID, updated.cwd, updated.threadID)
	}
	if len(updated.threads) != 1 || updated.threads[0].Title != "thread B" {
		t.Fatalf("project B threads = %+v", updated.threads)
	}
	if len(updated.rows) < 1 || updated.rows[0].Text != "history B" {
		t.Fatalf("project B rows = %+v", updated.rows)
	}
}

func TestWorkingDirectoryWorkspaceSwitchRejectsUnscopedThreadIndex(t *testing.T) {
	ctx := context.Background()
	rootA := t.TempDir()
	rootB := t.TempDir()
	stateDir := t.TempDir()
	index, err := threadindex.NewIndex(stateDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Touch("thread-a", "thread A"); err != nil {
		t.Fatal(err)
	}
	m, err := newModel(ctx, Config{
		Channel: NewChannel(), Store: store.NewFileStore("", stateDir), Index: index,
		DirectoryAccess:    &recordingDirectoryAccess{},
		WorkspaceResolver:  mappedDirectoryWorkspaceResolver{rootB: "project-b"},
		InitialWorkspaceID: "project-a", InitialThreadID: "thread-a", WorkspaceRoot: rootA,
	})
	if err != nil {
		t.Fatal(err)
	}
	next, cmd := m.handleSlash("/cd " + rootB)
	if cmd == nil {
		t.Fatal("workspace /cd did not start a load")
	}
	next, _ = next.(model).Update(cmd())
	updated := next.(model)
	if updated.workspaceID != "project-a" || updated.cwd != rootA {
		t.Fatalf("failed switch changed selection: workspace=%q cwd=%q", updated.workspaceID, updated.cwd)
	}
	if len(updated.rows) == 0 || !strings.Contains(updated.rows[len(updated.rows)-1].Text, "thread index does not support workspace") {
		t.Fatalf("switch failure was not surfaced: %+v", updated.rows)
	}
}

func TestWorkingDirectoryWorkspaceSwitchWaitsForActiveTurnBeforeGrant(t *testing.T) {
	ctx := context.Background()
	rootA := t.TempDir()
	rootB := t.TempDir()
	stateDir := t.TempDir()
	index, err := threadindex.NewWorkspaceIndex(stateDir, "project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	access := &recordingDirectoryAccess{}
	m, err := newModel(ctx, Config{
		Channel: NewChannel(), Store: store.NewFileStore("", stateDir), Index: index,
		DirectoryAccess: access, WorkspaceResolver: mappedDirectoryWorkspaceResolver{rootB: "project-b"},
		InitialWorkspaceID: "project-a", WorkspaceRoot: rootA,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.markTurn("task-a", m.threadID, "thinking")
	next, cmd := m.handleSlash("/cd " + rootB)
	if cmd == nil {
		t.Fatal("workspace /cd did not start resolution")
	}
	next, _ = next.(model).Update(cmd())
	updated := next.(model)
	if updated.workspaceID != "project-a" || updated.cwd != rootA {
		t.Fatalf("active-turn switch changed selection: workspace=%q cwd=%q", updated.workspaceID, updated.cwd)
	}
	if len(access.dirs) != 0 {
		t.Fatalf("external project was granted during an active turn: %+v", access.dirs)
	}
	if got := updated.rows[len(updated.rows)-1].Text; !strings.Contains(got, "active workspace turns") {
		t.Fatalf("active-turn rejection = %q", got)
	}
}

func TestRemoteMetaCommandCarriesSelectedWorkspaceBinding(t *testing.T) {
	root := t.TempDir()
	index, err := threadindex.NewWorkspaceIndex(t.TempDir(), "project-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.TouchWorkspaceThread("project-a", "thread-1", ""); err != nil {
		t.Fatal(err)
	}
	binding := spec.NewExecutionBinding("project-a", "view-a", "us::drew::w1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-a"
	channel := NewChannel()
	m := model{
		ctx: context.Background(), channel: channel, index: index,
		workspaceRoot: root, cwd: root, initialWorkspaceID: "project-a", workspaceID: "project-a", threadID: "thread-1",
		executionBindings: fixedExecutionBindingProvider{binding: binding},
		viewport:          viewport.New(), composer: newComposer(),
		turns: map[string]turnActivity{}, pendingApprovals: map[string]approvalRequest{},
		tools: map[string]toolEntry{}, tailing: true,
	}
	_, cmd := m.handleSlash("/model")
	if cmd == nil {
		t.Fatal("/model was not enqueued")
	}
	if result := cmd(); result.(sendResultMsg).Err != nil {
		t.Fatal(result)
	}
	inbound, err := channel.FetchTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := spec.GetExecutionBinding(inbound.Message)
	if err != nil || got == nil || got.WorkspaceID != "project-a" {
		t.Fatalf("meta command binding = %+v, %v", got, err)
	}
	if inbound.Addr.WorkspaceID != "project-a" {
		t.Fatalf("meta command workspace = %q", inbound.Addr.WorkspaceID)
	}
}
