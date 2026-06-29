package tui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/bubbles/v2/viewport"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/taskstate"
)

type fakeApprovalResolver struct {
	taskID    string
	requestID string
	decision  approval.Decision
	ok        bool
}

func (f *fakeApprovalResolver) Resolve(taskID, requestID string, decision approval.Decision) bool {
	f.taskID = taskID
	f.requestID = requestID
	f.decision = decision
	return f.ok
}

func TestModelResolveApproval_whenRequestPending(t *testing.T) {
	// Given
	resolver := &fakeApprovalResolver{ok: true}
	m := model{
		approvals:        resolver,
		pendingApprovals: map[string]approvalRequest{"call-1": {RequestID: "call-1", TaskID: "task-1", Tool: "shell", Status: "pending"}},
		viewport:         viewport.New(),
	}

	// When
	m.resolveApproval([]string{"/approve", "call-1", "session"}, true)

	// Then
	if resolver.taskID != "task-1" || resolver.requestID != "call-1" || !resolver.decision.Granted || resolver.decision.Scope != "session" {
		t.Fatalf("decision mismatch: %+v", resolver)
	}
	if len(m.pendingApprovals) != 0 {
		t.Fatalf("approval not removed: %+v", m.pendingApprovals)
	}
}

func TestModelHandlePermissions_approveAliasResolvesRequest(t *testing.T) {
	// Given
	resolver := &fakeApprovalResolver{ok: true}
	m := model{
		approvals:        resolver,
		pendingApprovals: map[string]approvalRequest{"call-1": {RequestID: "call-1", TaskID: "task-1", Tool: "shell", Status: "pending"}},
		viewport:         viewport.New(),
	}

	// When
	_, cmd := m.handlePermissions([]string{"/permissions", "approve", "call-1", "always"})

	// Then
	if cmd != nil {
		t.Fatal("permission approval should resolve synchronously")
	}
	if resolver.taskID != "task-1" || resolver.requestID != "call-1" || !resolver.decision.Granted || resolver.decision.Scope != "always" {
		t.Fatalf("decision mismatch: %+v", resolver)
	}
}

func TestModelHandleTasks_planCreateLoadsDrawer(t *testing.T) {
	// Given
	store := taskstate.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	m := model{ctx: context.Background(), taskStore: store, viewport: viewport.New()}

	// When
	_, cmd := m.handleTasks([]string{"/tasks", "plan", "create", "plan-1", "Build TUI"})
	if cmd == nil {
		t.Fatal("expected task command")
	}
	msg, ok := cmd().(drawerLoadedMsg)
	if !ok {
		t.Fatalf("unexpected message type %T", cmd())
	}

	// Then
	if msg.Err != nil {
		t.Fatalf("task command failed: %v", msg.Err)
	}
	if msg.Drawer != drawerTasks || !strings.Contains(msg.Text, "plan plan-1: Build TUI") {
		t.Fatalf("drawer message mismatch: %+v", msg)
	}
}
