package tui

import (
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// scopeMessage stamps the current logical workspace on every runner-bound
// message. Remote workspace tools additionally require an exact local view;
// the view and the selected workspace must agree before the turn is enqueued.
func (m model) scopeMessage(addr *protocol.MessageAddress, message *protocol.ChatMessage) error {
	addr.WorkspaceID = m.workspaceID
	message.Addr.WorkspaceID = m.workspaceID
	if m.executionBindings != nil {
		binding, err := m.executionBindings.ExecutionBindingForDirectory(m.ctx, m.currentWorkingDirectory())
		if err != nil {
			return fmt.Errorf("bind working directory: %w", err)
		}
		if m.workspaceID != "" && binding.WorkspaceID != m.workspaceID {
			return fmt.Errorf("working directory resolved to workspace %q, selected workspace is %q", binding.WorkspaceID, m.workspaceID)
		}
		addr.WorkspaceID = binding.WorkspaceID
		message.Addr.WorkspaceID = binding.WorkspaceID
		scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
			WriteAccess: workspacepkg.ViewWriteAccessReadWrite,
		})
		if err != nil {
			return fmt.Errorf("build working directory scope: %w", err)
		}
		if err := workspacepkg.PutExecutionScope(message, scope); err != nil {
			return fmt.Errorf("encode working directory binding: %w", err)
		}
		return nil
	}
	if cwd := m.currentWorkingDirectory(); cwd != "" && cwd != m.workspaceRoot {
		tools.StampWorkingDirectory(message, cwd)
	}
	return nil
}
