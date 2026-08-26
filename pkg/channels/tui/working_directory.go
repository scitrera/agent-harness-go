package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

func canonicalWorkspaceRoot(workspaceRoot string) (string, error) {
	workspaceRoot = strings.TrimSpace(workspaceRoot)
	if workspaceRoot == "" {
		return "", nil
	}
	root, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return "", err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(root)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workspace root is not a directory")
	}
	return root, nil
}

func resolveWorkspacePath(workspaceRoot, cwd, requestedPath string) (root string, target string, err error) {
	root, err = canonicalWorkspaceRoot(workspaceRoot)
	if err != nil {
		return "", "", err
	}
	if root == "" {
		return "", "", fmt.Errorf("workspace root is not configured")
	}
	if strings.TrimSpace(cwd) == "" {
		cwd = root
	}
	if !filepath.IsAbs(cwd) {
		cwd = filepath.Join(root, cwd)
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return "", "", err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", "", err
	}
	if err := ensurePathWithinWorkspace(root, cwd); err != nil {
		return "", "", err
	}

	requestedPath = strings.TrimSpace(requestedPath)
	if requestedPath == "" {
		requestedPath = "."
	}
	target = requestedPath
	if !filepath.IsAbs(target) {
		target = filepath.Join(cwd, target)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", "", err
	}
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		return "", "", err
	}
	if err := ensurePathWithinWorkspace(root, target); err != nil {
		return "", "", err
	}
	return root, target, nil
}

func resolveWorkingPath(cwd, requestedPath string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", fmt.Errorf("working directory is not configured")
	}
	base, err := filepath.Abs(cwd)
	if err != nil {
		return "", err
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		return "", err
	}
	requestedPath = strings.TrimSpace(requestedPath)
	if requestedPath == "" {
		requestedPath = "."
	}
	target := requestedPath
	if !filepath.IsAbs(target) {
		target = filepath.Join(base, target)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(target)
}

func ensurePathWithinWorkspace(root, target string) error {
	relative, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path is outside the workspace")
	}
	return nil
}

func workspaceRelativePath(root, target string) (string, error) {
	relative, err := filepath.Rel(root, target)
	if err != nil {
		return "", err
	}
	if err := ensurePathWithinWorkspace(root, target); err != nil {
		return "", err
	}
	return filepath.ToSlash(relative), nil
}

func pathWithinWorkspace(root, target string) bool {
	return root != "" && ensurePathWithinWorkspace(root, target) == nil
}

func (m model) currentWorkingDirectory() string {
	if m.cwd != "" {
		return m.cwd
	}
	return m.workspaceRoot
}

func (m model) grantExternalDirectory(target string) error {
	if pathWithinWorkspace(m.workspaceRoot, target) {
		return nil
	}
	if m.directoryAccess == nil {
		return fmt.Errorf("external working-directory access is not configured")
	}
	if err := m.directoryAccess.GrantWorkingDirectory(target); err != nil {
		return fmt.Errorf("grant external working directory: %w", err)
	}
	return nil
}

func (m model) handleWorkingDirectory(fields []string) (model, tea.Cmd, error) {
	if len(fields) == 0 {
		return m, nil, nil
	}
	switch fields[0] {
	case "/pwd":
		if m.cwd == "" {
			return m, nil, fmt.Errorf("workspace root is not configured")
		}
		m.addSystem(m.cwd)
		return m, nil, nil
	case "/cd":
		var requested string
		if len(fields) > 1 {
			requested = trimMatchingQuotes(strings.Join(fields[1:], " "))
		} else {
			requested = m.workspaceRoot
		}
		target, err := resolveWorkingPath(m.cwd, requested)
		if err != nil {
			return m, nil, err
		}
		info, err := os.Stat(target)
		if err != nil {
			return m, nil, err
		}
		if !info.IsDir() {
			return m, nil, fmt.Errorf("%s is not a directory", requested)
		}
		if m.workspaceResolver != nil {
			m.workspaceSwitching = true
			m.status = "resolving workspace"
			return m, switchWorkspaceCmd(
				m.ctx, m.workspaceResolver, m.index, m.store, m.initialWorkspaceID,
				m.workspaceID, m.threadID, target, m.directoryAccess,
				!pathWithinWorkspace(m.workspaceRoot, target), m.hasActiveWorkspaceTurns(),
			), nil
		}
		if err := m.grantExternalDirectory(target); err != nil {
			return m, nil, err
		}
		m.cwd = target
		m.addSystem("cwd " + m.cwd)
		return m, nil, nil
	default:
		return m, nil, nil
	}
}

func switchWorkspaceCmd(
	ctx context.Context,
	resolver DirectoryWorkspaceResolver,
	index threadindex.Store,
	store HistoryStore,
	initialWorkspaceID, currentWorkspaceID, currentThreadID, target string,
	directoryAccess DirectoryAccess,
	grantWorkspace, activeWorkspaceTurns bool,
) tea.Cmd {
	return func() tea.Msg {
		workspaceID, err := resolver.ResolveWorkspaceForDirectory(ctx, target)
		if err != nil {
			return workspaceLoadedMsg{CWD: target, Err: fmt.Errorf("resolve logical workspace: %w", err)}
		}
		workspaceID = strings.TrimSpace(workspaceID)
		if workspaceID == "" {
			return workspaceLoadedMsg{CWD: target, Err: fmt.Errorf("workspace resolver returned an empty workspace")}
		}
		if grantWorkspace {
			if activeWorkspaceTurns {
				return workspaceLoadedMsg{WorkspaceID: workspaceID, CWD: target, Err: fmt.Errorf("wait for active workspace turns to finish or cancel them before switching projects")}
			}
			workspaceAccess, ok := directoryAccess.(WorkspaceDirectoryAccess)
			if !ok {
				return workspaceLoadedMsg{WorkspaceID: workspaceID, CWD: target, Err: fmt.Errorf("dynamic workspace access is not configured")}
			}
			if err := workspaceAccess.GrantWorkspaceDirectory(target); err != nil {
				return workspaceLoadedMsg{WorkspaceID: workspaceID, CWD: target, Err: fmt.Errorf("grant workspace directory: %w", err)}
			}
		}
		if err := refreshWorkspaceThreads(ctx, index, initialWorkspaceID, workspaceID); err != nil {
			return workspaceLoadedMsg{WorkspaceID: workspaceID, CWD: target, Err: err}
		}
		threads := listWorkspaceThreads(index, initialWorkspaceID, workspaceID)
		var session threadindex.Session
		if workspaceID == currentWorkspaceID {
			for _, candidate := range threads {
				if candidate.ID == currentThreadID {
					session = candidate
					break
				}
			}
		}
		if session.ID == "" && len(threads) > 0 {
			session = threads[0]
		}
		if session.ID == "" {
			session, err = createWorkspaceThread(index, initialWorkspaceID, workspaceID)
			if err != nil {
				return workspaceLoadedMsg{WorkspaceID: workspaceID, CWD: target, Err: fmt.Errorf("create workspace thread: %w", err)}
			}
			threads = listWorkspaceThreads(index, initialWorkspaceID, workspaceID)
		}
		messages, err := loadWorkspaceHistory(ctx, store, initialWorkspaceID, workspaceID, session.ID)
		if err != nil {
			return workspaceLoadedMsg{WorkspaceID: workspaceID, CWD: target, Err: fmt.Errorf("load workspace history: %w", err)}
		}
		return workspaceLoadedMsg{
			WorkspaceID: workspaceID, CWD: target, Session: session, Threads: threads, Messages: messages,
		}
	}
}

func (m model) hasActiveWorkspaceTurns() bool {
	for _, activity := range m.turns {
		if m.workspaceID == "" || activity.WorkspaceID == "" || activity.WorkspaceID == m.workspaceID {
			return true
		}
	}
	return false
}

func (m *model) applyWorkspaceLoaded(msg workspaceLoadedMsg) {
	m.workspaceSwitching = false
	if msg.Err != nil {
		m.addSystem("workspace switch failed: " + msg.Err.Error())
		return
	}
	m.workspaceID = msg.WorkspaceID
	m.cwd = msg.CWD
	m.threadID = msg.Session.ID
	m.observeModelHistory(msg.WorkspaceID, msg.Session.ID, msg.Messages)
	m.threads = msg.Threads
	m.rows = rowsFromHistory(msg.Messages)
	m.inputHistory = inputHistoryFromMessages(msg.Messages)
	m.resetHistoryNavigation()
	m.renderedRows = map[string]renderedRowCache{}
	m.pendingApprovals = map[string]approvalRequest{}
	m.tools = map[string]toolEntry{}
	m.subagents = map[string]subagentActivity{}
	m.lastTaskID = ""
	m.selector.clear()
	m.drawer = drawerNone
	m.drawerContent = ""
	m.status = "workspace " + msg.WorkspaceID
	m.tailing = true
	m.addSystem("workspace " + msg.WorkspaceID + "\ncwd " + msg.CWD)
	m.refreshViewportToBottom()
}
