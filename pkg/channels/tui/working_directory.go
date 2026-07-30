package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func (m model) handleWorkingDirectory(fields []string) (model, error) {
	if len(fields) == 0 {
		return m, nil
	}
	switch fields[0] {
	case "/pwd":
		if m.cwd == "" {
			return m, fmt.Errorf("workspace root is not configured")
		}
		m.addSystem(m.cwd)
		return m, nil
	case "/cd":
		requested := "."
		if len(fields) > 1 {
			requested = trimMatchingQuotes(strings.Join(fields[1:], " "))
		} else {
			requested = m.workspaceRoot
		}
		_, target, err := resolveWorkspacePath(m.workspaceRoot, m.cwd, requested)
		if err != nil {
			return m, err
		}
		info, err := os.Stat(target)
		if err != nil {
			return m, err
		}
		if !info.IsDir() {
			return m, fmt.Errorf("%s is not a directory", requested)
		}
		m.cwd = target
		m.addSystem("cwd " + m.cwd)
		return m, nil
	default:
		return m, nil
	}
}
