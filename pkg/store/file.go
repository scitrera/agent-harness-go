// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package store provides filesystem reference implementations of the core
// memory seams: a durable per-thread history store and a workspace bootstrap
// loader. These are the OSS defaults; distributions may supply remote stores.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// bootstrapFileOrder is the order workspace bootstrap documents are surfaced in.
var bootstrapFileOrder = []string{"SOUL.md", "IDENTITY.md", "AGENTS.md", "USER.md", "TOOLS.md"}

const defaultMaxBootstrapBytes int64 = 64 << 10

// FileStore is a filesystem-backed HistoryStore + bootstrap.Loader. Thread
// history is one JSON file per thread under stateDir/history; bootstrap files
// are read from the workspace root.
type FileStore struct {
	workspaceRoot string
	stateDir      string
	maxBootstrap  int64

	mu sync.Mutex
}

// ThreadHistory is one workspace-scoped transcript returned for enumeration.
type ThreadHistory struct {
	ThreadID string
	Messages []protocol.ChatMessage
}

// NewFileStore returns a store rooted at workspaceRoot (bootstrap files) with
// history persisted under stateDir.
func NewFileStore(workspaceRoot, stateDir string) *FileStore {
	return &FileStore{workspaceRoot: workspaceRoot, stateDir: stateDir, maxBootstrap: defaultMaxBootstrapBytes}
}

func (s *FileStore) historyPath(threadID string) string {
	return filepath.Join(s.stateDir, "history", sanitize(threadID)+".json")
}

func (s *FileStore) workspaceHistoryPath(workspaceID, threadID string) string {
	return filepath.Join(s.stateDir, "history", "workspaces", workspacepkg.PathSegment(workspaceID), workspacepkg.PathSegment(threadID)+".json")
}

// LoadHistory returns the thread's persisted messages (empty if none).
func (s *FileStore) LoadHistory(_ context.Context, threadID string) ([]protocol.ChatMessage, error) {
	return s.loadHistory(s.historyPath(threadID))
}

// LoadWorkspaceHistory returns the transcript stored under the composite
// (workspaceID, threadID), isolated from both legacy and other workspace data.
func (s *FileStore) LoadWorkspaceHistory(_ context.Context, workspaceID, threadID string) ([]protocol.ChatMessage, error) {
	return s.loadHistory(s.workspaceHistoryPath(workspaceID, threadID))
}

// ListWorkspaceHistory returns every non-empty transcript in workspaceID,
// sorted by thread ID. It supports export and administrative surfaces without
// making callers understand the filesystem-safe ID encoding.
func (s *FileStore) ListWorkspaceHistory(_ context.Context, workspaceID string) ([]ThreadHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.workspaceHistoryPath(workspaceID, "thread"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read workspace history: %w", err)
	}
	out := make([]ThreadHistory, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		messages, err := readHistoryFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		if len(messages) == 0 {
			continue
		}
		threadID := ""
		for _, message := range messages {
			if message.Addr.ThreadID != "" {
				threadID = message.Addr.ThreadID
				break
			}
		}
		if threadID == "" {
			return nil, fmt.Errorf("decode history %s: messages carry no thread ID", filepath.Join(dir, entry.Name()))
		}
		out = append(out, ThreadHistory{ThreadID: threadID, Messages: messages})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ThreadID < out[j].ThreadID })
	return out, nil
}

func (s *FileStore) loadHistory(path string) ([]protocol.ChatMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readHistoryFile(path)
}

func readHistoryFile(path string) ([]protocol.ChatMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read history: %w", err)
	}
	var msgs []protocol.ChatMessage
	if err := json.Unmarshal(data, &msgs); err != nil {
		return nil, fmt.Errorf("decode history: %w", err)
	}
	return msgs, nil
}

// SaveHistory atomically writes the thread's messages.
func (s *FileStore) SaveHistory(_ context.Context, threadID string, messages []protocol.ChatMessage) error {
	return s.saveHistory(s.historyPath(threadID), messages)
}

// SaveWorkspaceHistory atomically writes a transcript under the composite
// (workspaceID, threadID).
func (s *FileStore) SaveWorkspaceHistory(_ context.Context, workspaceID, threadID string, messages []protocol.ChatMessage) error {
	return s.saveHistory(s.workspaceHistoryPath(workspaceID, threadID), messages)
}

func (s *FileStore) saveHistory(path string, messages []protocol.ChatMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(messages)
	if err != nil {
		return fmt.Errorf("encode history: %w", err)
	}
	if err := atomicfile.Write(path, data, 0o644); err != nil {
		return fmt.Errorf("save history: %w", err)
	}
	return nil
}

// DeleteHistory deletes the persisted transcript for threadID.
func (s *FileStore) DeleteHistory(_ context.Context, threadID string) error {
	return s.deleteHistory(s.historyPath(threadID))
}

// DeleteWorkspaceHistory deletes the transcript stored under the composite
// (workspaceID, threadID).
func (s *FileStore) DeleteWorkspaceHistory(_ context.Context, workspaceID, threadID string) error {
	return s.deleteHistory(s.workspaceHistoryPath(workspaceID, threadID))
}

func (s *FileStore) deleteHistory(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return fmt.Errorf("delete history: %w", err)
}

// LoadBootstrap reads the workspace bootstrap documents (in bootstrapFileOrder)
// that exist, each capped at maxBootstrap bytes.
func (s *FileStore) LoadBootstrap(_ context.Context) ([]bootstrap.File, error) {
	if s.workspaceRoot == "" {
		return nil, nil
	}
	var out []bootstrap.File
	for _, name := range bootstrapFileOrder {
		data, err := os.ReadFile(filepath.Join(s.workspaceRoot, name))
		if err != nil {
			continue // missing bootstrap file -> skip
		}
		if int64(len(data)) > s.maxBootstrap {
			data = data[:s.maxBootstrap]
		}
		out = append(out, bootstrap.File{Name: name, Content: string(data)})
	}
	return out, nil
}

// sanitize maps a thread id to a safe single-path-segment filename.
func sanitize(id string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "..", "_")
	clean := r.Replace(id)
	if clean == "" {
		return "default"
	}
	return clean
}
