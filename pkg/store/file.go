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
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
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

// NewFileStore returns a store rooted at workspaceRoot (bootstrap files) with
// history persisted under stateDir.
func NewFileStore(workspaceRoot, stateDir string) *FileStore {
	return &FileStore{workspaceRoot: workspaceRoot, stateDir: stateDir, maxBootstrap: defaultMaxBootstrapBytes}
}

func (s *FileStore) historyPath(threadID string) string {
	return filepath.Join(s.stateDir, "history", sanitize(threadID)+".json")
}

// LoadHistory returns the thread's persisted messages (empty if none).
func (s *FileStore) LoadHistory(_ context.Context, threadID string) ([]protocol.ChatMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.historyPath(threadID))
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
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(messages)
	if err != nil {
		return fmt.Errorf("encode history: %w", err)
	}
	if err := atomicfile.Write(s.historyPath(threadID), data, 0o644); err != nil {
		return fmt.Errorf("save history: %w", err)
	}
	return nil
}

// DeleteHistory deletes the persisted transcript for threadID.
func (s *FileStore) DeleteHistory(_ context.Context, threadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.historyPath(threadID))
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
