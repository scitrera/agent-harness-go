// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package harness

import (
	"context"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// MemoryStore is an in-process HistoryStore. It backs ephemeral sub-agent
// sessions (which must not persist to or pollute the durable thread history) and
// is handy for tests.
type MemoryStore struct {
	mu      sync.Mutex
	threads map[memoryHistoryKey][]protocol.ChatMessage
}

type memoryHistoryKey struct {
	workspaceID string
	threadID    string
}

// NewMemoryStore returns an empty in-memory history store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{threads: map[memoryHistoryKey][]protocol.ChatMessage{}}
}

// LoadHistory returns a copy of the thread's messages.
func (s *MemoryStore) LoadHistory(_ context.Context, threadID string) ([]protocol.ChatMessage, error) {
	return s.load(memoryHistoryKey{threadID: threadID}), nil
}

// LoadWorkspaceHistory returns a copy of the messages stored under the
// composite (workspaceID, threadID).
func (s *MemoryStore) LoadWorkspaceHistory(_ context.Context, workspaceID, threadID string) ([]protocol.ChatMessage, error) {
	return s.load(memoryHistoryKey{workspaceID: workspaceID, threadID: threadID}), nil
}

func (s *MemoryStore) load(key memoryHistoryKey) []protocol.ChatMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.threads[key]
	out := make([]protocol.ChatMessage, len(src))
	copy(out, src)
	return out
}

// SaveHistory replaces the thread's messages with a copy of the given slice.
func (s *MemoryStore) SaveHistory(_ context.Context, threadID string, messages []protocol.ChatMessage) error {
	s.save(memoryHistoryKey{threadID: threadID}, messages)
	return nil
}

// SaveWorkspaceHistory replaces the messages stored under the composite
// (workspaceID, threadID).
func (s *MemoryStore) SaveWorkspaceHistory(_ context.Context, workspaceID, threadID string, messages []protocol.ChatMessage) error {
	s.save(memoryHistoryKey{workspaceID: workspaceID, threadID: threadID}, messages)
	return nil
}

func (s *MemoryStore) save(key memoryHistoryKey, messages []protocol.ChatMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]protocol.ChatMessage, len(messages))
	copy(stored, messages)
	s.threads[key] = stored
}
