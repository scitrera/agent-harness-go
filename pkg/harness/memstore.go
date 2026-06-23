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
	threads map[string][]protocol.ChatMessage
}

// NewMemoryStore returns an empty in-memory history store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{threads: map[string][]protocol.ChatMessage{}}
}

// LoadHistory returns a copy of the thread's messages.
func (s *MemoryStore) LoadHistory(_ context.Context, threadID string) ([]protocol.ChatMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.threads[threadID]
	out := make([]protocol.ChatMessage, len(src))
	copy(out, src)
	return out, nil
}

// SaveHistory replaces the thread's messages with a copy of the given slice.
func (s *MemoryStore) SaveHistory(_ context.Context, threadID string, messages []protocol.ChatMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]protocol.ChatMessage, len(messages))
	copy(stored, messages)
	s.threads[threadID] = stored
	return nil
}
