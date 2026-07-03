package turn

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

// recordingStore is a durable HistoryStore that records the thread ids it is
// asked to load and the messages saved per thread, so tests can assert the
// sub-agent uses the INJECTED store (not a throwaway memory store) and that
// resume consults the same child thread.
type recordingStore struct {
	mu     sync.Mutex
	loaded []string
	saved  map[string][]protocol.ChatMessage
}

func (s *recordingStore) LoadHistory(_ context.Context, threadID string) ([]protocol.ChatMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loaded = append(s.loaded, threadID)
	src := s.saved[threadID]
	out := make([]protocol.ChatMessage, len(src))
	copy(out, src)
	return out, nil
}

func (s *recordingStore) SaveHistory(_ context.Context, threadID string, msgs []protocol.ChatMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saved == nil {
		s.saved = map[string][]protocol.ChatMessage{}
	}
	cp := make([]protocol.ChatMessage, len(msgs))
	copy(cp, msgs)
	s.saved[threadID] = cp
	return nil
}

func (s *recordingStore) loadCount(threadID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.loaded {
		if l == threadID {
			n++
		}
	}
	return n
}

func Test_Runner_RunSubagent_persists_child_thread_with_backref(t *testing.T) {
	// Given: a durable (injected) store and a memory hook with auto-commit on.
	store := &recordingStore{}
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            store,
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	// When: a NEW sub-agent runs under a parent thread.
	res, err := r.RunSubagent(context.Background(), subagent.Request{
		Task:            "research the API",
		Depth:           1,
		Parent:          protocol.MessageAddress{ThreadID: "parent-thread", WorkspaceID: "ws1", AgentID: "falcon"},
		ParentMessageID: "msg-parent-7",
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}

	// Then: it ran on a "<parent>::sub::N" child thread and returned that handle.
	if !strings.HasPrefix(res.ThreadID, "parent-thread::sub::") {
		t.Fatalf("child thread id = %q, want parent-thread::sub:: prefix", res.ThreadID)
	}
	// The DURABLE store was consulted (LoadHistory) for the child thread — proves
	// it uses r.store, not a throwaway NewMemoryStore.
	if store.loadCount(res.ThreadID) == 0 {
		t.Fatalf("expected LoadHistory for child thread %q, loaded=%v", res.ThreadID, store.loaded)
	}
	// The child thread was committed via the memory hook (child thread id, not parent).
	if mem.appendThread != res.ThreadID {
		t.Fatalf("committed thread = %q, want child %q", mem.appendThread, res.ThreadID)
	}
	if mem.appendWorkspace != "ws1" {
		t.Fatalf("committed workspace = %q, want ws1", mem.appendWorkspace)
	}
	if len(mem.appended) != 1 || len(mem.appended[0]) != 2 {
		t.Fatalf("expected one [task, assistant] commit, got %#v", mem.appended)
	}

	// The child's task (user) message carries the cross-thread back-ref + spawn meta.
	task := mem.appended[0][0]
	if task.Role != protocol.RoleUser {
		t.Fatalf("first committed message should be the task (user), got %s", task.Role)
	}
	if task.Ref == nil || task.Ref.ParentThreadID != "parent-thread" || task.Ref.ParentMessageID != "msg-parent-7" {
		t.Fatalf("task back-ref = %#v, want {parent-thread, msg-parent-7}", task.Ref)
	}
	spawn, ok := task.Meta["scitrera"]
	if !ok {
		t.Fatalf("task meta missing scitrera spawn stamp: %#v", task.Meta)
	}
	if !bytes.Contains(spawn, []byte(`"kind":"delegation"`)) || !bytes.Contains(spawn, []byte(`"by":"falcon"`)) {
		t.Fatalf("spawn meta = %s, want kind=delegation by=falcon", spawn)
	}
}

func Test_Runner_RunSubagent_resume_reuses_child_thread(t *testing.T) {
	// Given: a durable store + memory hook.
	store := &recordingStore{}
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            store,
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	parent := protocol.MessageAddress{ThreadID: "p", WorkspaceID: "ws"}

	// When: spawn a new sub-agent, then resume it via ResumeThreadID.
	res1, err := r.RunSubagent(context.Background(), subagent.Request{Task: "first", Parent: parent})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	child := res1.ThreadID
	res2, err := r.RunSubagent(context.Background(), subagent.Request{Task: "follow up", Parent: parent, ResumeThreadID: child})
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}

	// Then: resume reuses the SAME child thread id verbatim (no new ::sub::seq).
	if res2.ThreadID != child {
		t.Fatalf("resume thread = %q, want same child %q", res2.ThreadID, child)
	}
	// The store's LoadHistory was consulted for the resumed child thread again
	// (>=2 loads of that id: once per run).
	if store.loadCount(child) < 2 {
		t.Fatalf("expected the resumed child thread loaded >=2 times, got %d (%v)", store.loadCount(child), store.loaded)
	}
	// Resume does NOT re-stamp the back-ref/spawn meta (the thread already exists).
	last := mem.appended[len(mem.appended)-1][0]
	if last.Ref != nil {
		t.Fatalf("resume task message should carry no back-ref, got %#v", last.Ref)
	}
	if _, ok := last.Meta["scitrera"]; ok {
		t.Fatalf("resume task message should carry no spawn meta, got %#v", last.Meta)
	}
}

func Test_Runner_RunSubagent_siblings_get_distinct_threads(t *testing.T) {
	r, err := NewRunner(Config{
		Store:     &recordingStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	parent := protocol.MessageAddress{ThreadID: "p"}
	res1, err := r.RunSubagent(context.Background(), subagent.Request{Task: "x", Parent: parent})
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	res2, err := r.RunSubagent(context.Background(), subagent.Request{Task: "y", Parent: parent})
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if res1.ThreadID == res2.ThreadID {
		t.Fatalf("sibling sub-agents must get distinct threads, both = %q", res1.ThreadID)
	}
	if res1.ThreadID == "p" || res2.ThreadID == "p" {
		t.Fatalf("child thread must differ from parent %q", "p")
	}
}
