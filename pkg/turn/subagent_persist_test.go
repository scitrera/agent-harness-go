package turn

import (
	"bytes"
	"context"
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

	// Then: it adopted the registrar's canonical child thread id and returned
	// that handle everywhere.
	if res.ThreadID != "mem-thread-1" {
		t.Fatalf("child thread id = %q, want registrar-minted mem-thread-1", res.ThreadID)
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

func Test_Runner_RunSubagent_commits_backref_even_under_assistant_only(t *testing.T) {
	// Given: auto-commit in ASSISTANT-ONLY mode — the production sahara setting
	// (MemoryAutoCommitAssistantOnly: true, because a host pre-commits the PARENT
	// thread's user turn). A child sub-agent thread is minted internally, so nothing
	// pre-commits its task message — and that task message is the ONLY carrier of the
	// child→parent back-ref. Assistant-only must NOT drop it, or the linkage is lost
	// to everything but the "::sub::" thread-name convention (Gap B).
	store := &recordingStore{}
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:                         store,
		Loader:                        fakeLoader{},
		Provider:                      &fakeProvider{},
		Assembler:                     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:                        mem,
		MemoryAutoCommit:              true,
		MemoryAutoCommitAssistantOnly: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	res, err := r.RunSubagent(context.Background(), subagent.Request{
		Task:            "research the API",
		Depth:           1,
		Parent:          protocol.MessageAddress{ThreadID: "parent-thread", WorkspaceID: "ws1", AgentID: "falcon"},
		ParentMessageID: "msg-parent-7",
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}

	// Even under assistant-only, the child commit carries BOTH the task and the
	// assistant (assistant-only applies to the parent thread, not internally-minted
	// child threads).
	if len(mem.appended) != 1 || len(mem.appended[0]) != 2 {
		t.Fatalf("expected one [task, assistant] child commit under assistant-only, got %#v", mem.appended)
	}
	task := mem.appended[0][0]
	if task.Role != protocol.RoleUser {
		t.Fatalf("first committed child message should be the task (user), got %s", task.Role)
	}
	if task.Ref == nil || task.Ref.ParentThreadID != "parent-thread" || task.Ref.ParentMessageID != "msg-parent-7" {
		t.Fatalf("child→parent back-ref lost under assistant-only: task.Ref = %#v", task.Ref)
	}
	if mem.appendThread != res.ThreadID {
		t.Fatalf("committed thread = %q, want child %q", mem.appendThread, res.ThreadID)
	}
}

func Test_Runner_RunSubagent_declares_child_thread_parent_via_registrar(t *testing.T) {
	// Given: a memory backend that also implements ThreadRegistrar (the optional
	// thread-hierarchy seam). fakeMemory records EnsureThread calls.
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            &recordingStore{},
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
		Task:            "research",
		Parent:          protocol.MessageAddress{ThreadID: "parent-thread", WorkspaceID: "ws1"},
		ParentMessageID: "msg-7",
	})
	if err != nil {
		t.Fatalf("run subagent: %v", err)
	}

	// Then: the runner asked the registrar to MINT the child thread with its
	// parent before the session and commit, and adopted the returned id.
	if len(mem.ensured) != 1 {
		t.Fatalf("expected one EnsureThread call, got %#v", mem.ensured)
	}
	got := mem.ensured[0]
	if got.ThreadID != "" || res.ThreadID != "mem-thread-1" || got.ParentThreadID != "parent-thread" ||
		got.WorkspaceID != "ws1" || got.Origin != "subagent" {
		t.Fatalf("mint spec = %#v result=%q, want {thread=empty, parent=parent-thread, ws=ws1, origin=subagent} → mem-thread-1", got, res.ThreadID)
	}

	// And: resuming that child thread does NOT re-declare it (it already exists).
	if _, err := r.RunSubagent(context.Background(), subagent.Request{
		Task:           "follow up",
		Parent:         protocol.MessageAddress{ThreadID: "parent-thread", WorkspaceID: "ws1"},
		ResumeThreadID: res.ThreadID,
	}); err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if len(mem.ensured) != 1 {
		t.Fatalf("resume must not re-declare the thread, got %#v", mem.ensured)
	}
}

func Test_Runner_RunSubagent_mints_native_thread_when_history_backend_owns_writes(t *testing.T) {
	// OSS MemoryLayer is both the HistoryStore and registrar, so auto-commit is
	// intentionally off to avoid writing each message twice. Native thread
	// creation must remain independent and provide the canonical child id.
	store := &recordingStore{}
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            store,
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.RunSubagent(context.Background(), subagent.Request{
		Task: "research", Parent: protocol.MessageAddress{ThreadID: "parent", WorkspaceID: "ws"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ThreadID != "mem-thread-1" || len(mem.ensured) != 1 || store.loadCount("mem-thread-1") == 0 {
		t.Fatalf("result=%#v ensured=%#v loaded=%#v", res, mem.ensured, store.loaded)
	}
	if len(mem.appended) != 0 {
		t.Fatalf("auto-commit must remain off when history backend owns writes: %#v", mem.appended)
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
