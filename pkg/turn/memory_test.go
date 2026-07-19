package turn

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type fakeMemory struct {
	hits            []tools.MemoryHit
	appended        [][]protocol.ChatMessage
	appendWorkspace string
	appendThread    string
	appendOwnership string
	appendGrant     string
	recallWorkspace string
	recallQuery     string
	recallGrant     string
	ensured         []ThreadSpec
	mintCount       int
}

func (m *fakeMemory) Recall(_ context.Context, auth tools.MemoryAuthority, workspace, query string, _ int) ([]tools.MemoryHit, error) {
	m.recallWorkspace = workspace
	m.recallQuery = query
	m.recallGrant = auth.GrantID
	return m.hits, nil
}

func (m *fakeMemory) AppendThreadMessages(_ context.Context, auth tools.MemoryAuthority, workspace, threadID, ownership string, msgs []protocol.ChatMessage) error {
	m.appendWorkspace = workspace
	m.appendThread = threadID
	m.appendOwnership = ownership
	m.appendGrant = auth.GrantID
	m.appended = append(m.appended, msgs)
	return nil
}

// EnsureThread makes fakeMemory satisfy turn.ThreadRegistrar (the optional
// thread-hierarchy seam), recording the declared thread specs. An empty
// spec.ThreadID simulates a backend minting a canonical id; a supplied id echoes
// back unchanged.
func (m *fakeMemory) EnsureThread(_ context.Context, _ tools.MemoryAuthority, spec ThreadSpec) (string, error) {
	m.ensured = append(m.ensured, spec)
	if spec.ThreadID != "" {
		return spec.ThreadID, nil
	}
	m.mintCount++
	return fmt.Sprintf("mem-thread-%d", m.mintCount), nil
}

func userMessage(t *testing.T, text string) protocol.ChatMessage {
	t.Helper()
	p, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	return protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{p}}
}

func Test_Runner_Run_auto_commits_user_and_assistant(t *testing.T) {
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hi")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(mem.appended) != 1 || len(mem.appended[0]) != 2 {
		t.Fatalf("expected one append of [user, assistant], got %#v", mem.appended)
	}
	if mem.appended[0][0].Role != protocol.RoleUser || mem.appended[0][1].Role != protocol.RoleAssistant {
		t.Fatalf("unexpected appended roles: %#v", mem.appended[0])
	}
}

func Test_Runner_Run_no_autocommit_when_disabled(t *testing.T) {
	mem := &fakeMemory{}
	r, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: false,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hi")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(mem.appended) != 0 {
		t.Fatalf("expected no append when auto-commit disabled, got %#v", mem.appended)
	}
}

func Test_Runner_Run_threads_turn_grant_to_memory(t *testing.T) {
	mem := &fakeMemory{}
	// The Authority hook derives the per-turn grant (the distribution wires the
	// real grant extractor); here we inject a fixed grant and assert it threads
	// through to recall + append.
	r, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Provider:         &fakeProvider{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:           mem,
		MemoryAutoCommit: true,
		MemoryAutoRecall: true,
		Authority: func(_ protocol.MessageAddress, _ protocol.ChatMessage) tools.MemoryAuthority {
			return tools.MemoryAuthority{GrantID: "grant-77", SubjectType: "user", SubjectID: "u9"}
		},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	user := userMessage(t, "hi")
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "ws1", UserID: "u9"}, user); err != nil {
		t.Fatalf("run: %v", err)
	}
	if mem.recallGrant != "grant-77" {
		t.Fatalf("recall grant = %q, want grant-77", mem.recallGrant)
	}
	if mem.appendGrant != "grant-77" {
		t.Fatalf("append grant = %q, want grant-77", mem.appendGrant)
	}
}

func Test_Runner_Run_auto_recall_injects_memories(t *testing.T) {
	mem := &fakeMemory{hits: []tools.MemoryHit{{ID: "m1", Content: "the user likes teal"}}}
	provider := &fakeProvider{}
	r, err := NewRunner(Config{
		Store:                    &fakeStore{},
		Loader:                   fakeLoader{},
		Provider:                 provider,
		Assembler:                contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Memory:                   mem,
		MemoryAutoRecall:         true,
		MemoryRecallIncludeInput: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "ws1"}, userMessage(t, "what color do I like")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if mem.recallWorkspace != "ws1" || mem.recallQuery != "what color do I like" {
		t.Fatalf("recall workspace=%q query=%q", mem.recallWorkspace, mem.recallQuery)
	}
	found := false
	for _, msg := range provider.request.Messages {
		if msg.ID != "recalled-memories" {
			continue
		}
		for _, part := range msg.Content {
			if tp, ok := part.AsText(); ok && strings.Contains(tp.Text, "the user likes teal") {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("recalled memories not injected into provider request: %#v", provider.request.Messages)
	}
}
