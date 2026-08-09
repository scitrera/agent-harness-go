package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/promptnotes"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type fakeStore struct {
	mu       sync.Mutex
	messages []protocol.ChatMessage
}

type workspaceRecordingStore struct {
	workspaceID string
	threadID    string
	messages    []protocol.ChatMessage
}

func (s *workspaceRecordingStore) LoadHistory(context.Context, string) ([]protocol.ChatMessage, error) {
	return nil, errors.New("legacy load should not be used")
}

func (s *workspaceRecordingStore) SaveHistory(context.Context, string, []protocol.ChatMessage) error {
	return errors.New("legacy save should not be used")
}

func (s *workspaceRecordingStore) LoadWorkspaceHistory(_ context.Context, workspaceID, threadID string) ([]protocol.ChatMessage, error) {
	s.workspaceID = workspaceID
	s.threadID = threadID
	return append([]protocol.ChatMessage(nil), s.messages...), nil
}

func (s *workspaceRecordingStore) SaveWorkspaceHistory(_ context.Context, workspaceID, threadID string, messages []protocol.ChatMessage) error {
	s.workspaceID = workspaceID
	s.threadID = threadID
	s.messages = append([]protocol.ChatMessage(nil), messages...)
	return nil
}

func (s *fakeStore) LoadHistory(_ context.Context, _ string) ([]protocol.ChatMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]protocol.ChatMessage, len(s.messages))
	copy(out, s.messages)
	return out, nil
}

func (s *fakeStore) SaveHistory(_ context.Context, _ string, messages []protocol.ChatMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = make([]protocol.ChatMessage, len(messages))
	copy(s.messages, messages)
	return nil
}

type fakeLoader struct {
	files []bootstrap.File
}

func (l fakeLoader) LoadBootstrap(ctx context.Context) ([]bootstrap.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.files, nil
}

type fakeProvider struct {
	request provider.ChatRequest
	err     error
}

type workspacePromptNoteProvider struct {
	workspaceID string
	calls       int
	authority   tools.MemoryAuthority
}

func (p *workspacePromptNoteProvider) LoadWorkspace(ctx context.Context, workspaceID string) ([]promptnotes.Note, error) {
	p.workspaceID = workspaceID
	p.calls++
	p.authority, _ = tools.MemoryAuthorityFrom(ctx)
	return []promptnotes.Note{{Key: "workspace", Content: "workspace note", Enabled: true, SchemaVersion: 1}}, nil
}

func (p *fakeProvider) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	if err := ctx.Err(); err != nil {
		return provider.ChatResponse{}, err
	}
	p.request = req
	if p.err != nil {
		return provider.ChatResponse{}, p.err
	}
	part, err := protocol.NewTextPart("assistant response")
	if err != nil {
		return provider.ChatResponse{}, err
	}
	return provider.ChatResponse{Message: protocol.ChatMessage{ID: "assistant-1", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}, nil
}

type fakePublisher struct {
	events []channel.Event
}

func (p *fakePublisher) PublishEvent(_ context.Context, event channel.Event) error {
	p.events = append(p.events, event)
	return nil
}

func Test_Runner_Run_carriesMessageWorkingDirectoryIntoTools(t *testing.T) {
	var seenCWD string
	registry := tools.NewRegistry()
	if err := registry.Register("probe_cwd", tools.HandlerFunc(func(ctx context.Context, req tools.Request) (tools.Result, error) {
		seenCWD, _ = tools.WorkingDirectoryFrom(ctx)
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register probe: %v", err)
	}
	call, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-cwd", Name: "probe_cwd"})
	if err != nil {
		t.Fatalf("tool call: %v", err)
	}
	final, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	provider := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "assistant-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{call}}},
		{Message: protocol.ChatMessage{ID: "assistant-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{final}}},
	}}
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Registry:  registry,
		Provider:  provider,
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	user := protocol.ChatMessage{ID: "user-cwd", Role: protocol.RoleUser}
	wantCWD := filepath.Join(t.TempDir(), "code")
	tools.StampWorkingDirectory(&user, wantCWD)

	if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"}, user); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seenCWD != wantCWD {
		t.Fatalf("tool cwd = %q, want %q", seenCWD, wantCWD)
	}
	if len(provider.requests) == 0 || !requestTextContains(provider.requests[0], "Active working directory: "+fmt.Sprintf("%q", wantCWD)) {
		t.Fatalf("selected cwd missing from model request: %#v", provider.requests)
	}
}

func Test_NewRunner_defaultsMaxToolIterationsTo250(t *testing.T) {
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	if runner.maxToolIterations != 250 {
		t.Fatalf("max tool iterations = %d, want 250", runner.maxToolIterations)
	}
}

func Test_Runner_Run_appliesDefaultWorkspaceToWorkspaceLessTurn(t *testing.T) {
	store := &workspaceRecordingStore{}
	runner, err := NewRunner(Config{
		Store:              store,
		Loader:             fakeLoader{},
		Provider:           &fakeProvider{},
		Publisher:          &fakePublisher{},
		Assembler:          contextpack.NewAssembler(contextpack.Config{}),
		Model:              "test-model",
		DefaultWorkspaceID: " project-a ",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	assistant, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "shared"}, userMsg(t, "hello"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if store.workspaceID != "project-a" || store.threadID != "shared" {
		t.Fatalf("store key = (%q, %q)", store.workspaceID, store.threadID)
	}
	if assistant.Addr.WorkspaceID != "project-a" {
		t.Fatalf("assistant workspace = %q", assistant.Addr.WorkspaceID)
	}
}

func Test_Runner_RunCarriesResolvedWorkspaceIntoPromptNoteAuthority(t *testing.T) {
	notes := &workspacePromptNoteProvider{}
	model := &fakeProvider{}
	runner, err := NewRunner(Config{
		Store:              &fakeStore{},
		Loader:             fakeLoader{},
		Provider:           model,
		Publisher:          &fakePublisher{},
		Assembler:          contextpack.NewAssembler(contextpack.Config{PromptNotes: notes}),
		Model:              "test-model",
		DefaultWorkspaceID: "project-default",
		Authority: func(protocol.MessageAddress, protocol.ChatMessage) tools.MemoryAuthority {
			return tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"}
		},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	if _, err := runner.Run(context.Background(), protocol.MessageAddress{ThreadID: "thread"}, userMsg(t, "hello")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if notes.workspaceID != "project-default" {
		t.Fatalf("prompt-note workspace = %q", notes.workspaceID)
	}
	if notes.calls != 1 {
		t.Fatalf("prompt-note authority calls = %d, want one turn snapshot", notes.calls)
	}
	if notes.authority.GrantID != "grant-1" || notes.authority.SubjectID != "alice" {
		t.Fatalf("prompt-note authority context = %#v", notes.authority)
	}
	if !requestTextContains(model.request, "workspace note") {
		t.Fatalf("model request does not contain prompt note: %#v", model.request)
	}
}

func Test_Runner_Run_preservesExplicitWorkspace(t *testing.T) {
	store := &workspaceRecordingStore{}
	runner, err := NewRunner(Config{
		Store:              store,
		Loader:             fakeLoader{},
		Provider:           &fakeProvider{},
		Publisher:          &fakePublisher{},
		Assembler:          contextpack.NewAssembler(contextpack.Config{}),
		Model:              "test-model",
		DefaultWorkspaceID: "project-a",
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	_, err = runner.Run(context.Background(), protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "shared"}, userMsg(t, "hello"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if store.workspaceID != "project-b" {
		t.Fatalf("workspace = %q, want project-b", store.workspaceID)
	}
}

func Test_Runner_Run_dedups_host_precommitted_user_turn(t *testing.T) {
	// Given: the host committed the inbound user turn to the store before
	// dispatching, so the loaded history already ends with it (a different id, as
	// a separate system minted it).
	ctx := context.Background()
	committedPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	store := &fakeStore{messages: []protocol.ChatMessage{
		{ID: "host-committed", Role: protocol.RoleUser, Content: []protocol.ContentPart{committedPart}},
	}}
	provider := &fakeProvider{}
	runner, err := NewRunner(Config{
		Store:                 store,
		Loader:                fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:              provider,
		Publisher:             &fakePublisher{},
		Assembler:             contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:                 "test-model",
		DedupTrailingUserTurn: true,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	incomingPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	user := protocol.ChatMessage{ID: "task-delivered", Role: protocol.RoleUser, Content: []protocol.ContentPart{incomingPart}}

	// When
	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, user); err != nil {
		t.Fatalf("run turn: %v", err)
	}

	// Then: the user turn appears exactly once (the incoming, canonical copy), not
	// doubled, and the assistant follows it.
	if len(store.messages) != 2 {
		t.Fatalf("expected user+assistant (no dup), got %#v", store.messages)
	}
	if store.messages[0].ID != "task-delivered" || store.messages[0].Role != protocol.RoleUser {
		t.Fatalf("expected the incoming user turn to be canonical, got %#v", store.messages[0])
	}
	if store.messages[1].ID != "assistant-1" {
		t.Fatalf("expected assistant reply, got %#v", store.messages[1])
	}
}

func Test_Runner_Run_keeps_precommitted_turn_without_dedup_flag(t *testing.T) {
	// Without the flag, the pre-committed copy is left intact and the incoming turn
	// is appended too (the historical/default behavior — the host must avoid the
	// double-commit some other way).
	ctx := context.Background()
	committedPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	store := &fakeStore{messages: []protocol.ChatMessage{
		{ID: "host-committed", Role: protocol.RoleUser, Content: []protocol.ContentPart{committedPart}},
	}}
	runner, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:  &fakeProvider{},
		Publisher: &fakePublisher{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:     "test-model",
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	incomingPart, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	user := protocol.ChatMessage{ID: "task-delivered", Role: protocol.RoleUser, Content: []protocol.ContentPart{incomingPart}}
	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, user); err != nil {
		t.Fatalf("run turn: %v", err)
	}
	if len(store.messages) != 3 {
		t.Fatalf("expected committed + incoming + assistant (no dedup), got %#v", store.messages)
	}
}

func Test_Runner_Run_persists_user_and_assistant_and_publishes_final_event(t *testing.T) {
	// Given
	ctx := context.Background()
	store := &fakeStore{}
	provider := &fakeProvider{}
	publisher := &fakePublisher{}
	runner, err := NewRunner(Config{
		Store:     store,
		Loader:    fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:  provider,
		Publisher: publisher,
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:     "test-model",
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	user := protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}

	// When
	assistant, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, user)

	// Then
	if err != nil {
		t.Fatalf("run turn: %v", err)
	}
	// The RETURNED message is the canonical finalized message — it carries the
	// stream id (not the provider's per-call id), so callers and the persisted
	// MemoryLayer record match the id the frontend saw stream in.
	streamID := streamMessageID(protocol.MessageAddress{ThreadID: "thread-1"})
	if assistant.ID != streamID {
		t.Fatalf("unexpected assistant: %#v", assistant)
	}
	// The session Store keeps the raw per-iteration provider messages (used for
	// context assembly), so the assistant message there retains its provider id.
	if len(store.messages) != 2 || store.messages[0].ID != "user-1" || store.messages[1].ID != "assistant-1" {
		t.Fatalf("unexpected persisted history: %#v", store.messages)
	}
	if provider.request.Model != "test-model" || len(provider.request.Messages) != 2 {
		t.Fatalf("unexpected provider request: %#v", provider.request)
	}
	// Egress streams the lifecycle: message_started, the answer text appended
	// (the provider here is non-streaming, so finalize appends it so the
	// finalized message equals the stream reconstruction), then message_final.
	if len(publisher.events) != 3 {
		t.Fatalf("expected message_started + part_appended + message_final, got %#v", publisher.events)
	}
	if publisher.events[0].Type != channel.EventMessageStarted {
		t.Fatalf("first event should be message_started: %#v", publisher.events[0])
	}
	last := publisher.events[len(publisher.events)-1]
	if last.Type != channel.EventMessageFinal || last.Message == nil {
		t.Fatalf("last event should be message_final with a message: %#v", last)
	}
}

func Test_Runner_Run_returns_context_canceled_when_context_is_canceled(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &fakeProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}

	// When
	_, err = runner.Run(ctx, protocol.MessageAddress{ThreadID: "thread-1"}, protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}})

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
