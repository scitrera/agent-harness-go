package turn

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

type fakeStore struct {
	messages []protocol.ChatMessage
}

func (s *fakeStore) LoadHistory(_ context.Context, _ string) ([]protocol.ChatMessage, error) {
	out := make([]protocol.ChatMessage, len(s.messages))
	copy(out, s.messages)
	return out, nil
}

func (s *fakeStore) SaveHistory(_ context.Context, _ string, messages []protocol.ChatMessage) error {
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
	if assistant.ID != "assistant-1" {
		t.Fatalf("unexpected assistant: %#v", assistant)
	}
	if len(store.messages) != 2 || store.messages[0].ID != "user-1" || store.messages[1].ID != "assistant-1" {
		t.Fatalf("unexpected persisted history: %#v", store.messages)
	}
	if provider.request.Model != "test-model" || len(provider.request.Messages) != 2 {
		t.Fatalf("unexpected provider request: %#v", provider.request)
	}
	// Egress streams the lifecycle: message_started first, message_finalized last.
	if len(publisher.events) != 2 {
		t.Fatalf("expected message_started + message_final, got %#v", publisher.events)
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
