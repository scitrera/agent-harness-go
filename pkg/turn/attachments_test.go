package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// recordingProvider captures the messages of each request so a test can assert
// what the resolver produced.
type recordingProvider struct {
	lastMessages []protocol.ChatMessage
}

func (p *recordingProvider) Chat(_ context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	p.lastMessages = req.Messages
	part, _ := protocol.NewTextPart("ok")
	return provider.ChatResponse{Message: protocol.ChatMessage{ID: "asst", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}, nil
}

// stubResolver rewrites every user-role text part to a sentinel so the test can
// confirm Resolve ran on the assembled request before the provider was called.
type stubResolver struct{ called bool }

func (s *stubResolver) Resolve(_ context.Context, _ protocol.MessageAddress, messages []protocol.ChatMessage) ([]protocol.ChatMessage, error) {
	s.called = true
	out := append([]protocol.ChatMessage(nil), messages...)
	for i, m := range out {
		if m.Role != protocol.RoleUser {
			continue
		}
		p, _ := protocol.NewTextPart("RESOLVED")
		mm := m
		mm.Content = []protocol.ContentPart{p}
		out[i] = mm
	}
	return out, nil
}

func TestRunnerInvokesAttachmentResolverBeforeProvider(t *testing.T) {
	prov := &recordingProvider{}
	res := &stubResolver{}
	r, err := NewRunner(Config{
		Store:       &fakeStore{},
		Loader:      fakeLoader{},
		Provider:    prov,
		Assembler:   contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Attachments: res,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "hello")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !res.called {
		t.Fatal("attachment resolver was not invoked")
	}
	// The provider must have received the resolver's output, not the raw history.
	var sawResolved bool
	for _, m := range prov.lastMessages {
		for _, p := range m.Content {
			if tp, ok := p.AsText(); ok && tp.Text == "RESOLVED" {
				sawResolved = true
			}
		}
	}
	if !sawResolved {
		t.Fatalf("provider did not receive resolver output: %+v", prov.lastMessages)
	}
}

func TestNewRunnerDefaultsToNoopResolver(t *testing.T) {
	r, err := NewRunner(Config{
		Store:     &fakeStore{},
		Loader:    fakeLoader{},
		Provider:  &recordingProvider{},
		Assembler: contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, ok := r.attachments.(NoopAttachmentResolver); !ok {
		t.Fatalf("expected NoopAttachmentResolver default, got %T", r.attachments)
	}
}

func TestNoopResolverReturnsMessagesUnchanged(t *testing.T) {
	img, _ := protocol.NewImagePart(protocol.ImagePart{VFSRef: "vfs_x"})
	in := []protocol.ChatMessage{{Role: protocol.RoleUser, Content: []protocol.ContentPart{img}}}
	out, err := NoopAttachmentResolver{}.Resolve(context.Background(), protocol.MessageAddress{}, in)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 1 || len(out[0].Content) != 1 {
		t.Fatalf("noop resolver altered messages: %+v", out)
	}
}
