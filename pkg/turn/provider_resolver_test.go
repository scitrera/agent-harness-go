package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

func multiProviderRegistry() *modelpkg.Registry {
	return modelpkg.NewRegistryWithProviders(
		[]modelpkg.Model{
			{Name: "on-openrouter", Capabilities: modelpkg.Capabilities{Tools: true}, Provider: "openrouter"},
			{Name: "on-groq", Capabilities: modelpkg.Capabilities{Tools: true}, Provider: "groq"},
			{Name: "env-keyed", Capabilities: modelpkg.Capabilities{Tools: true}, Provider: "enved"},
			{Name: "bare", Capabilities: modelpkg.Capabilities{Tools: true}},
		},
		"bare",
		[]modelpkg.ProviderConfig{
			{Name: "openrouter", BaseURL: "https://openrouter.example/api", APIKey: "or-key", Format: "openai"},
			{Name: "groq", BaseURL: "https://groq.example/api", APIKey: "groq-key", Format: "native"},
			{Name: "enved", BaseURL: "https://enved.example/api", APIKeyEnv: "TEST_PROVIDER_KEY"},
		},
	)
}

func TestNewProviderResolverNilRegistry(t *testing.T) {
	if r := NewProviderResolver(nil, modelpkg.ProviderConfig{}, nil); r != nil {
		t.Fatal("nil registry should yield a nil resolver")
	}
}

func TestProviderResolverPerModel(t *testing.T) {
	reg := multiProviderRegistry()
	r := NewProviderResolver(reg, modelpkg.ProviderConfig{BaseURL: "http://default.local", Format: "openai"}, nil)

	// A model referencing a provider yields a non-nil client built from that
	// provider's BaseURL.
	p1, ok := r.ProviderForModel("on-openrouter")
	if !ok || p1 == nil {
		t.Fatalf("on-openrouter: ok=%v p=%v", ok, p1)
	}
	if got := p1.(*provider.SidecarClient).BaseURL(); got != "https://openrouter.example/api" {
		t.Fatalf("on-openrouter base url = %q", got)
	}

	// A distinct provider yields a distinct client.
	p2, ok := r.ProviderForModel("on-groq")
	if !ok || p2 == nil {
		t.Fatalf("on-groq: ok=%v p=%v", ok, p2)
	}
	if p1 == p2 {
		t.Fatal("expected distinct clients per provider")
	}
	if got := p2.(*provider.SidecarClient).BaseURL(); got != "https://groq.example/api" {
		t.Fatalf("on-groq base url = %q", got)
	}

	// The client is cached: a repeat request returns the same instance.
	p1again, _ := r.ProviderForModel("on-openrouter")
	if p1again != p1 {
		t.Fatal("expected cached client on repeat resolution")
	}

	// A bare model (no provider) → default.
	if p, ok := r.ProviderForModel("bare"); ok || p != nil {
		t.Fatalf("bare model should fall back to default: ok=%v p=%v", ok, p)
	}
	// An unknown model → default.
	if p, ok := r.ProviderForModel("ghost"); ok || p != nil {
		t.Fatalf("unknown model should fall back to default: ok=%v p=%v", ok, p)
	}
}

func TestProviderResolverEnvKeyResolution(t *testing.T) {
	reg := multiProviderRegistry()
	r := NewProviderResolver(reg, modelpkg.ProviderConfig{}, nil).(*providerResolver)

	t.Setenv("TEST_PROVIDER_KEY", "secret-from-env")
	pc, _ := reg.ProviderFor("env-keyed")
	cfg := r.sidecarConfig(pc)
	if cfg.AuthHeader != "Bearer secret-from-env" {
		t.Fatalf("env-var key not resolved into auth header: %q", cfg.AuthHeader)
	}
	if cfg.BaseURL != "https://enved.example/api" {
		t.Fatalf("base url = %q", cfg.BaseURL)
	}
}

func TestProviderResolverFillsFromDefault(t *testing.T) {
	// A provider that omits base_url/format/key inherits them from the env default.
	reg := modelpkg.NewRegistryWithProviders(
		[]modelpkg.Model{{Name: "m", Capabilities: modelpkg.Capabilities{Tools: true}, Provider: "sparse"}},
		"m",
		[]modelpkg.ProviderConfig{{Name: "sparse"}},
	)
	def := modelpkg.ProviderConfig{BaseURL: "http://default.local", APIKey: "def-key", Format: "native"}
	r := NewProviderResolver(reg, def, nil).(*providerResolver)
	pc, _ := reg.ProviderFor("m")
	cfg := r.sidecarConfig(pc)
	if cfg.BaseURL != "http://default.local" {
		t.Fatalf("base url not filled from default: %q", cfg.BaseURL)
	}
	if cfg.AuthHeader != "Bearer def-key" {
		t.Fatalf("key not filled from default: %q", cfg.AuthHeader)
	}
	if cfg.Format != provider.FormatNative {
		t.Fatalf("format not filled from default: %q", cfg.Format)
	}
}

// recordingProviderNamed records whether it was called; distinct instances stand
// in for distinct upstreams so a test can assert which one served a turn.
type recordingProviderNamed struct {
	name   string
	called bool
}

func (p *recordingProviderNamed) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	if err := ctx.Err(); err != nil {
		return provider.ChatResponse{}, err
	}
	p.called = true
	part, err := protocol.NewTextPart("reply from " + p.name)
	if err != nil {
		return provider.ChatResponse{}, err
	}
	return provider.ChatResponse{Message: protocol.ChatMessage{ID: "asst-" + p.name, Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}, nil
}

// fakeResolver routes exactly one model name to a specific provider; every other
// model falls back to the default (returns nil,false).
type fakeResolver struct {
	model string
	p     Provider
}

func (f *fakeResolver) ProviderForModel(model string) (Provider, bool) {
	if model == f.model {
		return f.p, true
	}
	return nil, false
}

func Test_Runner_uses_resolver_provider_for_matching_model(t *testing.T) {
	ctx := context.Background()
	special := &recordingProviderNamed{name: "special"}
	def := &recordingProviderNamed{name: "default"}
	resolver := &fakeResolver{model: "special-model", p: special}

	// Turn 1: the turn's model is "special-model" → the resolver's provider serves it.
	store := &fakeStore{}
	runner, err := NewRunner(Config{
		Store:            store,
		Loader:           fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:         def,
		Publisher:        &fakePublisher{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:            "special-model",
		ProviderResolver: resolver,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	part, _ := protocol.NewTextPart("hi")
	user := protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, user); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !special.called {
		t.Fatal("expected the resolver's provider to serve the matching model")
	}
	if def.called {
		t.Fatal("did not expect the default provider to be called for the matching model")
	}
}

func Test_Runner_uses_default_provider_when_resolver_declines(t *testing.T) {
	ctx := context.Background()
	special := &recordingProviderNamed{name: "special"}
	def := &recordingProviderNamed{name: "default"}
	// Resolver only matches "special-model"; the turn model is "plain-model".
	resolver := &fakeResolver{model: "special-model", p: special}

	store := &fakeStore{}
	runner, err := NewRunner(Config{
		Store:            store,
		Loader:           fakeLoader{files: []bootstrap.File{{Name: "SOUL.md", Content: "name: Scitrera"}}},
		Provider:         def,
		Publisher:        &fakePublisher{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 8}),
		Model:            "plain-model",
		ProviderResolver: resolver,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	part, _ := protocol.NewTextPart("hi")
	user := protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
	if _, err := runner.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, user); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !def.called {
		t.Fatal("expected the default provider to serve a model the resolver declines")
	}
	if special.called {
		t.Fatal("did not expect the resolver's provider to be called for a non-matching model")
	}
}
