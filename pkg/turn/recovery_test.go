package turn

import (
	"context"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
)

// recoveryProvider records each call's model and defers the success/failure
// decision to fail(call, model). fail returns nil to succeed.
type recoveryProvider struct {
	calls   int
	models  []string
	efforts []string
	fail    func(call int, model string) error
}

func (p *recoveryProvider) Chat(_ context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	p.calls++
	p.models = append(p.models, req.Model)
	p.efforts = append(p.efforts, req.ReasoningEffort)
	if err := p.fail(p.calls, req.Model); err != nil {
		return provider.ChatResponse{}, err
	}
	part, _ := protocol.NewTextPart("ok")
	return provider.ChatResponse{Message: protocol.ChatMessage{ID: "assistant-ok", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{part}}}, nil
}

// noBackoff disables the real sleep so tests don't wait.
func noBackoff(int) time.Duration { return 0 }

// fallbackSelector picks Default for the initial pick and `to` on any retry.
type fallbackSelector struct{ to string }

func (s fallbackSelector) SelectModel(_ context.Context, in modelpkg.SelectInput) (string, error) {
	if len(in.Attempts) == 0 {
		return in.Default, nil
	}
	return s.to, nil
}

func twoModelRegistry() *modelpkg.Registry {
	return modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "primary", Capabilities: modelpkg.Capabilities{Tools: true, Vision: true}},
		{Name: "fast", Capabilities: modelpkg.Capabilities{Tools: true}},
	}, "primary")
}

func TestRecovery_TransientBackoffSameModel(t *testing.T) {
	// Two transient failures then success: default budget (2) covers it.
	prov := &recoveryProvider{fail: func(call int, _ string) error {
		if call <= 2 {
			return &provider.ProviderError{Kind: provider.FailureServer, Status: 503, Body: "upstream"}
		}
		return nil
	}}
	r, err := NewRunner(Config{
		Store:        &fakeStore{},
		Loader:       fakeLoader{},
		Provider:     prov,
		Assembler:    contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		RetryBackoff: noBackoff,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	assistant, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if assistant.ID != "assistant-ok" {
		t.Fatalf("expected recovery, got %#v", assistant)
	}
	if prov.calls != 3 {
		t.Fatalf("expected 2 transient failures + 1 success, got %d calls", prov.calls)
	}
}

func TestRecovery_TransientExhaustedSurfaces(t *testing.T) {
	prov := &recoveryProvider{fail: func(int, string) error {
		return &provider.ProviderError{Kind: provider.FailureOverloaded, Status: 529, Body: "overloaded"}
	}}
	r, err := NewRunner(Config{
		Store:        &fakeStore{},
		Loader:       fakeLoader{},
		Provider:     prov,
		Assembler:    contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		RetryBackoff: noBackoff,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err == nil {
		t.Fatal("expected transient failure to surface after retries exhausted")
	}
	if prov.calls != 3 { // initial + default 2 transient retries
		t.Fatalf("expected 3 calls, got %d", prov.calls)
	}
}

func TestRecovery_TransientDisabled(t *testing.T) {
	prov := &recoveryProvider{fail: func(int, string) error {
		return &provider.ProviderError{Kind: provider.FailureServer, Status: 500, Body: "boom"}
	}}
	r, err := NewRunner(Config{
		Store:               &fakeStore{},
		Loader:              fakeLoader{},
		Provider:            prov,
		Assembler:           contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		MaxTransientRetries: -1, // disabled → surface immediately
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err == nil {
		t.Fatal("expected immediate surface with transient retries disabled")
	}
	if prov.calls != 1 {
		t.Fatalf("expected exactly 1 call, got %d", prov.calls)
	}
}

func TestRecovery_CrossModelFallback(t *testing.T) {
	// Non-retryable, non-terminal failure on the initial model; the selector
	// escalates to "fast", which succeeds.
	prov := &recoveryProvider{fail: func(_ int, model string) error {
		if model == "primary" {
			return &provider.ProviderError{Kind: provider.FailureUnknown, Status: 418, Body: "teapot"}
		}
		return nil
	}}
	r, err := NewRunner(Config{
		Store:         &fakeStore{},
		Loader:        fakeLoader{},
		Provider:      prov,
		Assembler:     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Model:         "primary",
		ModelRegistry: twoModelRegistry(),
		ModelSelector: fallbackSelector{to: "fast"},
		RetryBackoff:  noBackoff,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	assistant, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if assistant.ID != "assistant-ok" {
		t.Fatalf("expected fallback recovery, got %#v", assistant)
	}
	if len(prov.models) != 2 || prov.models[0] != "primary" || prov.models[1] != "fast" {
		t.Fatalf("expected primary then fast, got %v", prov.models)
	}
}

func TestRecovery_RecomputesReasoningForFallbackModel(t *testing.T) {
	registry := modelpkg.NewRegistry([]modelpkg.Model{
		{Name: "primary", Capabilities: modelpkg.Capabilities{Tools: true}, Reasoning: modelpkg.ReasoningConfig{
			DefaultEffort: "high", AllowedEfforts: []string{"high", "xhigh"},
		}},
		{Name: "fast", Capabilities: modelpkg.Capabilities{Tools: true}, Reasoning: modelpkg.ReasoningConfig{
			DefaultEffort: "low", AllowedEfforts: []string{"low", "medium"},
		}},
	}, "primary")
	prov := &recoveryProvider{fail: func(_ int, model string) error {
		if model == "primary" {
			return &provider.ProviderError{Kind: provider.FailureUnknown, Status: 418, Body: "teapot"}
		}
		return nil
	}}
	r, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Provider: prov,
		Assembler: contextpack.NewAssembler(contextpack.Config{}),
		Model:     "primary", ModelRegistry: registry, ModelSelector: fallbackSelector{to: "fast"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err != nil {
		t.Fatal(err)
	}
	if len(prov.efforts) != 2 || prov.efforts[0] != "high" || prov.efforts[1] != "low" {
		t.Fatalf("fallback efforts = %#v, want [high low]", prov.efforts)
	}
}

func TestRecovery_CapabilityDefaultDeclinesFallback(t *testing.T) {
	// With a registry but only the default CapabilityDefault selector, a
	// non-retryable failure must NOT switch models (fallback is opt-in).
	prov := &recoveryProvider{fail: func(int, string) error {
		return &provider.ProviderError{Kind: provider.FailureUnknown, Status: 418, Body: "teapot"}
	}}
	r, err := NewRunner(Config{
		Store:         &fakeStore{},
		Loader:        fakeLoader{},
		Provider:      prov,
		Assembler:     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Model:         "primary",
		ModelRegistry: twoModelRegistry(), // ModelSelector nil → CapabilityDefault
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err == nil {
		t.Fatal("expected error: CapabilityDefault must not fall back across models")
	}
	if prov.calls != 1 {
		t.Fatalf("expected exactly 1 call (no fallback), got %d", prov.calls)
	}
}

func TestRecovery_TerminalNeverSwitches(t *testing.T) {
	// Auth failure is terminal: never retried or switched, even with a fallback
	// selector wired.
	prov := &recoveryProvider{fail: func(int, string) error {
		return &provider.ProviderError{Kind: provider.FailureAuth, Status: 401, Body: "bad key"}
	}}
	r, err := NewRunner(Config{
		Store:         &fakeStore{},
		Loader:        fakeLoader{},
		Provider:      prov,
		Assembler:     contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 50}),
		Model:         "primary",
		ModelRegistry: twoModelRegistry(),
		ModelSelector: fallbackSelector{to: "fast"},
		RetryBackoff:  noBackoff,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	if _, err := r.Run(context.Background(), protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "q")); err == nil {
		t.Fatal("expected auth failure to surface immediately")
	}
	if prov.calls != 1 {
		t.Fatalf("expected exactly 1 call for a terminal failure, got %d", prov.calls)
	}
}
