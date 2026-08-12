package turn

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// fakeDynamicProvider implements the ToolProvider seam with a fixed id, standing
// in for a single external tool source (e.g. the platform-bridge registry).
type fakeDynamicProvider struct {
	descs   []tools.Descriptor
	err     error
	invoked []tools.Request
}

func (f *fakeDynamicProvider) ID() string { return "dynamic" }

func (f *fakeDynamicProvider) Tools(_ context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) ([]tools.Descriptor, error) {
	return f.descs, f.err
}

func (f *fakeDynamicProvider) Invoke(_ context.Context, req tools.Request) (tools.Result, error) {
	f.invoked = append(f.invoked, req)
	return tools.Result{CallID: req.CallID, Name: req.Name, Payload: []byte(`{"ok":true}`)}, nil
}

// fakeToolProvider implements the ToolProvider seam with a configurable id, so
// multi-provider routing can be exercised.
type fakeToolProvider struct {
	id      string
	descs   []tools.Descriptor
	err     error
	invoked []tools.Request
}

func (f *fakeToolProvider) ID() string { return f.id }

func (f *fakeToolProvider) Tools(_ context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) ([]tools.Descriptor, error) {
	return f.descs, f.err
}

func (f *fakeToolProvider) Invoke(_ context.Context, req tools.Request) (tools.Result, error) {
	f.invoked = append(f.invoked, req)
	return tools.Result{CallID: req.CallID, Name: req.Name, Payload: []byte(`{"ok":true}`)}, nil
}

// newToolsRunner builds a bare runner over the static specs plus, when set, a
// single ToolProvider on the unified provider list.
func newToolsRunner(static []provider.ToolSpec, p ToolProvider) *Runner {
	names := make(map[string]struct{}, len(static))
	for _, s := range static {
		names[s.Name] = struct{}{}
	}
	r := &Runner{toolSpecs: static, staticToolNames: names}
	if p != nil {
		r.toolProviders = []ToolProvider{p}
		live, err := catalog.NewStandaloneLiveService(catalog.LiveServiceOptions{MaxLease: turnToolCatalogLease})
		if err != nil {
			panic(err)
		}
		r.toolCatalog = live
		r.toolCatalogGen = "test-generation"
	}
	return r
}

func TestAssembleTurnTools(t *testing.T) {
	static := []provider.ToolSpec{{Name: "local_a"}, {Name: "dup"}}

	t.Run("nil provider returns static only", func(t *testing.T) {
		r := newToolsRunner(static, nil)
		tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
		if len(tt.specs) != 2 || tt.providerByTool != nil {
			t.Fatalf("expected static-only specs and nil route map, got %d specs / %v", len(tt.specs), tt.providerByTool)
		}
	})

	t.Run("merges discovered tools; static wins on collision", func(t *testing.T) {
		upstreamRef := json.RawMessage(`{"provider_id":"tools-wss","registration_id":"office-1"}`)
		r := newToolsRunner(static, &fakeDynamicProvider{descs: []tools.Descriptor{
			{Name: "remote_x", Description: "x", Concurrency: tools.ConcurrencyParallelSafe,
				CatalogMeta: map[string]json.RawMessage{"provider_tool_ref": upstreamRef}},
			{Name: "dup", Description: "should be dropped"},      // collides with a static tool
			{Name: "remote_x", Description: "duplicate dynamic"}, // intra-batch dup
		}})
		tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
		// static (2) + remote_x (1); dup dropped (static wins), remote_x deduped.
		if len(tt.specs) != 3 {
			t.Fatalf("expected 3 specs, got %d: %+v", len(tt.specs), tt.specs)
		}
		if _, ok := tt.providerByTool["remote_x"]; !ok {
			t.Error("remote_x should route to a provider")
		}
		if _, ok := tt.providerByTool["dup"]; ok {
			t.Error("dup collides with a static tool and must NOT route to a provider")
		}
		if len(tt.providerByTool) != 1 {
			t.Errorf("expected exactly 1 routed tool, got %d", len(tt.providerByTool))
		}
		if tt.concurrencyByTool["remote_x"] != tools.ConcurrencyParallelSafe {
			t.Errorf("remote_x concurrency contract was not retained: %+v", tt.concurrencyByTool)
		}
		ref, ok := tt.refByTool["remote_x"]
		if !ok || ref.ProviderID != "dynamic" || ref.Name != "remote_x" || ref.Revision == "" {
			t.Errorf("remote_x exact catalog reference was not retained: %+v", ref)
		}
		call := protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "remote_x"}
		bound, err := bindCatalogToolCall(call, tt)
		if err != nil {
			t.Fatalf("bind catalog call: %v", err)
		}
		if _, err := r.invokeTool(context.Background(), nil, protocol.MessageAddress{}, bound, tt); err != nil {
			t.Fatalf("invoke provider tool: %v", err)
		}
		provider := tt.providerByTool["remote_x"].(*fakeDynamicProvider)
		if len(provider.invoked) != 1 || string(provider.invoked[0].CatalogMeta["provider_tool_ref"]) != string(upstreamRef) {
			t.Fatalf("provider admitted catalog metadata = %+v", provider.invoked)
		}
	})

	t.Run("discovery error degrades to static", func(t *testing.T) {
		r := newToolsRunner(static, &fakeDynamicProvider{err: errors.New("bridge down")})
		tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
		if len(tt.specs) != 2 || tt.providerByTool != nil {
			t.Fatalf("expected static-only on error, got %d specs / %v", len(tt.specs), tt.providerByTool)
		}
	})
}

// Two providers each contribute a tool; both merge into the model-visible set and
// each invocation routes to the provider that surfaced it (the new multi-provider
// capability). Earlier-in-list ordering is exercised by the distinct names.
func TestAssembleTurnTools_MultipleProviders(t *testing.T) {
	static := []provider.ToolSpec{{Name: "local_a"}}
	p1 := &fakeToolProvider{id: "p1", descs: []tools.Descriptor{{Name: "alpha", Description: "a"}}}
	p2 := &fakeToolProvider{id: "p2", descs: []tools.Descriptor{{Name: "beta", Description: "b"}}}
	r := newToolsRunner(static, p1)
	r.toolProviders = append(r.toolProviders, p2)

	tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
	if len(tt.specs) != 3 {
		t.Fatalf("expected 3 specs (static + 2 providers), got %d: %+v", len(tt.specs), tt.specs)
	}
	if tt.providerByTool["alpha"] != ToolProvider(p1) {
		t.Errorf("alpha should route to p1")
	}
	if tt.providerByTool["beta"] != ToolProvider(p2) {
		t.Errorf("beta should route to p2")
	}

	if _, err := r.invokeTool(context.Background(), nil, protocol.MessageAddress{}, protocol.ToolInvokeEnvelope{CallID: "c1", Name: "alpha"}, tt); err != nil {
		t.Fatalf("invoke alpha: %v", err)
	}
	if _, err := r.invokeTool(context.Background(), nil, protocol.MessageAddress{}, protocol.ToolInvokeEnvelope{CallID: "c2", Name: "beta"}, tt); err != nil {
		t.Fatalf("invoke beta: %v", err)
	}
	if len(p1.invoked) != 1 || p1.invoked[0].Name != "alpha" {
		t.Errorf("p1 should have been invoked once for alpha, got %+v", p1.invoked)
	}
	if len(p2.invoked) != 1 || p2.invoked[0].Name != "beta" {
		t.Errorf("p2 should have been invoked once for beta, got %+v", p2.invoked)
	}
}

func TestInvokeTool_RoutesDynamicWithAuthority(t *testing.T) {
	fake := &fakeDynamicProvider{descs: []tools.Descriptor{{Name: "remote_x", Description: "remote"}}}
	r := newToolsRunner(nil, fake)
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{SubjectType: "user", SubjectID: "alice", GrantID: "g1"})
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "remote_x"}
	tt := r.assembleTurnTools(ctx, protocol.MessageAddress{WorkspaceID: "ws1", ThreadID: "t1"}, protocol.ChatMessage{})

	res, err := r.invokeTool(ctx, nil, protocol.MessageAddress{}, call, tt)
	if err != nil {
		t.Fatalf("invokeTool: %v", err)
	}
	if res.Name != "remote_x" {
		t.Errorf("result name = %q, want remote_x", res.Name)
	}
	if len(fake.invoked) != 1 {
		t.Fatalf("expected dynamic provider invoked once, got %d", len(fake.invoked))
	}
	if got := fake.invoked[0].Authority; got.SubjectID != "alice" || got.GrantID != "g1" {
		t.Errorf("dynamic invoke authority = %+v, want subject=alice grant=g1", got)
	}
	if fake.invoked[0].ToolRef == nil || !toolReferencesEqual(*fake.invoked[0].ToolRef, tt.refByTool["remote_x"]) {
		t.Errorf("dynamic invoke exact ref = %+v, want %+v", fake.invoked[0].ToolRef, tt.refByTool["remote_x"])
	}
}

func TestInvokeTool_RejectsMismatchedCatalogReference(t *testing.T) {
	fake := &fakeDynamicProvider{descs: []tools.Descriptor{{Name: "remote_x", Description: "remote"}}}
	r := newToolsRunner(nil, fake)
	tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
	wrong := tt.refByTool["remote_x"]
	wrong.Revision = "sha256:not-the-admitted-revision"
	_, err := r.invokeTool(context.Background(), nil, protocol.MessageAddress{}, protocol.ToolInvokeEnvelope{
		CallID: "c1", Name: "remote_x", ToolRef: &wrong,
	}, tt)
	if err == nil {
		t.Fatal("mismatched exact catalog reference should be rejected")
	}
	if len(fake.invoked) != 0 {
		t.Fatalf("provider invoked despite mismatched exact ref: %+v", fake.invoked)
	}
}

func TestAssembleTurnTools_CatalogDiscoveryDecisionIsIndependent(t *testing.T) {
	fake := &fakeDynamicProvider{descs: []tools.Descriptor{{Name: "remote_x", Description: "remote"}}}
	r := newToolsRunner(nil, fake)
	live, err := catalog.NewStandaloneLiveService(catalog.LiveServiceOptions{
		MaxLease: turnToolCatalogLease,
		Authorizer: catalog.EntryAuthorizerFunc(func(_ context.Context, action string, _ catalog.QueryBinding, entry spec.ToolCatalogEntry) (bool, error) {
			if entry.Effect != spec.ToolEffectExecute {
				t.Errorf("default catalog effect = %q, want execute", entry.Effect)
			}
			return action != catalog.CatalogActionDiscover, nil
		}),
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	r.toolCatalog = live
	tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
	if len(tt.specs) != 0 || len(tt.providerByTool) != 0 || len(tt.refByTool) != 0 {
		t.Fatalf("discovery-denied provider tool was advertised: %+v", tt)
	}
}

func TestInvokeTool_RejectsExpiredCatalogGeneration(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	fake := &fakeDynamicProvider{descs: []tools.Descriptor{{Name: "remote_x", Description: "remote"}}}
	r := newToolsRunner(nil, fake)
	live, err := catalog.NewStandaloneLiveService(catalog.LiveServiceOptions{
		Now: func() time.Time { return now }, MaxLease: turnToolCatalogLease,
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	r.toolCatalog = live
	r.now = func() time.Time { return now }
	tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
	now = now.Add(turnToolCatalogLease + time.Second)
	_, err = r.invokeTool(context.Background(), nil, protocol.MessageAddress{}, protocol.ToolInvokeEnvelope{
		CallID: "c1", Name: "remote_x",
	}, tt)
	if err == nil {
		t.Fatal("expired catalog generation should be rejected")
	}
	if len(fake.invoked) != 0 {
		t.Fatalf("provider invoked after catalog lease expiry: %+v", fake.invoked)
	}
}

// Regression: a DYNAMIC (discovered) tool's result must land in session history
// just like a static tool's, so the model sees it on the next iteration and
// doesn't re-call the tool in an infinite loop (the bridge frontend_*/backend_*
// looping bug). Exercises the full turn loop, not just invokeTool.
func Test_Runner_Run_dynamic_tool_result_appended_to_history(t *testing.T) {
	ctx := context.Background()
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "remote_x"})
	if err != nil {
		t.Fatalf("tool call part: %v", err)
	}
	finalPart, err := protocol.NewTextPart("done")
	if err != nil {
		t.Fatalf("final text: %v", err)
	}
	prov := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	dyn := &fakeDynamicProvider{descs: []tools.Descriptor{
		{Name: "remote_x", Description: "remote", Parameters: json.RawMessage(`{"type":"object"}`)},
	}}
	store := &fakeStore{}
	r, err := NewRunner(Config{
		Store:             store,
		Loader:            fakeLoader{},
		Registry:          tools.NewRegistry(),
		Provider:          prov,
		Assembler:         contextpack.NewAssembler(contextpack.Config{MaxHistoryMessages: 10}),
		ToolProviders:     []ToolProvider{dyn},
		MaxToolIterations: 2,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}

	_, err = r.Run(ctx, protocol.MessageAddress{ThreadID: "t1"}, userMessage(t, "use remote"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// The model was reprompted after the tool ran (2 provider calls), and the
	// dynamic tool's result is persisted as a tool-result message.
	if len(prov.requests) != 2 {
		t.Fatalf("expected 2 provider calls (reprompt after tool), got %d", len(prov.requests))
	}
	if len(dyn.invoked) != 1 {
		t.Fatalf("expected dynamic tool invoked once, got %d", len(dyn.invoked))
	}
	if dyn.invoked[0].ToolRef == nil {
		t.Fatal("dynamic tool invocation did not carry an exact catalog reference")
	}
	var toolResults int
	for _, m := range store.messages {
		if m.Role == protocol.RoleToolResult {
			toolResults++
		}
	}
	if toolResults != 1 {
		t.Fatalf("dynamic tool result not persisted to history: %#v", store.messages)
	}
}
