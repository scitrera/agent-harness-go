package turn

import (
	"context"
	"errors"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type fakeDynamicProvider struct {
	descs   []tools.Descriptor
	err     error
	invoked []tools.Request
}

func (f *fakeDynamicProvider) Discover(_ context.Context, _ protocol.MessageAddress, _ protocol.ChatMessage) ([]tools.Descriptor, error) {
	return f.descs, f.err
}

func (f *fakeDynamicProvider) Invoke(_ context.Context, req tools.Request) (tools.Result, error) {
	f.invoked = append(f.invoked, req)
	return tools.Result{CallID: req.CallID, Name: req.Name, Payload: []byte(`{"ok":true}`)}, nil
}

func newToolsRunner(static []provider.ToolSpec, dyn DynamicToolProvider) *Runner {
	names := make(map[string]struct{}, len(static))
	for _, s := range static {
		names[s.Name] = struct{}{}
	}
	return &Runner{toolSpecs: static, staticToolNames: names, dynamicTools: dyn}
}

func TestAssembleTurnTools(t *testing.T) {
	static := []provider.ToolSpec{{Name: "local_a"}, {Name: "dup"}}

	t.Run("nil provider returns static only", func(t *testing.T) {
		r := newToolsRunner(static, nil)
		tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
		if len(tt.specs) != 2 || tt.dynamicNames != nil {
			t.Fatalf("expected static-only specs and nil dynamicNames, got %d specs / %v", len(tt.specs), tt.dynamicNames)
		}
	})

	t.Run("merges discovered tools; static wins on collision", func(t *testing.T) {
		r := newToolsRunner(static, &fakeDynamicProvider{descs: []tools.Descriptor{
			{Name: "remote_x", Description: "x"},
			{Name: "dup", Description: "should be dropped"}, // collides with a static tool
			{Name: "remote_x", Description: "duplicate dynamic"}, // intra-batch dup
		}})
		tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
		// static (2) + remote_x (1); dup dropped (static wins), remote_x deduped.
		if len(tt.specs) != 3 {
			t.Fatalf("expected 3 specs, got %d: %+v", len(tt.specs), tt.specs)
		}
		if _, ok := tt.dynamicNames["remote_x"]; !ok {
			t.Error("remote_x should be a dynamic name")
		}
		if _, ok := tt.dynamicNames["dup"]; ok {
			t.Error("dup collides with a static tool and must NOT be dynamic")
		}
		if len(tt.dynamicNames) != 1 {
			t.Errorf("expected exactly 1 dynamic name, got %d", len(tt.dynamicNames))
		}
	})

	t.Run("discovery error degrades to static", func(t *testing.T) {
		r := newToolsRunner(static, &fakeDynamicProvider{err: errors.New("bridge down")})
		tt := r.assembleTurnTools(context.Background(), protocol.MessageAddress{}, protocol.ChatMessage{})
		if len(tt.specs) != 2 || tt.dynamicNames != nil {
			t.Fatalf("expected static-only on error, got %d specs / %v", len(tt.specs), tt.dynamicNames)
		}
	})
}

func TestInvokeTool_RoutesDynamicWithAuthority(t *testing.T) {
	fake := &fakeDynamicProvider{}
	r := &Runner{dynamicTools: fake}
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{SubjectType: "user", SubjectID: "alice", GrantID: "g1"})
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "remote_x"}
	tt := turnTools{dynamicNames: map[string]struct{}{"remote_x": {}}}

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
}
