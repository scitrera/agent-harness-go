package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// A per-turn WithExcludedTools name is dropped from the advertised set — whether
// it's a static tool or a provider tool — and r.toolSpecs is not mutated.
func TestAssembleTurnTools_ExcludedToolsDropped(t *testing.T) {
	static := []provider.ToolSpec{{Name: "spawn_subagent"}, {Name: "read_file"}}
	prov := &fakeDynamicProvider{descs: []tools.Descriptor{{Name: "remote_x"}}}
	r := newToolsRunner(static, prov)

	ctx := WithExcludedTools(context.Background(), []string{"spawn_subagent", "remote_x"})
	tt := r.assembleTurnTools(ctx, protocol.MessageAddress{}, protocol.ChatMessage{})

	for _, s := range tt.specs {
		if s.Name == "spawn_subagent" || s.Name == "remote_x" {
			t.Errorf("excluded tool %q still advertised", s.Name)
		}
	}
	if len(tt.specs) != 1 || tt.specs[0].Name != "read_file" {
		t.Fatalf("expected only read_file advertised, got %+v", tt.specs)
	}
	if _, ok := tt.providerByTool["remote_x"]; ok {
		t.Error("excluded provider tool must not route")
	}
	if len(r.toolSpecs) != 2 {
		t.Errorf("r.toolSpecs must not be mutated by the filter, got %+v", r.toolSpecs)
	}
}

// The exclusion is a hard gate: even a tool that WOULD route to a provider is
// rejected before execution when scoped out (a static registry tool stays
// invokable otherwise). Proves the invokeTool guard, not just the advertise-filter.
func TestInvokeTool_ExcludedToolRejected(t *testing.T) {
	fake := &fakeDynamicProvider{}
	r := &Runner{}
	ctx := WithExcludedTools(context.Background(), []string{"spawn_subagent"})
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "spawn_subagent"}
	tt := turnTools{providerByTool: map[string]ToolProvider{"spawn_subagent": fake}}

	res, err := r.invokeTool(ctx, nil, protocol.MessageAddress{}, call, tt)
	if err != nil {
		t.Fatalf("invokeTool: %v", err)
	}
	if !res.IsError {
		t.Errorf("excluded tool should return an error result, got %+v", res)
	}
	if len(fake.invoked) != 0 {
		t.Errorf("excluded tool must NOT execute, got %d invocations", len(fake.invoked))
	}
}

// Empty exclusion set is a no-op (default turns unchanged).
func TestWithExcludedTools_EmptyIsNoop(t *testing.T) {
	ctx := WithExcludedTools(context.Background(), nil)
	if toolExcluded(ctx, "anything") {
		t.Error("nil exclusion set must exclude nothing")
	}
	ctx = WithExcludedTools(context.Background(), []string{""})
	if excludedTools(ctx) != nil {
		t.Error("all-empty names must produce no exclusion set")
	}
}
