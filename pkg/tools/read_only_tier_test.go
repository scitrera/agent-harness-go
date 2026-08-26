package tools

import (
	"context"
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/mcp"
)

// denyAll stands in for a base policy that pre-authorizes nothing, so any
// allow observed in these tests can only have come from the read-only tier.
type denyAll struct{}

func (denyAll) Decide(Request) Decision {
	return Decision{Code: DecisionRequiresApproval, Reason: "tool is not pre-authorized", AuditCode: "tool.approval_required"}
}

func localToolRegistry(t *testing.T) *Registry {
	t.Helper()
	workspace, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	registry := NewRegistry()
	if err := RegisterLocal(registry, LocalConfig{Workspace: workspace, Exa: &localtools.ExaClient{}}); err != nil {
		t.Fatalf("register tools: %v", err)
	}
	return registry
}

func TestLocalDescriptorsDeclareTheirEffect(t *testing.T) {
	// Given: every local tool should classify itself, since an unclassified tool
	// is invisible to any effect-based policy.
	registry := localToolRegistry(t)
	want := map[string]spec.ToolEffect{
		"read_file":    spec.ToolEffectRead,
		"list_dir":     spec.ToolEffectRead,
		"inspect_file": spec.ToolEffectRead,
		"write_file":   spec.ToolEffectWrite,
		"edit_file":    spec.ToolEffectWrite,
		"apply_patch":  spec.ToolEffectWrite,
		"shell":        spec.ToolEffectExecute,
		"python":       spec.ToolEffectExecute,
		"web_search":   spec.ToolEffectExternal,
	}

	// When / Then
	for name, wantEffect := range want {
		got, ok := registry.EffectOf(name)
		if !ok {
			t.Errorf("%s declares no effect", name)
			continue
		}
		if got != wantEffect {
			t.Errorf("%s effect = %q, want %q", name, got, wantEffect)
		}
	}
}

func TestReadOnlyTierAllowsReadsAndStillPromptsForTheRest(t *testing.T) {
	// Given
	registry := localToolRegistry(t)
	policy := NewReadOnlyAutoApprove(denyAll{}, registry.EffectOf)

	// When / Then: reads pass without a prompt...
	for _, name := range []string{"read_file", "list_dir", "inspect_file"} {
		if d := policy.Decide(Request{Name: name}); !d.Allowed() {
			t.Errorf("%s = %q, want allow", name, d.Code)
		}
	}
	// ...and everything that can change the world still stops for a human.
	for _, name := range []string{"write_file", "edit_file", "apply_patch", "shell", "python", "web_search"} {
		if d := policy.Decide(Request{Name: name}); d.Allowed() {
			t.Errorf("%s was auto-approved; only read effects qualify", name)
		}
	}
}

func TestReadOnlyTierDoesNotApproveUnclassifiedTools(t *testing.T) {
	// Given: a tool registered with no descriptor at all. Treating "never said"
	// as "read" would auto-approve every tool that forgot to classify itself.
	registry := NewRegistry()
	if err := registry.Register("mystery", HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{}`))
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	policy := NewReadOnlyAutoApprove(denyAll{}, registry.EffectOf)

	// When
	decision := policy.Decide(Request{Name: "mystery"})

	// Then
	if decision.Allowed() {
		t.Fatal("an unclassified tool must not be auto-approved")
	}
	if _, ok := registry.EffectOf("mystery"); ok {
		t.Fatal("EffectOf must report false for a tool that declared no effect")
	}
}

func TestReadOnlyTierWithNoLookupFailsClosed(t *testing.T) {
	// Given: a misconfiguration that forgot the lookup.
	policy := NewReadOnlyAutoApprove(denyAll{}, nil)

	// When / Then: it must defer, not approve.
	if policy.Decide(Request{Name: "read_file"}).Allowed() {
		t.Fatal("a nil lookup must disable the tier, not approve everything")
	}
}

func TestReadOnlyTierDefersToTheBasePolicyForNonReads(t *testing.T) {
	// Given: a base policy that pre-authorizes shell.
	registry := localToolRegistry(t)
	base := StaticPolicy{Allowed: map[string]string{"shell": "configured local tool"}}
	policy := NewReadOnlyAutoApprove(base, registry.EffectOf)

	// When / Then: the tier adds reads without taking away the base's allowances.
	if d := policy.Decide(Request{Name: "shell"}); !d.Allowed() {
		t.Fatalf("shell = %q, want the base policy's allow to survive", d.Code)
	}
	if d := policy.Decide(Request{Name: "read_file"}); d.AuditCode != "tool.read_only" {
		t.Fatalf("read_file audit code = %q, want tool.read_only", d.AuditCode)
	}
}

func TestMCPEffectTrustsOnlyAnExplicitReadOnlyHint(t *testing.T) {
	// Given: annotations are the server's claim about itself, so only an
	// explicit true may downgrade the class.
	readOnly, notReadOnly := true, false
	cases := []struct {
		name string
		tool mcp.Tool
		want spec.ToolEffect
	}{
		{"explicit read-only", mcp.Tool{Annotations: mcp.ToolAnnotations{ReadOnlyHint: &readOnly}}, spec.ToolEffectRead},
		{"explicit not read-only", mcp.Tool{Annotations: mcp.ToolAnnotations{ReadOnlyHint: &notReadOnly}}, spec.ToolEffectExecute},
		{"absent annotation", mcp.Tool{}, spec.ToolEffectExecute},
	}

	// When / Then
	for _, tc := range cases {
		if got := mcpEffect(tc.tool); got != tc.want {
			t.Errorf("%s: effect = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestMCPToolDecodesReadOnlyHint(t *testing.T) {
	// Given a server's tools/list entry.
	var tool mcp.Tool
	if err := json.Unmarshal([]byte(`{"name":"search","annotations":{"readOnlyHint":true}}`), &tool); err != nil {
		t.Fatalf("decode tool: %v", err)
	}

	// When / Then
	if !tool.Annotations.IsReadOnly() {
		t.Fatal("readOnlyHint:true did not survive decoding")
	}
	if mcpEffect(tool) != spec.ToolEffectRead {
		t.Fatalf("effect = %q, want read", mcpEffect(tool))
	}
}
