// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"

	"github.com/scitrera/agent-harness-go/pkg/approval"
)

// fixedPolicy returns a preset decision regardless of the request.
type fixedPolicy struct{ code tools.DecisionCode }

func (p fixedPolicy) Decide(tools.Request) tools.Decision {
	return tools.Decision{Code: p.code, Reason: "test policy"}
}

// fixedAuthorizer returns a preset decision regardless of input.
type fixedAuthorizer struct{ out AuthzOutcome }

func (f fixedAuthorizer) Authorize(context.Context, AuthzInput) AuthzDecision {
	return AuthzDecision{Outcome: f.out}
}

// (a) resolver ordering / short-circuit: Deny wins, Prompt outranks Allow
// (order-independent), Abstain passes, none-decides → the Allow seed stands.
func Test_resolveAuthz_ordering(t *testing.T) {
	ctx := context.Background()
	in := AuthzInput{}
	allow := fixedAuthorizer{Allow}
	deny := fixedAuthorizer{Deny}
	prompt := fixedAuthorizer{Prompt}
	abstain := fixedAuthorizer{Abstain}

	cases := []struct {
		name  string
		seed  AuthzOutcome
		auths []ToolAuthorizer
		want  AuthzOutcome
	}{
		{"deny short-circuits", Allow, []ToolAuthorizer{allow, deny, prompt}, Deny},
		{"prompt outranks allow", Allow, []ToolAuthorizer{allow, prompt}, Prompt},
		{"prompt outranks allow (reordered)", Allow, []ToolAuthorizer{prompt, allow}, Prompt},
		{"abstain passes", Allow, []ToolAuthorizer{abstain, allow}, Allow},
		{"none decides keeps seed", Allow, nil, Allow},
		{"allow authorizer over abstain seed", Abstain, []ToolAuthorizer{allow}, Allow},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveAuthz(ctx, in, c.seed, c.auths...); got.Outcome != c.want {
				t.Fatalf("resolveAuthz = %v, want %v", got.Outcome, c.want)
			}
		})
	}
}

// (b) a pre-authorized tool is Allowed by the grant authorizer, but a Deny-ing
// safety authorizer still blocks it — proving safety runs for pre-authorized.
func Test_authorizeTool_safety_runs_for_preauthorized(t *testing.T) {
	ctx := context.Background()
	in := AuthzInput{
		Call:  protocol.ToolInvokeEnvelope{CallID: "c1", Name: "preauth"},
		Trust: tools.TrustPreAuthorized,
	}

	// Grant alone (noop safety) → Allow.
	allowed := (&Runner{}).authorizeTool(ctx, in, trustBase(in.Trust))
	if allowed.Outcome != Allow {
		t.Fatalf("pre-authorized without safety = %v, want Allow", allowed.Outcome)
	}

	// Deny-ing safety still blocks the pre-authorized tool.
	r := &Runner{safetyAuthorizer: fixedAuthorizer{Deny}}
	if got := r.authorizeTool(ctx, in, trustBase(in.Trust)); got.Outcome != Deny {
		t.Fatalf("pre-authorized with denying safety = %v, want Deny", got.Outcome)
	}
}

func Test_authorizeTool_freshApprovalIgnoresDurableGrant(t *testing.T) {
	awaiter := &recordingAwaiter{}
	granter := &fakeGranter{}
	runner := &Runner{
		approvals: awaiter, approvalGranter: granter,
		grantStore: stubGrantStore{granted: map[string]bool{"ws1/apply_refinement": true}},
	}
	in := AuthzInput{
		Call: protocol.ToolInvokeEnvelope{CallID: "apply-1", Name: "apply_refinement"},
		Addr: protocol.MessageAddress{WorkspaceID: "ws1"}, Trust: tools.TrustRequiresFreshApproval,
	}
	decision := runner.authorizeTool(context.Background(), in, trustBase(in.Trust))
	if decision.Outcome != Allow || !awaiter.consulted {
		t.Fatalf("fresh approval decision = %+v, consulted=%t", decision, awaiter.consulted)
	}
	if len(granter.session) != 0 || len(granter.always) != 0 {
		t.Fatalf("fresh approval must not create reusable grants: %+v", granter)
	}
}

// recordingAwaiter grants and records that it was consulted.
type recordingAwaiter struct{ consulted bool }

func (a *recordingAwaiter) Await(context.Context, string, string) (approval.Decision, error) {
	a.consulted = true
	return approval.Decision{Granted: true, Scope: "session"}, nil
}

// (c) a provider tool whose safety authorizer escalates Allow→Prompt drives the
// interactive path: the awaiter is consulted and, on grant, the provider runs.
func Test_authorizeTool_provider_safety_escalation_prompts(t *testing.T) {
	ctx := context.Background()
	awaiter := &recordingAwaiter{}
	granter := &fakeGranter{}
	p := &fakeToolProvider{id: "p1"}
	r := &Runner{
		approvals:        awaiter,
		approvalGranter:  granter,
		safetyAuthorizer: fixedAuthorizer{Prompt}, // escalate the TrustDefault Allow
	}
	addr := protocol.MessageAddress{WorkspaceID: "ws1", TaskID: "task1"}
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "remote_x"}

	res, err := r.invokeToolProvider(ctx, p, addr, call, tools.TrustDefault)
	if err != nil {
		t.Fatalf("invokeToolProvider: %v", err)
	}
	if !awaiter.consulted {
		t.Fatal("safety escalation should have driven the interactive prompt")
	}
	if len(p.invoked) != 1 || p.invoked[0].Name != "remote_x" {
		t.Fatalf("provider should have run after approval, got %+v", p.invoked)
	}
	if res.Name != "remote_x" {
		t.Fatalf("result name = %q, want remote_x", res.Name)
	}
	if len(granter.session) != 1 || granter.session[0] != "ws1/remote_x" {
		t.Fatalf("expected session grant ws1/remote_x, got %#v", granter.session)
	}
}

// (d) a default-trust provider tool is gated by ToolPolicy: a Deny blocks it
// (provider not invoked, requires-approval error fed back), a RequiresApproval
// drives the interactive prompt (provider runs on grant).
func Test_authorizeToolProvider_policy_gates_default_trust(t *testing.T) {
	ctx := context.Background()
	addr := protocol.MessageAddress{WorkspaceID: "ws1", TaskID: "task1"}
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "remote_x"}

	t.Run("deny blocks before invoke", func(t *testing.T) {
		p := &fakeToolProvider{id: "p1"}
		r := &Runner{toolPolicy: fixedPolicy{tools.DecisionDeny}}
		if _, err := r.invokeToolProvider(ctx, p, addr, call, tools.TrustDefault); err == nil {
			t.Fatal("policy deny should error")
		}
		if len(p.invoked) != 0 {
			t.Fatalf("provider must not run when policy denies, got %+v", p.invoked)
		}
	})

	t.Run("requires_approval prompts then runs on grant", func(t *testing.T) {
		p := &fakeToolProvider{id: "p1"}
		awaiter := &recordingAwaiter{}
		r := &Runner{
			toolPolicy:      fixedPolicy{tools.DecisionRequiresApproval},
			approvals:       awaiter,
			approvalGranter: &fakeGranter{},
		}
		if _, err := r.invokeToolProvider(ctx, p, addr, call, tools.TrustDefault); err != nil {
			t.Fatalf("invokeToolProvider: %v", err)
		}
		if !awaiter.consulted {
			t.Fatal("policy requires_approval should have driven the interactive prompt")
		}
		if len(p.invoked) != 1 {
			t.Fatalf("provider should run after approval, got %+v", p.invoked)
		}
	})
}

// (e) a pre-authorized provider tool SKIPS the policy (grant short-circuit ahead
// of policy), so a Deny-ing policy still lets it run — but a Deny-ing safety,
// which runs after the grant for every tool, still blocks it.
func Test_authorizeToolProvider_grant_skips_policy(t *testing.T) {
	ctx := context.Background()
	addr := protocol.MessageAddress{WorkspaceID: "ws1", TaskID: "task1"}
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "remote_x"}

	t.Run("grant beats denying policy", func(t *testing.T) {
		p := &fakeToolProvider{id: "p1"}
		r := &Runner{toolPolicy: fixedPolicy{tools.DecisionDeny}}
		if _, err := r.invokeToolProvider(ctx, p, addr, call, tools.TrustPreAuthorized); err != nil {
			t.Fatalf("pre-authorized should skip policy: %v", err)
		}
		if len(p.invoked) != 1 {
			t.Fatalf("pre-authorized provider should run, got %+v", p.invoked)
		}
	})

	t.Run("denying safety still blocks a granted tool", func(t *testing.T) {
		p := &fakeToolProvider{id: "p1"}
		r := &Runner{
			toolPolicy:       fixedPolicy{tools.DecisionDeny},
			safetyAuthorizer: fixedAuthorizer{Deny},
		}
		if _, err := r.invokeToolProvider(ctx, p, addr, call, tools.TrustPreAuthorized); err == nil {
			t.Fatal("denying safety should block even a granted tool")
		}
		if len(p.invoked) != 0 {
			t.Fatalf("provider must not run when safety denies, got %+v", p.invoked)
		}
	})
}

// (f) with a nil ToolPolicy and the default no-op safety, a default-trust
// provider tool resolves to Allow and runs directly (behavior unchanged).
func Test_authorizeToolProvider_nil_policy_allows(t *testing.T) {
	ctx := context.Background()
	addr := protocol.MessageAddress{WorkspaceID: "ws1", TaskID: "task1"}
	call := protocol.ToolInvokeEnvelope{CallID: "c1", Name: "remote_x"}
	p := &fakeToolProvider{id: "p1"}
	r := &Runner{}
	if _, err := r.invokeToolProvider(ctx, p, addr, call, tools.TrustDefault); err != nil {
		t.Fatalf("nil policy + noop safety should allow: %v", err)
	}
	if len(p.invoked) != 1 {
		t.Fatalf("provider should run, got %+v", p.invoked)
	}
}

// (g) a local, outright-allowed tool with a Deny-ing safety authorizer is blocked
// BEFORE its body executes (pre-execution safety), and the turn still completes
// (the requires-approval-style error is fed back to the model).
func Test_invokeWithApproval_preexec_safety_blocks_local(t *testing.T) {
	ran := false
	// StaticPolicy allows "record" outright — without pre-exec safety it would run.
	policy := tools.NewDynamicPolicy(tools.StaticPolicy{Allowed: map[string]string{"record": "ok"}}, nil)
	registry := tools.NewAuditedRegistry(policy, nil)
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		ran = true
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	callPart, err := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
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
	runner, err := NewRunner(Config{
		Store:            &fakeStore{},
		Loader:           fakeLoader{},
		Registry:         registry,
		Provider:         prov,
		Publisher:        &fakePublisher{},
		Assembler:        contextpack.NewAssembler(contextpack.Config{}),
		SafetyAuthorizer: fixedAuthorizer{Deny},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	addr := protocol.MessageAddress{ThreadID: "th1", TaskID: "task1", WorkspaceID: "ws1"}
	if _, err := runner.Run(context.Background(), addr, userTurn(t)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if ran {
		t.Fatal("local tool must NOT execute when pre-exec safety denies")
	}
}
