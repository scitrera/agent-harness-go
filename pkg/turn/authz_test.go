package turn

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

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
