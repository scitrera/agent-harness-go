package turn

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/contextpack"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type fakeAwaiter struct {
	decision approval.Decision
	err      error
}

func (f fakeAwaiter) Await(_ context.Context, _, _ string) (approval.Decision, error) {
	return f.decision, f.err
}

type fakeGranter struct {
	session []string
	always  []string
}

func (g *fakeGranter) GrantSession(ws, tool string) { g.session = append(g.session, ws+"/"+tool) }
func (g *fakeGranter) GrantAlways(_ context.Context, ws, tool string) error {
	g.always = append(g.always, ws+"/"+tool)
	return nil
}

// buildApprovalRunner wires a runner whose only tool ("record") is gated by a
// DynamicPolicy (not pre-authorized), with a scripted provider that calls it.
func buildApprovalRunner(t *testing.T, awaiter approval.Awaiter, granter ApprovalGranter, ran *bool) *Runner {
	t.Helper()
	policy := tools.NewDynamicPolicy(tools.StaticPolicy{Allowed: map[string]string{}}, nil)
	registry := tools.NewAuditedRegistry(policy, nil)
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		*ran = true
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register tool: %v", err)
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
		Store:           &fakeStore{},
		Loader:          fakeLoader{},
		Registry:        registry,
		Provider:        prov,
		Publisher:       &fakePublisher{},
		Assembler:       contextpack.NewAssembler(contextpack.Config{}),
		Approvals:       awaiter,
		ApprovalGranter: granter,
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	return runner
}

func approvalPartIn(msg protocol.ChatMessage) (spec.ApprovalRequestPart, bool) {
	for _, p := range msg.Content {
		if body, ok := p.AsApprovalRequest(); ok {
			return body, true
		}
	}
	return spec.ApprovalRequestPart{}, false
}

func userTurn(t *testing.T) protocol.ChatMessage {
	t.Helper()
	part, err := protocol.NewTextPart("hi")
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	return protocol.ChatMessage{ID: "u1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}}
}

func Test_Runner_approval_approve_runs_tool_grants_and_marks_approved(t *testing.T) {
	ran := false
	granter := &fakeGranter{}
	runner := buildApprovalRunner(t, fakeAwaiter{decision: approval.Decision{Granted: true, Scope: "session"}}, granter, &ran)

	addr := protocol.MessageAddress{ThreadID: "th1", TaskID: "task1", WorkspaceID: "ws1"}
	assistant, err := runner.Run(context.Background(), addr, userTurn(t))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !ran {
		t.Fatal("tool should have run after approval")
	}
	if len(granter.session) != 1 || granter.session[0] != "ws1/record" {
		t.Fatalf("expected session grant ws1/record, got %#v", granter.session)
	}
	part, ok := approvalPartIn(assistant)
	if !ok {
		t.Fatalf("no approval_request part in final message: %#v", assistant.Content)
	}
	if part.Status != spec.ApprovalApproved || part.Tool != "record" {
		t.Fatalf("approval part = %#v", part)
	}
}

func Test_Runner_approval_deny_skips_tool_and_marks_denied(t *testing.T) {
	ran := false
	runner := buildApprovalRunner(t, fakeAwaiter{decision: approval.Decision{Granted: false}}, &fakeGranter{}, &ran)

	addr := protocol.MessageAddress{ThreadID: "th1", TaskID: "task1", WorkspaceID: "ws1"}
	assistant, err := runner.Run(context.Background(), addr, userTurn(t))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if ran {
		t.Fatal("tool must NOT run when denied")
	}
	part, ok := approvalPartIn(assistant)
	if !ok {
		t.Fatalf("no approval_request part: %#v", assistant.Content)
	}
	if part.Status != spec.ApprovalDenied {
		t.Fatalf("expected denied, got %q", part.Status)
	}
}

// failAwaiter fails the test if its Await is ever consulted — proving the
// durable-grant slow-path short-circuited the prompt.
type failAwaiter struct{ t *testing.T }

func (f failAwaiter) Await(_ context.Context, _, _ string) (approval.Decision, error) {
	f.t.Fatal("approval awaiter must NOT be consulted when a durable grant exists")
	return approval.Decision{}, nil
}

// stubGrantStore reports IsGranted from a fixed set; Grant/ListGranted are
// inert (the slow-path durable check only calls IsGranted).
type stubGrantStore struct{ granted map[string]bool } // key: ws/tool

func (s stubGrantStore) ListGranted(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (s stubGrantStore) Grant(_ context.Context, _, _ string) error                { return nil }
func (s stubGrantStore) IsGranted(_ context.Context, ws, tool string) (bool, error) {
	return s.granted[ws+"/"+tool], nil
}

func Test_Runner_durable_grant_runs_tool_without_prompting(t *testing.T) {
	ran := false
	granter := &fakeGranter{}
	runner := buildApprovalRunner(t, failAwaiter{t: t}, granter, &ran)
	// Pre-existing durable "always" grant for ws1/record.
	runner.grantStore = stubGrantStore{granted: map[string]bool{"ws1/record": true}}

	addr := protocol.MessageAddress{ThreadID: "th1", TaskID: "task1", WorkspaceID: "ws1"}
	assistant, err := runner.Run(context.Background(), addr, userTurn(t))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !ran {
		t.Fatal("durably-granted tool should have run without prompting")
	}
	// A session grant is recorded so subsequent calls in-turn skip the gate too.
	if len(granter.session) != 1 || granter.session[0] != "ws1/record" {
		t.Fatalf("expected session grant ws1/record, got %#v", granter.session)
	}
	// No approval_request part should be emitted on the durable short-circuit.
	if part, ok := approvalPartIn(assistant); ok {
		t.Fatalf("no approval_request expected on durable grant, got %#v", part)
	}
}

// authCapturingStore records the MemoryAuthority present on the ctx passed to
// IsGranted, so a test can prove the runner puts the per-turn OBO on the ctx the
// durable grant store reads (it acts under the user's grant, not anonymously).
type authCapturingStore struct {
	mu   sync.Mutex
	seen tools.MemoryAuthority
	saw  bool
}

func (s *authCapturingStore) IsGranted(ctx context.Context, _, _ string) (bool, error) {
	a, _ := tools.MemoryAuthorityFrom(ctx)
	s.mu.Lock()
	s.seen, s.saw = a, true
	s.mu.Unlock()
	return false, nil // not granted → fall through to the (denying) prompt
}
func (s *authCapturingStore) Grant(context.Context, string, string) error           { return nil }
func (s *authCapturingStore) ListGranted(context.Context, string) ([]string, error) { return nil, nil }

func Test_Runner_grant_store_sees_turn_OBO_on_ctx(t *testing.T) {
	policy := tools.NewDynamicPolicy(tools.StaticPolicy{Allowed: map[string]string{}}, nil)
	registry := tools.NewAuditedRegistry(policy, nil)
	if err := registry.Register("record", tools.HandlerFunc(func(_ context.Context, req tools.Request) (tools.Result, error) {
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	callPart, _ := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "record", Args: protocol.RawToArgs(json.RawMessage(`{}`))})
	finalPart, _ := protocol.NewTextPart("done")
	prov := &scriptedProvider{responses: []provider.ChatResponse{
		{Message: protocol.ChatMessage{ID: "a-tool", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{callPart}}},
		{Message: protocol.ChatMessage{ID: "a-final", Role: protocol.RoleAssistant, Content: []protocol.ContentPart{finalPart}}},
	}}
	store := &authCapturingStore{}
	runner, err := NewRunner(Config{
		Store: &fakeStore{}, Loader: fakeLoader{}, Registry: registry, Provider: prov,
		Publisher: &fakePublisher{}, Assembler: contextpack.NewAssembler(contextpack.Config{}),
		Approvals:       fakeAwaiter{decision: approval.Decision{Granted: false}}, // deny → turn completes
		ApprovalGranter: policy,
		GrantStore:      store,
		Authority: func(protocol.MessageAddress, protocol.ChatMessage) tools.MemoryAuthority {
			return tools.MemoryAuthority{SubjectType: "user", SubjectID: "u1", GrantID: "g1"}
		},
	})
	if err != nil {
		t.Fatalf("runner: %v", err)
	}
	addr := protocol.MessageAddress{ThreadID: "th1", TaskID: "task1", WorkspaceID: "ws1"}
	if _, err := runner.Run(context.Background(), addr, userTurn(t)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !store.saw {
		t.Fatal("grant store IsGranted was never called")
	}
	if store.seen.SubjectType != "user" || store.seen.SubjectID != "u1" || store.seen.GrantID != "g1" {
		t.Fatalf("grant store did not see the turn OBO on ctx: %#v", store.seen)
	}
}
