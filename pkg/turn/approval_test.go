package turn

import (
	"context"
	"encoding/json"
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
