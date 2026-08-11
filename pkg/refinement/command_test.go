package refinement

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type recordingAuditAuthorizer struct {
	addr   protocol.MessageAddress
	userID string
	err    error
	calls  int
}

func (a *recordingAuditAuthorizer) AuthorizeRefinementAudit(_ context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) error {
	a.calls++
	a.addr = addr
	a.userID = user.ID
	return a.err
}

func TestRefinementCommandRendersAttentionPageAndOpaqueContinuation(t *testing.T) {
	store := newTestStore(t)
	appendOutcome := func(operationID, refinementID string, outcome Outcome) {
		t.Helper()
		request := proposal("refinements/" + refinementID + "/application")
		request.RefinementID = refinementID
		request.Phase = PhaseApplication
		request.Outcome = outcome
		request.Summary = "repair\noperator output"
		request.Edits[0].ResourceKind = ResourceMemory
		request.Edits[0].Error = "authority conflict\nspoof"
		if _, err := store.Append(context.Background(), "project-a", operationID, request); err != nil {
			t.Fatal(err)
		}
	}
	appendOutcome("op-failed", "refine-failed", OutcomeFailed)
	appendOutcome("op-partial", "refine-partial", OutcomePartiallyApplied)
	authorizer := &recordingAuditAuthorizer{}
	service := &Service{Store: store, AuditAuthority: "MemoryLayer", AuditAuthorizer: authorizer}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "ops", TaskID: "command-task"}
	user := protocol.ChatMessage{ID: "command-user"}

	text, err := service.RunRefinementAuditCommand(context.Background(), addr, user, "--attention --resource memory --limit 1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"source: MemoryLayer authoritative", "phase=application outcome=partially_applied", "summary=\"repair operator output\"",
		"attention=required", "Next page: /refinements --attention --resource memory --limit 1 --cursor ",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("command output missing %q:\n%s", want, text)
		}
	}
	if authorizer.calls != 1 || authorizer.addr.WorkspaceID != "project-a" || authorizer.userID != "command-user" {
		t.Fatalf("authorizer = %+v", authorizer)
	}
}

func TestRefinementCommandKeepsSyntaxLocalAndAuthorizationFailuresClosed(t *testing.T) {
	service := &Service{Store: newTestStore(t)}
	text, err := service.RunRefinementAuditCommand(context.Background(), protocol.MessageAddress{WorkspaceID: "ws"}, protocol.ChatMessage{}, "--attention --outcome failed")
	if err != nil || !strings.Contains(text, refinementCommandUsage) || !strings.Contains(text, "cannot be combined") {
		t.Fatalf("syntax text=%q err=%v", text, err)
	}
	text, err = service.RunRefinementAuditCommand(context.Background(), protocol.MessageAddress{WorkspaceID: "ws"}, protocol.ChatMessage{}, "--help")
	if err != nil || !strings.Contains(text, "partially_applied") {
		t.Fatalf("help text=%q err=%v", text, err)
	}
	service.AuditAuthorizer = &recordingAuditAuthorizer{err: errors.New("denied")}
	if _, err := service.RunRefinementAuditCommand(context.Background(), protocol.MessageAddress{WorkspaceID: "ws"}, protocol.ChatMessage{}, ""); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("authorization error = %v", err)
	}
}
