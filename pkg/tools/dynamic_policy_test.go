package tools

import (
	"context"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func req(ws, tool string) Request {
	return Request{Name: tool, Addr: protocol.MessageAddress{WorkspaceID: ws}}
}

func Test_DynamicPolicy_requires_approval_then_session_grant_allows(t *testing.T) {
	p := NewDynamicPolicy(StaticPolicy{Allowed: map[string]string{"read_file": "ok"}}, nil)

	// base-allowed tool passes
	if d := p.Decide(req("ws1", "read_file")); !d.Allowed() {
		t.Fatalf("read_file should be allowed: %#v", d)
	}
	// ungated tool requires approval
	if d := p.Decide(req("ws1", "shell")); d.Code != DecisionRequiresApproval {
		t.Fatalf("shell should require approval: %#v", d)
	}
	// after a session grant it's allowed — but only in that workspace
	p.GrantSession("ws1", "shell")
	if d := p.Decide(req("ws1", "shell")); !d.Allowed() {
		t.Fatalf("shell should be allowed after session grant: %#v", d)
	}
	if d := p.Decide(req("ws2", "shell")); d.Code != DecisionRequiresApproval {
		t.Fatalf("grant must be workspace-scoped: %#v", d)
	}
}

type memGrantStore struct{ granted map[string][]string }

func (s *memGrantStore) ListGranted(_ context.Context, ws string) ([]string, error) {
	return s.granted[ws], nil
}
func (s *memGrantStore) Grant(_ context.Context, ws, tool string) error {
	if s.granted == nil {
		s.granted = map[string][]string{}
	}
	s.granted[ws] = append(s.granted[ws], tool)
	return nil
}

func Test_DynamicPolicy_always_grant_persists_and_hydrates(t *testing.T) {
	store := &memGrantStore{}
	p := NewDynamicPolicy(StaticPolicy{Allowed: map[string]string{}}, store)

	if err := p.GrantAlways(context.Background(), "ws1", "shell"); err != nil {
		t.Fatalf("grant always: %v", err)
	}
	if len(store.granted["ws1"]) != 1 || store.granted["ws1"][0] != "shell" {
		t.Fatalf("store did not persist: %#v", store.granted)
	}

	// A fresh policy hydrates the durable grant from the store.
	p2 := NewDynamicPolicy(StaticPolicy{Allowed: map[string]string{}}, store)
	if d := p2.Decide(req("ws1", "shell")); d.Code != DecisionRequiresApproval {
		t.Fatalf("pre-hydrate should require approval: %#v", d)
	}
	if err := p2.Hydrate(context.Background(), "ws1"); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if d := p2.Decide(req("ws1", "shell")); !d.Allowed() {
		t.Fatalf("post-hydrate should allow: %#v", d)
	}
}
