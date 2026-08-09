package authhandoff

import (
	"encoding/json"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func Test_Store_put_resolve_is_single_use(t *testing.T) {
	s := New()
	auth := tools.MemoryAuthority{GrantID: "g1", SubjectType: "user", SubjectID: "u9"}
	tok := s.Put(auth)
	if tok == "" {
		t.Fatal("Put returned an empty token for a non-zero authority")
	}
	got, ok := s.Resolve(tok)
	if !ok || got != auth {
		t.Fatalf("Resolve = %+v (ok=%v), want %+v", got, ok, auth)
	}
	if _, again := s.Resolve(tok); again {
		t.Fatal("token must be single-use (second Resolve should miss)")
	}
}

func Test_Store_put_zero_authority_returns_empty_token(t *testing.T) {
	s := New()
	if tok := s.Put(tools.MemoryAuthority{}); tok != "" {
		t.Fatalf("zero authority should yield an empty token, got %q", tok)
	}
}

func Test_Store_resolve_unknown_token_misses(t *testing.T) {
	s := New()
	if _, ok := s.Resolve("nope"); ok {
		t.Fatal("unknown token must not resolve")
	}
	if _, ok := s.Resolve(""); ok {
		t.Fatal("empty token must not resolve")
	}
	var nilStore *Store
	if _, ok := nilStore.Resolve("x"); ok {
		t.Fatal("nil store must not resolve")
	}
	if tok := nilStore.Put(tools.MemoryAuthority{GrantID: "g"}); tok != "" {
		t.Fatalf("nil store Put should return empty, got %q", tok)
	}
}

func Test_Store_tokens_are_unique(t *testing.T) {
	s := New()
	a := s.Put(tools.MemoryAuthority{SubjectID: "u1"})
	b := s.Put(tools.MemoryAuthority{SubjectID: "u2"})
	if a == b {
		t.Fatal("distinct Put calls must mint distinct tokens")
	}
}

func Test_Store_resolve_message_consumes_opaque_handoff(t *testing.T) {
	s := New()
	auth := tools.MemoryAuthority{GrantID: "g1", SubjectType: "user", SubjectID: "u9"}
	token := s.Put(auth)
	raw, err := json.Marshal(map[string]string{"authority_handoff": token})
	if err != nil {
		t.Fatal(err)
	}
	message := protocol.ChatMessage{Meta: map[string]json.RawMessage{"scitrera": raw}}

	got, ok := s.ResolveMessage(message)
	if !ok || got != auth {
		t.Fatalf("ResolveMessage = %+v (ok=%v), want %+v", got, ok, auth)
	}
	if _, again := s.ResolveMessage(message); again {
		t.Fatal("message handoff must remain single-use")
	}
}

func Test_Store_resolve_message_rejects_malformed_metadata(t *testing.T) {
	s := New()
	for _, message := range []protocol.ChatMessage{
		{},
		{Meta: map[string]json.RawMessage{"scitrera": json.RawMessage(`{`)}},
		{Meta: map[string]json.RawMessage{"scitrera": json.RawMessage(`{"other":"value"}`)}},
	} {
		if _, ok := s.ResolveMessage(message); ok {
			t.Fatalf("malformed/absent handoff unexpectedly resolved: %+v", message.Meta)
		}
	}
}
