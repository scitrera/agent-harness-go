package aetherwire

import (
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestEphemeral_ReadsNestedScitreraFlag(t *testing.T) {
	msg := spec.NewChatMessage("m1", spec.RoleUser)
	msg.Meta = map[string]json.RawMessage{
		"scitrera": json.RawMessage(`{"ephemeral":true,"authority_grant_id":"grant-1"}`),
	}
	if !Ephemeral(msg) {
		t.Fatal("expected ephemeral=true")
	}
	// The flag must not disturb the sibling grant read.
	if got := AuthorityGrantID(msg); got != "grant-1" {
		t.Fatalf("authority_grant_id = %q, want grant-1", got)
	}
}

func TestEphemeral_DefaultsFalse(t *testing.T) {
	// No meta, empty scitrera, and explicit false all read false.
	if Ephemeral(spec.NewChatMessage("m1", spec.RoleUser)) {
		t.Fatal("no meta should be non-ephemeral")
	}
	msg := spec.NewChatMessage("m2", spec.RoleUser)
	msg.Meta = map[string]json.RawMessage{"scitrera": json.RawMessage(`{"agent_name":"Sam"}`)}
	if Ephemeral(msg) {
		t.Fatal("scitrera without ephemeral should be non-ephemeral")
	}
	msg2 := spec.NewChatMessage("m3", spec.RoleUser)
	msg2.Meta = map[string]json.RawMessage{"scitrera": json.RawMessage(`{"ephemeral":false}`)}
	if Ephemeral(msg2) {
		t.Fatal("ephemeral:false should be non-ephemeral")
	}
}

func TestEphemeral_TopLevelFallback(t *testing.T) {
	msg := spec.NewChatMessage("m1", spec.RoleUser)
	msg.Meta = map[string]json.RawMessage{"ephemeral": json.RawMessage(`true`)}
	if !Ephemeral(msg) {
		t.Fatal("expected top-level ephemeral fallback to read true")
	}
}
