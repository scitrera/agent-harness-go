package steering

import (
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"testing"
)

func TestAddressKeyIsolatesUsersAndSharedConversationWriters(t *testing.T) {
	alice := protocol.MessageAddress{TenantID: "alpha", UserID: "alice", WorkspaceID: "same", ThreadID: "same"}
	bob := alice
	bob.UserID = "bob"
	inbox := New()
	defer inbox.Begin(AddressKey(alice))()
	if inbox.Park(AddressKey(bob), protocol.ChatMessage{ID: "bob-private"}) {
		t.Fatal("bob steered alice's turn")
	}
	if !inbox.Park(AddressKey(alice), protocol.ChatMessage{ID: "alice"}) {
		t.Fatal("own steering lost")
	}
	if AddressKey(alice) == AddressKey(bob) || ConversationKey(alice) == ConversationKey(bob) {
		t.Fatal("private conversation collision")
	}
	alice.Ownership = "workspace"
	bob.Ownership = "workspace"
	if AddressKey(alice) == AddressKey(bob) {
		t.Fatal("cross-user steering in shared conversation")
	}
	if ConversationKey(alice) != ConversationKey(bob) {
		t.Fatal("shared transcript writers must serialize")
	}
	beta := alice
	beta.TenantID = "beta"
	if ConversationKey(alice) == ConversationKey(beta) {
		t.Fatal("cross-tenant collision")
	}
}
