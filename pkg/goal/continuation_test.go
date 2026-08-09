package goal

import (
	"bytes"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func continuationAdmissionFixture(t *testing.T) ContinuationAdmission {
	t.Helper()
	addr := protocol.MessageAddress{
		WorkspaceID: "project-a", ThreadID: "session-1", RequestID: "request-2",
	}
	part, err := protocol.NewTextPart("continue")
	if err != nil {
		t.Fatal(err)
	}
	return ContinuationAdmission{
		WorkspaceID: "project-a", SessionID: "session-1", GoalID: "goal-1",
		ParentTaskID: "parent-task", ParentMessageID: "assistant-1", Attempt: 2,
		Inbound: channel.Inbound{Addr: addr, Message: protocol.ChatMessage{
			ID: "continuation-2", Role: protocol.RoleUser, Addr: addr,
			Content: []protocol.ContentPart{part},
		}},
		Authority: tools.MemoryAuthority{GrantID: "secret-grant", SubjectType: "user", SubjectID: "alice"},
	}
}

func TestContinuationEnvelopeRoundTripsWithoutAuthority(t *testing.T) {
	admission := continuationAdmissionFixture(t)
	payload, err := MarshalContinuationEnvelope(admission)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("secret-grant")) || bytes.Contains(payload, []byte("alice")) {
		t.Fatalf("credential or subject leaked into payload: %s", payload)
	}
	envelope, err := ParseContinuationEnvelope(payload)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Schema != ContinuationEnvelopeSchema || envelope.GoalID != admission.GoalID ||
		envelope.Inbound.Message.ID != admission.Inbound.Message.ID || envelope.Attempt != admission.Attempt {
		t.Fatalf("round trip = %#v", envelope)
	}
	if _, err := ParseContinuationEnvelope(append(payload, []byte(` {}`)...)); err == nil {
		t.Fatal("trailing payload value was accepted")
	}
}

func TestContinuationAdmissionRejectsMismatchedWorkspaceAndPartialReceipt(t *testing.T) {
	admission := continuationAdmissionFixture(t)
	admission.Inbound.Message.Addr.WorkspaceID = "project-b"
	if err := admission.Validate(); err == nil {
		t.Fatal("workspace mismatch was accepted")
	}
	if err := (ContinuationReceipt{Backend: "aether"}).Validate(); err == nil {
		t.Fatal("receipt without task id was accepted")
	}
}
