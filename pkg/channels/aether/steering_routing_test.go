// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"encoding/json"
	"testing"

	sdkmsg "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/steering"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

func routingChannel() *Channel {
	return &Channel{
		tasks:             make(chan channel.Inbound, 1),
		replyTo:           map[string]string{},
		executionBindings: map[string]spec.ExecutionBinding{},
		executionPolicies: map[string]ScheduledViewPolicy{},
		executionAccess:   map[string]workspacepkg.ExecutionBindingAuthorizationRequest{},
	}
}

// A steering send owns no task, so it must not write any task-keyed state:
// every write would land on the shared "" key, last-writer-wins. The symptom is
// a misroute, not a leak — replyTopic answers a task-less event (a steering
// rejection) on whichever client interjected most recently, instead of falling
// through to that event's own user session.
func Test_enqueueTurn_writes_no_task_keyed_state_for_a_steering_send(t *testing.T) {
	c := routingChannel()
	msg := steering.Mark(protocol.ChatMessage{
		ID: "steer-1", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{ThreadID: "t1", UserID: "user-a", RequestID: "win-a"},
	})

	if err := c.enqueueTurn(context.Background(), "us::user-a::win-a", msg, nil); err != nil {
		t.Fatalf("enqueueTurn: %v", err)
	}

	if len(c.replyTo) != 0 {
		t.Fatalf("replyTo = %#v, want no entry for a task-less send", c.replyTo)
	}
	// It still has to reach the runtime — the point is to park it, not drop it.
	select {
	case in := <-c.tasks:
		if in.Message.ID != "steer-1" {
			t.Fatalf("enqueued %q, want the steering send", in.Message.ID)
		}
	default:
		t.Fatal("steering send was not enqueued")
	}
}

func Test_enqueueTurn_still_records_reply_topic_for_a_real_turn(t *testing.T) {
	c := routingChannel()
	msg := protocol.ChatMessage{
		ID: "u1", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{ThreadID: "t1", TaskID: "task-1"},
	}

	if err := c.enqueueTurn(context.Background(), "us::user-a::win-a", msg, nil); err != nil {
		t.Fatalf("enqueueTurn: %v", err)
	}

	if c.replyTo["task-1"] != "us::user-a::win-a" {
		t.Fatalf("replyTo = %#v, want the turn's source topic", c.replyTo)
	}
}

// The routing consequence: with no "" entry, a task-less event resolves to the
// user session named on its own address.
func Test_replyTopic_routes_a_task_less_event_to_its_own_user_session(t *testing.T) {
	c := routingChannel()
	// Another client's steering send arrived first; under the old behavior it
	// would have claimed the shared "" key.
	if err := c.enqueueTurn(context.Background(), "us::other-user::other-window",
		steering.Mark(protocol.ChatMessage{
			ID: "steer-other", Role: protocol.RoleUser,
			Addr: protocol.MessageAddress{ThreadID: "t2", UserID: "other-user", RequestID: "other-window"},
		}), nil); err != nil {
		t.Fatalf("enqueueTurn: %v", err)
	}

	got := c.replyTopic(protocol.MessageAddress{ThreadID: "t1", UserID: "user-a", RequestID: "win-a"})

	if got == "us::other-user::other-window" {
		t.Fatal("a task-less event resolved to a different client's topic")
	}
	if got == "" {
		t.Fatal("a task-less event with a user session resolved to nothing")
	}
}

// A binding is meaningless on a turn that owns no task: the access receipt is
// correlated BY task id, and a steering send inherits the running turn's
// execution view anyway. Before this was explicit, the cross-host path failed
// closed only because ExecutionBindingAccessRequest errors on an empty
// correlation id — and the same-host path skipped the receipt entirely.
func Test_onMessage_rejects_an_execution_binding_with_no_task_identity(t *testing.T) {
	c := routingChannel()
	msg := steering.Mark(protocol.ChatMessage{
		ID: "steer-bound", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{ThreadID: "t1", WorkspaceID: "ws-1", UserID: "user-a", RequestID: "win-a"},
	})
	scope, err := workspacepkg.NewExecutionScope(
		spec.ExecutionBinding{
			SchemaVersion: spec.WorkspaceExecutionSchemaVersion,
			WorkspaceID:   "ws-1", ViewID: "view-1",
			ToolHostID: "us::user-a::win-a", ExecutionSite: spec.ExecutionSiteClient,
		},
		workspacepkg.ExecutionViewPolicy{WriteAccess: workspacepkg.ViewWriteAccessReadOnly},
	)
	if err != nil {
		t.Fatalf("build scope: %v", err)
	}
	if err := workspacepkg.PutExecutionScope(&msg, scope); err != nil {
		t.Fatalf("stamp scope: %v", err)
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// SourceTopic is left empty so rejectTurn short-circuits before publishing:
	// this fixture has no gateway client, and the assertion is about admission,
	// not about how the rejection is delivered.
	if err := c.onMessage(context.Background(), &sdkmsg.Message{Payload: payload}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}

	select {
	case in := <-c.tasks:
		t.Fatalf("admitted a bound turn with no task identity: %q", in.Message.ID)
	default:
	}
}
