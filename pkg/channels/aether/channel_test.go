package aether

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/aetherwire"
	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// newTestChannel builds a channel with the egress seam captured. New() does not
// dial, so no gateway is needed.
func newTestChannel(t *testing.T) (*Channel, *[]sentMessage) {
	t.Helper()
	c, err := New(Config{ServerAddr: "127.0.0.1:1", Workspace: "default", Specifier: "test"})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	sent := &[]sentMessage{}
	c.sendMessage = func(topic string, payload []byte) error {
		*sent = append(*sent, sentMessage{Topic: topic, Payload: payload})
		return nil
	}
	return c, sent
}

type sentMessage struct {
	Topic   string
	Payload []byte
}

type testWorkspaceResolver struct{}

func (testWorkspaceResolver) ResolveWorkspace(_ context.Context, requested string) (string, error) {
	if requested == "" || requested == "project-a" {
		return "project-a", nil
	}
	return "", context.Canceled
}

// testMessage wraps a payload as it would arrive from the gateway.
func testMessage(payload []byte) *sdk.Message {
	return &sdk.Message{Payload: payload}
}

func userTurn(t *testing.T, addr protocol.MessageAddress, text string) []byte {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	body, err := json.Marshal(protocol.ChatMessage{
		ID:      "user-1",
		Role:    protocol.RoleUser,
		Addr:    addr,
		Content: []protocol.ContentPart{part},
	})
	if err != nil {
		t.Fatalf("marshal turn: %v", err)
	}
	return body
}

// controlPayload builds a control message the way it actually arrives on the
// wire — raw JSON — since the spec's exported constructors cannot express a
// control part carrying request_id + scope.
func controlPayload(t *testing.T, addr protocol.MessageAddress, part map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id":      "ctrl-1",
		"role":    "user",
		"addr":    addr,
		"content": []any{part},
	})
	if err != nil {
		t.Fatalf("marshal control: %v", err)
	}
	return body
}

// A turn arriving with no task id is normal in oss (frontends need not mint
// Aether tasks). It must still be accepted, with an id minted locally so cancel
// and approval correlation have something to key on.
func TestOnMessageMintsTaskIDForTaskLessTurn(t *testing.T) {
	c, _ := newTestChannel(t)
	ctx := context.Background()

	payload := userTurn(t, protocol.MessageAddress{ThreadID: "thread-1"}, "hello")
	if err := c.onMessage(ctx, &sdk.Message{Payload: payload, SourceTopic: "us::drew::w1"}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}

	in, err := c.FetchTask(ctx)
	if err != nil {
		t.Fatalf("FetchTask: %v", err)
	}
	if in.Addr.TaskID == "" {
		t.Fatal("task-less turn was not given a task id")
	}
	if in.Message.Addr.TaskID != in.Addr.TaskID {
		t.Fatalf("message addr task %q != inbound addr task %q", in.Message.Addr.TaskID, in.Addr.TaskID)
	}
}

func TestOnMessageAppliesWorkspaceVisibility(t *testing.T) {
	c, err := New(Config{
		ServerAddr: "127.0.0.1:1", Workspace: "routing", Specifier: "test",
		SessionWorkspace: "project-a", WorkspaceResolver: testWorkspaceResolver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.onMessage(context.Background(), &sdk.Message{
		Payload: userTurn(t, protocol.MessageAddress{ThreadID: "thread-1"}, "hi"),
	}); err != nil {
		t.Fatal(err)
	}
	in, err := c.FetchTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if in.Addr.WorkspaceID != "project-a" || in.Message.Addr.WorkspaceID != "project-a" {
		t.Fatalf("resolved inbound = %+v", in)
	}
	if err := c.onMessage(context.Background(), &sdk.Message{
		Payload: userTurn(t, protocol.MessageAddress{WorkspaceID: "routing", ThreadID: "thread-old"}, "old client"),
	}); err != nil {
		t.Fatal(err)
	}
	legacy, err := c.FetchTask(context.Background())
	if err != nil || legacy.Addr.WorkspaceID != "project-a" {
		t.Fatalf("legacy transport alias = %+v, %v", legacy, err)
	}
	if err := c.onMessage(context.Background(), &sdk.Message{
		Payload: userTurn(t, protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "thread-2"}, "no"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case rejected := <-c.tasks:
		t.Fatalf("unlisted workspace was enqueued: %+v", rejected)
	default:
	}
}

// Stream events go back to the topic the turn arrived from — "reply to where it
// came from" — not to a task lane.
func TestPublishEventRepliesToOriginatingTopic(t *testing.T) {
	c, sent := newTestChannel(t)
	ctx := context.Background()

	addr := protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1", WorkspaceID: "default"}
	payload := userTurn(t, addr, "hello")
	if err := c.onMessage(ctx, &sdk.Message{Payload: payload, SourceTopic: "us::drew::w1"}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}
	if _, err := c.FetchTask(ctx); err != nil {
		t.Fatalf("FetchTask: %v", err)
	}

	err := c.PublishEvent(ctx, channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      addr,
		MessageID: "assistant-1",
		Delta:     "hi",
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(*sent))
	}
	if got := (*sent)[0].Topic; got != "us::drew::w1" {
		t.Fatalf("reply topic = %q, want us::drew::w1", got)
	}
	var envelope map[string]any
	if err := json.Unmarshal((*sent)[0].Payload, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	options, ok := envelope["options"].(map[string]any)
	if !ok {
		t.Fatalf("envelope has no options: %v", envelope)
	}
	if options["thread_id"] != "thread-1" {
		t.Fatalf("envelope thread_id = %v, want thread-1", options["thread_id"])
	}
	if _, ok := options["chat_stream_event"]; !ok {
		t.Fatal("envelope carries no chat_stream_event")
	}
}

func TestPublishEventCanUseRealAetherTaskLane(t *testing.T) {
	c, err := New(Config{
		ServerAddr:             "127.0.0.1:1",
		Workspace:              "aether-routing",
		SessionWorkspace:       "project-a",
		Specifier:              "test",
		PreferTaskMessageLanes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var sent []sentMessage
	c.sendMessage = func(topic string, payload []byte) error {
		sent = append(sent, sentMessage{Topic: topic, Payload: payload})
		return nil
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "thread-1", TaskID: "task-real"}
	if err := c.PublishEvent(context.Background(), channel.Event{
		Type: channel.EventTokenDelta, Addr: addr, MessageID: "m1", Index: 0, Delta: "hi",
	}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].Topic != aetherwire.TaskMessageTopic("project-a", "task-real") {
		t.Fatalf("task-lane sends = %+v", sent)
	}
}

// With no recorded originator and no user in the address there is nowhere to
// send: the event is dropped rather than misrouted.
func TestPublishEventWithoutReplyTargetIsDropped(t *testing.T) {
	c, sent := newTestChannel(t)

	err := c.PublishEvent(context.Background(), channel.Event{
		Type:      channel.EventTokenDelta,
		Addr:      protocol.MessageAddress{ThreadID: "thread-1", TaskID: "unknown"},
		MessageID: "assistant-1",
		Delta:     "hi",
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("sent %d messages, want 0", len(*sent))
	}
}

// The reply target is released when the turn finalizes, so a long-lived worker
// does not accumulate one entry per turn.
func TestReplyTargetReleasedOnFinal(t *testing.T) {
	c, _ := newTestChannel(t)
	ctx := context.Background()

	addr := protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1", WorkspaceID: "default"}
	if err := c.onMessage(ctx, &sdk.Message{Payload: userTurn(t, addr, "hi"), SourceTopic: "us::drew::w1"}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}
	if _, err := c.FetchTask(ctx); err != nil {
		t.Fatalf("FetchTask: %v", err)
	}
	final := protocol.ChatMessage{ID: "assistant-1", Role: protocol.RoleAssistant, Addr: addr}
	if err := c.PublishEvent(ctx, channel.Event{Type: channel.EventMessageFinal, Addr: addr, Message: &final}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	c.mu.Lock()
	remaining := len(c.replyTo)
	c.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("replyTo still holds %d entries after finalize", remaining)
	}
}

// A control message is applied, not delivered as a turn.
func TestControlCancelIsNotDeliveredAsATurn(t *testing.T) {
	c, _ := newTestChannel(t)
	tc := turncancel.New()
	c.SetCanceller(tc)
	ctx := context.Background()

	turnCtx, release := tc.Begin(context.Background(), "task-9")
	defer release()

	body := controlPayload(t, protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-9"},
		map[string]any{"type": "control", "kind": spec.ControlCancel, "task_id": "task-9"})
	if err := c.onMessage(ctx, &sdk.Message{Payload: body, SourceTopic: "us::drew::w1"}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}

	select {
	case <-turnCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("cancel control did not cancel the in-flight turn")
	}
	select {
	case in := <-c.tasks:
		t.Fatalf("control was delivered as a turn: %+v", in)
	default:
	}
}

func TestControlClearCarriesCompositeWorkspaceThreadAddress(t *testing.T) {
	c, _ := newTestChannel(t)
	want := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "shared", TaskID: "task-1"}
	cleared := make(chan protocol.MessageAddress, 1)
	c.SetThreadClearer(func(addr protocol.MessageAddress) error {
		cleared <- addr
		return nil
	})

	body := controlPayload(t, want, map[string]any{"type": "control", "kind": spec.ControlClear})
	if err := c.onMessage(context.Background(), &sdk.Message{Payload: body}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}
	select {
	case got := <-cleared:
		if got.WorkspaceID != want.WorkspaceID || got.ThreadID != want.ThreadID {
			t.Fatalf("clear address = %+v, want %+v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("clear control was not applied")
	}
	select {
	case in := <-c.tasks:
		t.Fatalf("clear control was delivered as a turn: %+v", in)
	default:
	}
}

// Approve/deny controls resolve the turn's pending approval prompt.
func TestControlApproveResolvesPendingApproval(t *testing.T) {
	c, _ := newTestChannel(t)
	broker := approval.New()
	c.SetApprovalBroker(broker)

	decided := make(chan approval.Decision, 1)
	go func() {
		d, err := broker.Await(context.Background(), "task-5", "req-1")
		if err == nil {
			decided <- d
		}
	}()
	// Let the awaiter register before resolving.
	time.Sleep(50 * time.Millisecond)

	body := controlPayload(t, protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-5"},
		map[string]any{"type": "control", "kind": spec.ControlApprove, "request_id": "req-1", "scope": "once"})
	if err := c.onMessage(context.Background(), &sdk.Message{Payload: body}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}

	select {
	case d := <-decided:
		if !d.Granted || d.Scope != "once" {
			t.Fatalf("decision = %+v, want granted once", d)
		}
	case <-time.After(time.Second):
		t.Fatal("approve control did not resolve the pending approval")
	}
}

// Non-wire events (tool lifecycle, errors) are not published: tool activity
// reaches clients as part_appended.
func TestNonWireEventsAreNotPublished(t *testing.T) {
	c, sent := newTestChannel(t)
	ctx := context.Background()
	addr := protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"}
	if err := c.onMessage(ctx, &sdk.Message{Payload: userTurn(t, addr, "hi"), SourceTopic: "us::drew::w1"}); err != nil {
		t.Fatalf("onMessage: %v", err)
	}
	if _, err := c.FetchTask(ctx); err != nil {
		t.Fatalf("FetchTask: %v", err)
	}
	for _, kind := range []channel.EventType{channel.EventToolLifecycle, channel.EventError, channel.EventToolResult} {
		if err := c.PublishEvent(ctx, channel.Event{Type: kind, Addr: addr}); err != nil {
			t.Fatalf("PublishEvent(%s): %v", kind, err)
		}
	}
	if len(*sent) != 0 {
		t.Fatalf("sent %d messages for non-wire events, want 0", len(*sent))
	}
}

type blockingSessionService struct {
	mu      sync.Mutex
	request spec.SessionAttachRequest
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSessionService) Attach(_ context.Context, request spec.SessionAttachRequest) (spec.SessionAttachResult, error) {
	s.mu.Lock()
	s.request = request
	s.mu.Unlock()
	if s.entered != nil {
		close(s.entered)
	}
	if s.release != nil {
		<-s.release
	}
	return spec.SessionAttachResult{
		ProtocolVersion: spec.SessionProtocolVersion,
		SchemaRevision:  request.SchemaRevision,
		WorkspaceID:     "project-a",
		SessionID:       request.SessionID,
		Capabilities:    spec.NormalizeSessionCapabilities(request.Capabilities),
		Snapshot: spec.SessionSnapshot{
			WorkspaceID: "project-a",
			SessionID:   request.SessionID,
			Cursor:      spec.SessionCursor{Generation: "worker-a", Sequence: 0},
			Messages:    []spec.ChatMessage{},
			State:       map[string]json.RawMessage{},
		},
	}, nil
}

func sessionAttachPayload(t *testing.T, requestID string, request spec.SessionAttachRequest) []byte {
	t.Helper()
	frame, err := spec.NewSessionFrame(spec.SessionFrameAttachRequest, requestID, request)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := aetherwire.SessionFramePayload(frame)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestSessionAttachRepliesBeforeBufferedLiveEvents(t *testing.T) {
	c, _ := newTestChannel(t)
	sent := make(chan sentMessage, 2)
	c.sendMessage = func(topic string, payload []byte) error {
		sent <- sentMessage{Topic: topic, Payload: payload}
		return nil
	}
	service := &blockingSessionService{entered: make(chan struct{}), release: make(chan struct{})}
	c.SetSessionService(service)
	request := spec.NewSessionAttachRequest("thread-1", "client-1")
	request.WorkspaceID = "project-a"
	attachPayload := sessionAttachPayload(t, "attach-1", request)

	returned := make(chan error, 1)
	go func() {
		returned <- c.onMessage(context.Background(), &sdk.Message{
			Payload:     attachPayload,
			SourceTopic: "us::drew::w1",
		})
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("session frame blocked the SDK receive callback")
	}
	<-service.entered
	event := spec.SessionEvent{
		ProtocolVersion: spec.SessionProtocolVersion,
		SchemaRevision:  spec.SessionSchemaRevision,
		WorkspaceID:     "project-a",
		SessionID:       "thread-1",
		Cursor:          spec.SessionCursor{Generation: "worker-a", Sequence: 1},
		Kind:            spec.SessionEventChatStream,
		Payload:         json.RawMessage(`{"event":"token_delta","message_id":"m1","index":0,"text":"hi"}`),
	}
	if err := c.PublishSessionEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	close(service.release)
	var frames []sentMessage
	for len(frames) < 2 {
		select {
		case frame := <-sent:
			frames = append(frames, frame)
		case <-time.After(time.Second):
			t.Fatalf("received %d session frames, want result + event", len(frames))
		}
	}
	first, ok, err := aetherwire.ParseSessionFrame(frames[0].Payload)
	if err != nil || !ok || first.Type != spec.SessionFrameAttachResult {
		t.Fatalf("first frame = %+v, %v, %v", first, ok, err)
	}
	second, ok, err := aetherwire.ParseSessionFrame(frames[1].Payload)
	if err != nil || !ok || second.Type != spec.SessionFrameEvent {
		t.Fatalf("second frame = %+v, %v, %v", second, ok, err)
	}
	select {
	case inbound := <-c.tasks:
		t.Fatalf("attach was delivered as a turn: %+v", inbound)
	default:
	}
}
