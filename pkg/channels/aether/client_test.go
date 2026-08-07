package aether

import (
	"context"
	"encoding/json"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/aetherwire"
	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func newTestClient(t *testing.T) (*Client, *[][]byte) {
	t.Helper()
	c, err := NewClient(ClientConfig{
		ServerAddr:     "127.0.0.1:1",
		Workspace:      "default",
		AgentSpecifier: "test",
		UserID:         "drew",
		WindowID:       "w1",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	sent := &[][]byte{}
	c.sendToAgent = func(payload []byte) error {
		*sent = append(*sent, payload)
		return nil
	}
	return c, sent
}

// memProjection is an in-memory stand-in for the UI's history store.
type memProjection struct {
	threads map[string][]protocol.ChatMessage
}

func newMemProjection() *memProjection {
	return &memProjection{threads: map[string][]protocol.ChatMessage{}}
}

func (m *memProjection) LoadHistory(_ context.Context, threadID string) ([]protocol.ChatMessage, error) {
	return m.threads[threadID], nil
}

func (m *memProjection) SaveHistory(_ context.Context, threadID string, msgs []protocol.ChatMessage) error {
	m.threads[threadID] = append([]protocol.ChatMessage(nil), msgs...)
	return nil
}

// The agent needs to know who to answer, so the client stamps its own session
// identity onto a turn that does not carry one.
func TestEnqueueStampsSessionIdentity(t *testing.T) {
	c, sent := newTestClient(t)
	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	addr := protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"}
	err = c.Enqueue(context.Background(), channel.Inbound{
		Addr:    addr,
		Message: protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent %d payloads, want 1", len(*sent))
	}
	var msg protocol.ChatMessage
	if err := json.Unmarshal((*sent)[0], &msg); err != nil {
		t.Fatalf("decode sent turn: %v", err)
	}
	if msg.Addr.UserID != "drew" || msg.Addr.RequestID != "w1" {
		t.Fatalf("identity = %+v, want user drew window w1", msg.Addr)
	}
	if msg.Addr.WorkspaceID != "default" {
		t.Fatalf("workspace = %q, want default", msg.Addr.WorkspaceID)
	}
	if msg.Addr.ThreadID != "thread-1" || msg.Addr.TaskID != "task-1" {
		t.Fatalf("addr = %+v, want thread-1/task-1 preserved", msg.Addr)
	}
}

// Round-trip: what the agent publishes is what the client decodes. A token
// delta carries no address of its own, so the client must attribute it to the
// thread and task it submitted — otherwise the UI files it as background
// activity for some other thread and the reply never appears.
func TestOnMessageDecodesAndAttributesTokenDelta(t *testing.T) {
	c, _ := newTestClient(t)
	part, err := protocol.NewTextPart("hi")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	if err := c.Enqueue(context.Background(), channel.Inbound{
		Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Message: protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}},
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	payload, err := aetherwire.StreamEnvelope(
		aetherwire.TurnMeta{AppWorkspace: "default", TaskID: "task-1", ThreadID: "thread-1", WindowID: "w1"},
		spec.TokenDeltaEvent{MessageID: "assistant-1", Index: 0, Text: "hello"},
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	if err := c.onMessage(context.Background(), testMessage(payload)); err != nil {
		t.Fatalf("onMessage: %v", err)
	}

	select {
	case event := <-c.Events():
		if event.Type != channel.EventTokenDelta {
			t.Fatalf("event type = %q, want token_delta", event.Type)
		}
		if event.Delta != "hello" {
			t.Fatalf("delta = %q, want hello", event.Delta)
		}
		if event.Addr.ThreadID != "thread-1" {
			t.Fatalf("thread = %q, want thread-1", event.Addr.ThreadID)
		}
		if event.Addr.TaskID != "task-1" {
			t.Fatalf("task = %q, want task-1 (attributed from the submitted turn)", event.Addr.TaskID)
		}
	default:
		t.Fatal("no event decoded")
	}
}

// A finalized message carries its own address, which wins over the client's
// bookkeeping.
func TestOnMessageUsesFinalizedMessageAddress(t *testing.T) {
	c, _ := newTestClient(t)
	final := protocol.ChatMessage{
		ID:   "assistant-1",
		Role: protocol.RoleAssistant,
		Addr: protocol.MessageAddress{ThreadID: "thread-9", TaskID: "task-9"},
	}
	payload, err := aetherwire.StreamEnvelope(
		aetherwire.TurnMeta{AppWorkspace: "default", ThreadID: "thread-9"},
		spec.MessageFinalizedEvent{MessageID: final.ID, Message: final},
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	if err := c.onMessage(context.Background(), testMessage(payload)); err != nil {
		t.Fatalf("onMessage: %v", err)
	}
	event := <-c.Events()
	if event.Addr.ThreadID != "thread-9" || event.Addr.TaskID != "task-9" {
		t.Fatalf("addr = %+v, want the finalized message's own address", event.Addr)
	}
}

// A payload that is not a stream envelope is ignored: the session topic carries
// other traffic, and failing the receive loop over it would drop the connection.
func TestOnMessageIgnoresNonStreamPayloads(t *testing.T) {
	c, _ := newTestClient(t)
	for _, payload := range [][]byte{[]byte(`not json`), []byte(`{"hello":"world"}`), []byte(`{"options":{}}`)} {
		if err := c.onMessage(context.Background(), testMessage(payload)); err != nil {
			t.Fatalf("onMessage(%s): %v", payload, err)
		}
	}
	select {
	case event := <-c.Events():
		t.Fatalf("non-stream payload produced an event: %+v", event)
	default:
	}
}

// The client keeps a local copy of what it witnessed so the transcript survives
// a restart, and a re-delivered final must not duplicate a message.
func TestProjectionRecordsTurnAndDedupes(t *testing.T) {
	c, _ := newTestClient(t)
	projection := newMemProjection()
	c.SetHistoryProjection(projection)

	part, err := protocol.NewTextPart("hello")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	if err := c.Enqueue(context.Background(), channel.Inbound{
		Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Message: protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}},
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	final := protocol.ChatMessage{
		ID:   "assistant-1",
		Role: protocol.RoleAssistant,
		Addr: protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
	}
	payload, err := aetherwire.StreamEnvelope(
		aetherwire.TurnMeta{AppWorkspace: "default", ThreadID: "thread-1"},
		spec.MessageFinalizedEvent{MessageID: final.ID, Message: final},
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	for i := 0; i < 2; i++ { // deliver the same final twice
		if err := c.onMessage(context.Background(), testMessage(payload)); err != nil {
			t.Fatalf("onMessage: %v", err)
		}
		<-c.Events()
	}

	history := projection.threads["thread-1"]
	if len(history) != 2 {
		t.Fatalf("projection has %d messages, want 2 (user + assistant, deduped): %+v", len(history), history)
	}
	if history[0].ID != "user-1" || history[1].ID != "assistant-1" {
		t.Fatalf("projection order = %s, %s", history[0].ID, history[1].ID)
	}
}

// Cancel and approval decisions travel to the agent as control messages, since
// the turn they act on is running in another process.
func TestCancelAndResolveSendControls(t *testing.T) {
	c, sent := newTestClient(t)
	part, err := protocol.NewTextPart("hi")
	if err != nil {
		t.Fatalf("text part: %v", err)
	}
	if err := c.Enqueue(context.Background(), channel.Inbound{
		Addr:    protocol.MessageAddress{ThreadID: "thread-1", TaskID: "task-1"},
		Message: protocol.ChatMessage{ID: "user-1", Role: protocol.RoleUser, Content: []protocol.ContentPart{part}},
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if !c.Cancel("task-1") {
		t.Fatal("Cancel reported failure")
	}
	if !c.Resolve("task-1", "req-1", approval.Decision{Granted: true, Scope: "session"}) {
		t.Fatal("Resolve reported failure")
	}
	if len(*sent) != 3 {
		t.Fatalf("sent %d payloads, want 3 (turn + cancel + approve)", len(*sent))
	}

	cancelPart := firstControlPart(t, (*sent)[1])
	if cancelPart["kind"] != spec.ControlCancel || cancelPart["task_id"] != "task-1" {
		t.Fatalf("cancel control = %+v", cancelPart)
	}
	approvePart := firstControlPart(t, (*sent)[2])
	if approvePart["kind"] != spec.ControlApprove {
		t.Fatalf("approve kind = %v, want %v", approvePart["kind"], spec.ControlApprove)
	}
	if approvePart["request_id"] != "req-1" || approvePart["scope"] != "session" {
		t.Fatalf("approve control = %+v", approvePart)
	}
	// The control must be addressed to the thread whose turn it acts on, so the
	// agent can route it.
	var envelope struct {
		Addr protocol.MessageAddress `json:"addr"`
	}
	if err := json.Unmarshal((*sent)[1], &envelope); err != nil {
		t.Fatalf("decode control: %v", err)
	}
	if envelope.Addr.ThreadID != "thread-1" {
		t.Fatalf("control thread = %q, want thread-1", envelope.Addr.ThreadID)
	}
}

func firstControlPart(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var msg struct {
		Content []map[string]any `json:"content"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("decode control message: %v", err)
	}
	if len(msg.Content) == 0 {
		t.Fatalf("control message has no content: %s", payload)
	}
	return msg.Content[0]
}
