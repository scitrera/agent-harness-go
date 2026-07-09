package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// clientMsg is a decoded agent->client line (a response or a notification).
type clientMsg struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

// testClient drives a Channel over an in-memory stdio pipe: it writes JSON-RPC
// request lines and reads decoded agent output on a background goroutine (so a
// PublishEvent write and the client read never deadlock on the synchronous
// io.Pipe).
type testClient struct {
	w    io.Writer
	msgs chan clientMsg
}

func (tc *testClient) readLoop(r io.Reader) {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			var m clientMsg
			if json.Unmarshal(line, &m) == nil {
				tc.msgs <- m
			}
		}
		if err != nil {
			return
		}
	}
}

func (tc *testClient) send(t *testing.T, id json.RawMessage, method string, params any) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var line []byte
	if id == nil {
		line, _ = json.Marshal(rpcNotification{JSONRPC: jsonRPCVersion, Method: method, Params: raw})
	} else {
		line, _ = json.Marshal(rpcRequest{JSONRPC: jsonRPCVersion, ID: id, Method: method, Params: raw})
	}
	if _, err := tc.w.Write(append(line, '\n')); err != nil {
		t.Fatalf("client write: %v", err)
	}
}

func (tc *testClient) next(t *testing.T) clientMsg {
	t.Helper()
	select {
	case m := <-tc.msgs:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for agent message")
		return clientMsg{}
	}
}

func newTestPair(t *testing.T) (*Channel, *testClient, func()) {
	t.Helper()
	cr, cw := io.Pipe() // client -> agent
	ar, aw := io.Pipe() // agent -> client
	ch := NewChannel(cr, aw)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = ch.Serve(ctx) }()
	tc := &testClient{w: cw, msgs: make(chan clientMsg, 64)}
	go tc.readLoop(ar)
	cleanup := func() {
		cancel()
		_ = cw.Close()
		_ = aw.Close()
	}
	return ch, tc, cleanup
}

// TestInitializeNewSessionPrompt drives initialize -> session/new ->
// session/prompt and asserts FetchTask yields the expected inbound turn.
func TestInitializeNewSessionPrompt(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()

	tc.send(t, json.RawMessage("1"), methodInitialize, initializeParams{ProtocolVersion: 1})
	initResp := tc.next(t)
	if string(initResp.ID) != "1" {
		t.Fatalf("init id = %q, want 1", initResp.ID)
	}
	var ir initializeResult
	if err := json.Unmarshal(initResp.Result, &ir); err != nil {
		t.Fatalf("init result: %v", err)
	}
	if ir.ProtocolVersion != 1 || ir.AgentCapabilities.LoadSession {
		t.Fatalf("unexpected init capabilities: %+v", ir)
	}

	tc.send(t, json.RawMessage("2"), methodSessionNew, newSessionParams{Cwd: "/tmp"})
	nsResp := tc.next(t)
	var ns newSessionResult
	if err := json.Unmarshal(nsResp.Result, &ns); err != nil {
		t.Fatalf("new session result: %v", err)
	}
	if ns.SessionID == "" {
		t.Fatal("empty sessionId")
	}

	tc.send(t, json.RawMessage("3"), methodSessionPrompt, promptParams{
		SessionID: ns.SessionID,
		Prompt:    []contentBlock{{Type: "text", Text: "hello agent"}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	in, err := ch.FetchTask(ctx)
	if err != nil {
		t.Fatalf("FetchTask: %v", err)
	}

	sess := ch.sessionByID(ns.SessionID)
	if sess == nil {
		t.Fatal("session not registered")
	}
	if in.Addr.ThreadID != sess.threadID {
		t.Fatalf("inbound thread = %q, want %q", in.Addr.ThreadID, sess.threadID)
	}
	if in.Message.Role != protocol.RoleUser {
		t.Fatalf("inbound role = %q, want user", in.Message.Role)
	}
	if len(in.Message.Content) != 1 {
		t.Fatalf("inbound content parts = %d, want 1", len(in.Message.Content))
	}
	txt, ok := in.Message.Content[0].AsText()
	if !ok || txt.Text != "hello agent" {
		t.Fatalf("inbound text = %q (ok=%v), want 'hello agent'", txt.Text, ok)
	}
}

// TestStreamTextThenFinal asserts a text part streams as agent_message_chunk and
// EventMessageFinal resolves the pending session/prompt response with a stop
// reason.
func TestStreamTextThenFinal(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()

	tc.send(t, json.RawMessage("1"), methodSessionNew, newSessionParams{Cwd: "/tmp"})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	tc.send(t, json.RawMessage("2"), methodSessionPrompt, promptParams{
		SessionID: ns.SessionID,
		Prompt:    []contentBlock{{Type: "text", Text: "hi"}},
	})
	ctx := context.Background()
	if _, err := ch.FetchTask(ctx); err != nil {
		t.Fatalf("FetchTask: %v", err)
	}
	sess := ch.sessionByID(ns.SessionID)

	// Stream an assistant text part -> agent_message_chunk notification.
	part, _ := protocol.NewTextPart("answer")
	if err := ch.PublishEvent(ctx, channel.Event{
		Type:      channel.EventPartAppended,
		Addr:      protocol.MessageAddress{ThreadID: sess.threadID},
		MessageID: "m1",
		Part:      &part,
	}); err != nil {
		t.Fatalf("PublishEvent part: %v", err)
	}
	chunkMsg := tc.next(t)
	if chunkMsg.Method != methodSessionUpdate {
		t.Fatalf("expected session/update, got %q", chunkMsg.Method)
	}
	var sn sessionNotification
	if err := json.Unmarshal(chunkMsg.Params, &sn); err != nil {
		t.Fatalf("session update params: %v", err)
	}
	if sn.SessionID != ns.SessionID {
		t.Fatalf("update sessionId = %q, want %q", sn.SessionID, ns.SessionID)
	}
	var chunk agentMessageChunk
	if err := json.Unmarshal(sn.Update, &chunk); err != nil {
		t.Fatalf("chunk decode: %v", err)
	}
	if chunk.SessionUpdate != updateAgentMessageChunk || chunk.Content.Text != "answer" {
		t.Fatalf("unexpected chunk: %+v", chunk)
	}

	// Finalize the turn -> deferred session/prompt response with stop reason.
	if err := ch.PublishEvent(ctx, channel.Event{
		Type: channel.EventMessageFinal,
		Addr: protocol.MessageAddress{ThreadID: sess.threadID},
	}); err != nil {
		t.Fatalf("PublishEvent final: %v", err)
	}
	finalResp := tc.next(t)
	if string(finalResp.ID) != "2" {
		t.Fatalf("prompt response id = %q, want 2", finalResp.ID)
	}
	var pr promptResult
	if err := json.Unmarshal(finalResp.Result, &pr); err != nil {
		t.Fatalf("prompt result: %v", err)
	}
	if pr.StopReason != stopEndTurn {
		t.Fatalf("stopReason = %q, want %q", pr.StopReason, stopEndTurn)
	}
}

// TestToolLifecycleMapping asserts a tool lifecycle started/finished maps to
// tool_call then tool_call_update.
func TestToolLifecycleMapping(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()

	tc.send(t, json.RawMessage("1"), methodSessionNew, newSessionParams{Cwd: "/tmp"})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	sess := ch.sessionByID(ns.SessionID)
	ctx := context.Background()
	addr := protocol.MessageAddress{ThreadID: sess.threadID}

	started, _ := json.Marshal(toolLifecyclePayload{Status: "started", CallID: "call-1", ToolName: "read_file"})
	if err := ch.PublishEvent(ctx, channel.Event{Type: channel.EventToolLifecycle, Addr: addr, Payload: started}); err != nil {
		t.Fatalf("publish started: %v", err)
	}
	var sn sessionNotification
	if err := json.Unmarshal(tc.next(t).Params, &sn); err != nil {
		t.Fatalf("started params: %v", err)
	}
	var tcStart toolCall
	if err := json.Unmarshal(sn.Update, &tcStart); err != nil {
		t.Fatalf("tool_call decode: %v", err)
	}
	if tcStart.SessionUpdate != updateToolCall || tcStart.ToolCallID != "call-1" ||
		tcStart.Kind != toolKindRead || tcStart.Status != toolStatusInProgress {
		t.Fatalf("unexpected tool_call: %+v", tcStart)
	}

	finished, _ := json.Marshal(toolLifecyclePayload{Status: "finished", CallID: "call-1", ToolName: "read_file"})
	if err := ch.PublishEvent(ctx, channel.Event{Type: channel.EventToolLifecycle, Addr: addr, Payload: finished}); err != nil {
		t.Fatalf("publish finished: %v", err)
	}
	if err := json.Unmarshal(tc.next(t).Params, &sn); err != nil {
		t.Fatalf("finished params: %v", err)
	}
	var tcUpd toolCallUpdate
	if err := json.Unmarshal(sn.Update, &tcUpd); err != nil {
		t.Fatalf("tool_call_update decode: %v", err)
	}
	if tcUpd.SessionUpdate != updateToolCallUpdate || tcUpd.ToolCallID != "call-1" || tcUpd.Status != toolStatusCompleted {
		t.Fatalf("unexpected tool_call_update: %+v", tcUpd)
	}
}

// TestCancelResolvesPrompt asserts session/cancel resolves the in-flight prompt
// with stopReason "cancelled" and fires the cancel hook.
func TestCancelResolvesPrompt(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()

	tc.send(t, json.RawMessage("1"), methodSessionNew, newSessionParams{Cwd: "/tmp"})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	hookFired := make(chan struct{}, 1)
	ch.SetCancelHook(ns.SessionID, func() { hookFired <- struct{}{} })

	tc.send(t, json.RawMessage("2"), methodSessionPrompt, promptParams{
		SessionID: ns.SessionID,
		Prompt:    []contentBlock{{Type: "text", Text: "hi"}},
	})
	if _, err := ch.FetchTask(context.Background()); err != nil {
		t.Fatalf("FetchTask: %v", err)
	}

	tc.send(t, nil, methodSessionCancel, cancelParams{SessionID: ns.SessionID})

	resp := tc.next(t)
	if string(resp.ID) != "2" {
		t.Fatalf("cancel response id = %q, want 2", resp.ID)
	}
	var pr promptResult
	if err := json.Unmarshal(resp.Result, &pr); err != nil {
		t.Fatalf("cancel result: %v", err)
	}
	if pr.StopReason != stopCancelled {
		t.Fatalf("stopReason = %q, want %q", pr.StopReason, stopCancelled)
	}
	select {
	case <-hookFired:
	case <-time.After(time.Second):
		t.Fatal("cancel hook did not fire")
	}
}
