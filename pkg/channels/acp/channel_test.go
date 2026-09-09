// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/approval"
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

// reply writes a JSON-RPC response line echoing id (used to answer an outbound
// agent request such as session/request_permission).
func (tc *testClient) reply(t *testing.T, id json.RawMessage, result any) {
	t.Helper()
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	line, _ := json.Marshal(rpcResponse{JSONRPC: jsonRPCVersion, ID: id, Result: raw})
	if _, err := tc.w.Write(append(line, '\n')); err != nil {
		t.Fatalf("client reply: %v", err)
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

// fakeResolver records Resolve calls and signals each on a channel.
type fakeResolver struct {
	mu   sync.Mutex
	got  []resolveCall
	done chan struct{}
}

type resolveCall struct {
	taskID, reqID string
	decision      approval.Decision
}

func (f *fakeResolver) Resolve(taskID, reqID string, d approval.Decision) bool {
	f.mu.Lock()
	f.got = append(f.got, resolveCall{taskID, reqID, d})
	f.mu.Unlock()
	select {
	case f.done <- struct{}{}:
	default:
	}
	return true
}

func (f *fakeResolver) last(t *testing.T) resolveCall {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(2 * time.Second):
		t.Fatal("resolver.Resolve not called")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[len(f.got)-1]
}

// approvalTestSetup drives initialize -> session/new and returns the registered
// session plus the fake resolver wired onto the channel.
func approvalTestSetup(t *testing.T) (*Channel, *testClient, *session, *fakeResolver, func()) {
	t.Helper()
	ch, tc, cleanup := newTestPair(t)
	fake := &fakeResolver{done: make(chan struct{}, 4)}
	ch.SetApprovalResolver(fake)

	tc.send(t, json.RawMessage("1"), methodInitialize, initializeParams{ProtocolVersion: 1})
	tc.next(t)
	tc.send(t, json.RawMessage("2"), methodSessionNew, newSessionParams{Cwd: "/tmp"})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	return ch, tc, ch.sessionByID(ns.SessionID), fake, cleanup
}

// pendingApprovalPart builds a pending approval_request content part.
func pendingApprovalPart(reqID, tool string, options []string) protocol.ContentPart {
	return spec.NewApprovalRequestPart(spec.ApprovalRequestPart{
		ID:      reqID,
		Tool:    tool,
		Summary: "Use the " + tool + " tool",
		Options: options,
		Status:  spec.ApprovalPending,
	})
}

// feedPendingApproval publishes a pending approval part and reads the resulting
// outbound session/request_permission request off the client side.
func feedPendingApproval(t *testing.T, ch *Channel, tc *testClient, sess *session, taskID, reqID, tool string, options []string) clientMsg {
	t.Helper()
	part := pendingApprovalPart(reqID, tool, options)
	if err := ch.PublishEvent(context.Background(), channel.Event{
		Type: channel.EventPartAppended,
		Addr: protocol.MessageAddress{ThreadID: sess.threadID, TaskID: taskID},
		Part: &part,
	}); err != nil {
		t.Fatalf("PublishEvent approval: %v", err)
	}
	req := tc.next(t)
	if req.Method != methodRequestPermission {
		t.Fatalf("expected %q, got %q", methodRequestPermission, req.Method)
	}
	return req
}

// TestRequestPermissionGranted asserts a pending approval_request drives a
// session/request_permission round-trip and a selected option resolves the
// broker with a matching grant.
func TestRequestPermissionGranted(t *testing.T) {
	ch, tc, sess, fake, cleanup := approvalTestSetup(t)
	defer cleanup()

	req := feedPendingApproval(t, ch, tc, sess, "task-1", "appr-1", "write_file", []string{"once", "session", "always"})

	var params requestPermissionParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("permission params: %v", err)
	}
	if params.SessionID != sess.id || params.ToolCall.ToolCallID != "appr-1" {
		t.Fatalf("unexpected params: %+v", params)
	}
	// once/session/always scopes + a synthesized reject option.
	if len(params.Options) != 4 || params.Options[3].OptionID != rejectOptionID {
		t.Fatalf("unexpected options: %+v", params.Options)
	}

	tc.reply(t, req.ID, requestPermissionResult{Outcome: permissionOutcome{Outcome: "selected", OptionID: "session"}})

	got := fake.last(t)
	if got.taskID != "task-1" || got.reqID != "appr-1" {
		t.Fatalf("resolve keys = (%q,%q), want (task-1,appr-1)", got.taskID, got.reqID)
	}
	if !got.decision.Granted || got.decision.Scope != "session" {
		t.Fatalf("decision = %+v, want granted session", got.decision)
	}
}

// TestRequestPermissionRejected asserts the reject option and a cancelled
// outcome both resolve the broker with a deny decision.
func TestRequestPermissionRejected(t *testing.T) {
	ch, tc, sess, fake, cleanup := approvalTestSetup(t)
	defer cleanup()

	// Reject option selected -> deny.
	req := feedPendingApproval(t, ch, tc, sess, "task-1", "appr-1", "shell", []string{"once", "session"})
	tc.reply(t, req.ID, requestPermissionResult{Outcome: permissionOutcome{Outcome: "selected", OptionID: rejectOptionID}})
	if got := fake.last(t); got.decision.Granted {
		t.Fatalf("reject option: decision = %+v, want denied", got.decision)
	}

	// Cancelled outcome -> deny.
	req = feedPendingApproval(t, ch, tc, sess, "task-2", "appr-2", "shell", []string{"once", "session"})
	tc.reply(t, req.ID, requestPermissionResult{Outcome: permissionOutcome{Outcome: "cancelled"}})
	got := fake.last(t)
	if got.reqID != "appr-2" || got.decision.Granted {
		t.Fatalf("cancelled: %+v, want denied for appr-2", got)
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

// TestCancelInvokesCanceller asserts session/cancel invokes the wired channel
// canceller with the in-flight turn's task id AND resolves the prompt with
// stopReason "cancelled".
func TestCancelInvokesCanceller(t *testing.T) {
	ch, tc, cleanup := newTestPair(t)
	defer cleanup()

	tc.send(t, json.RawMessage("1"), methodSessionNew, newSessionParams{Cwd: "/tmp"})
	var ns newSessionResult
	if err := json.Unmarshal(tc.next(t).Result, &ns); err != nil {
		t.Fatalf("new session: %v", err)
	}
	gotID := make(chan string, 1)
	ch.SetCanceller(func(id string) { gotID <- id })

	tc.send(t, json.RawMessage("2"), methodSessionPrompt, promptParams{
		SessionID: ns.SessionID,
		Prompt:    []contentBlock{{Type: "text", Text: "hi"}},
	})
	in, err := ch.FetchTask(context.Background())
	if err != nil {
		t.Fatalf("FetchTask: %v", err)
	}
	wantID := in.Message.Addr.TaskID
	if wantID == "" {
		t.Fatal("inbound task id is empty")
	}

	tc.send(t, nil, methodSessionCancel, cancelParams{SessionID: ns.SessionID})

	resp := tc.next(t)
	var pr promptResult
	if err := json.Unmarshal(resp.Result, &pr); err != nil {
		t.Fatalf("cancel result: %v", err)
	}
	if pr.StopReason != stopCancelled {
		t.Fatalf("stopReason = %q, want %q", pr.StopReason, stopCancelled)
	}
	select {
	case id := <-gotID:
		if id != wantID {
			t.Fatalf("canceller got id %q, want %q", id, wantID)
		}
	case <-time.After(time.Second):
		t.Fatal("canceller was not invoked")
	}
}
