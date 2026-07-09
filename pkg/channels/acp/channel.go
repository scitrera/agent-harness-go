// Package acp is a reference Agent Client Protocol (ACP) v1 input channel for
// the OSS agent-harness. It speaks JSON-RPC 2.0 over a stdio reader/writer pair
// (an editor/IDE launches the harness and drives it over the ACP wire): client
// session/prompt requests become inbound turns (Receiver) and turn stream
// events are translated into ACP session/update notifications (Publisher). It
// is stdlib + messaging-spec only (no external deps); pair it with a turn.Runner
// whose Publisher is this Channel and drive it with runtime.RunLoop, running
// Serve in a goroutine.
//
// Scope of this slice: text + tool-call streaming + session lifecycle. Client
// delegation (fs/* and terminal/*) and the permission round-trip
// (session/request_permission) are NOT implemented — see the TODO(acp) seams.
package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const (
	// inboxBuffer bounds queued-but-unstarted turns before Enqueue blocks.
	inboxBuffer = 64
	// agentName / agentVersion identify this implementation in initialize.
	agentName    = "agent-harness-go"
	agentVersion = "0.1.0"
)

// session tracks one ACP session and its in-flight prompt turn. The ACP
// sessionId is mapped to a harness thread id (Addr.ThreadID); both index the
// registry so a completing turn (keyed by thread id via EventMessageFinal) and
// a session/cancel (keyed by session id) resolve the same in-flight prompt.
type session struct {
	id       string // ACP sessionId
	threadID string // harness Addr.ThreadID

	mu         sync.Mutex
	promptID   json.RawMessage // in-flight session/prompt request id (nil = none)
	resolved   bool            // whether the current prompt has been answered
	cancelHook func()          // orchestrator-supplied cancellation callback
}

// Channel is an ACP v1 transport. session/prompt requests arrive over stdio and
// are drained by FetchTask; turn stream events are translated into session/update
// notifications by PublishEvent. Enqueue injects an inbound turn directly (used
// for background pushes), mirroring the web channel.
type Channel struct {
	conn  *conn
	inbox chan channel.Inbound

	mu       sync.Mutex
	byID     map[string]*session // sessionId  -> session
	byThread map[string]*session // threadID   -> session

	seq atomic.Uint64
}

// NewChannel returns a ready Channel reading JSON-RPC from r and writing to w
// (typically os.Stdin / os.Stdout). Call Serve to run the read loop.
func NewChannel(r io.Reader, w io.Writer) *Channel {
	return &Channel{
		conn:     newConn(r, w),
		inbox:    make(chan channel.Inbound, inboxBuffer),
		byID:     make(map[string]*session),
		byThread: make(map[string]*session),
	}
}

// Serve runs the JSON-RPC read loop, dispatching each inbound message until the
// reader closes (io.EOF, returns nil) or a write fails. It is blocking; run it
// in a goroutine. ctx is threaded to enqueue on prompt handling; note a read
// blocked on the stdio reader is interrupted only by closing that reader, not
// by ctx.
func (c *Channel) Serve(ctx context.Context) error {
	for {
		req, err := c.conn.read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := c.dispatch(ctx, req); err != nil {
			return err
		}
	}
}

func (c *Channel) dispatch(ctx context.Context, req rpcRequest) error {
	switch req.Method {
	case methodInitialize:
		return c.handleInitialize(req)
	case methodSessionNew:
		return c.handleNewSession(req)
	case methodSessionPrompt:
		return c.handlePrompt(ctx, req)
	case methodSessionCancel:
		c.handleCancel(req)
		return nil
	default:
		if req.isNotification() {
			return nil // ignore unknown notifications
		}
		// TODO(acp): authenticate, session/load, session/set_mode, fs/*, terminal/*.
		return c.conn.writeError(req.ID, codeMethodNotFound, "method not found: "+req.Method)
	}
}

// ─── ingress: channel.Receiver + channel.Enqueuer ────────────────────────

// FetchTask implements channel.Receiver. It blocks until an inbound turn is
// available or ctx is cancelled; idleness simply blocks (it never returns
// ErrNoTask), which suits runtime.RunLoop.
func (c *Channel) FetchTask(ctx context.Context) (channel.Inbound, error) {
	select {
	case in := <-c.inbox:
		return in, nil
	case <-ctx.Done():
		return channel.Inbound{}, ctx.Err()
	}
}

// Enqueue implements channel.Enqueuer: push an inbound turn into the inbox. It
// blocks only while the inbox buffer is full (or until ctx is cancelled).
func (c *Channel) Enqueue(ctx context.Context, in channel.Inbound) error {
	select {
	case c.inbox <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ─── method handlers ─────────────────────────────────────────────────────

func (c *Channel) handleInitialize(req rpcRequest) error {
	var p initializeParams
	_ = json.Unmarshal(req.Params, &p) // tolerate absent/partial params
	version := p.ProtocolVersion
	if version == 0 {
		version = protocolVersionV1
	}
	res := initializeResult{
		ProtocolVersion: version,
		AgentCapabilities: agentCapabilities{
			// loadSession stays false until history replay is wired.
			// TODO(acp): advertise loadSession + implement session/load.
			LoadSession: false,
			// Baseline text prompts only; image/audio/embeddedContext off.
			PromptCapabilities: promptCapabilities{},
		},
		AgentInfo: &implementation{Name: agentName, Version: agentVersion},
	}
	return c.conn.writeResponse(req.ID, res)
}

func (c *Channel) handleNewSession(req rpcRequest) error {
	var p newSessionParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return c.conn.writeError(req.ID, codeInvalidParams, err.Error())
		}
	}
	// TODO(acp): honor p.Cwd / p.MCPServers / p.AdditionalDirectories once
	// fs+terminal client-delegation is implemented.
	sess := c.newSession()
	return c.conn.writeResponse(req.ID, newSessionResult{SessionID: sess.id})
}

// handlePrompt converts the prompt content blocks into an inbound turn and
// enqueues it, then returns WITHOUT writing a JSON-RPC response. The response is
// deferred: it is written when the turn completes (PublishEvent sees
// EventMessageFinal for this session's thread) via resolvePrompt, correlating
// the stored request id with the terminal stop reason.
func (c *Channel) handlePrompt(ctx context.Context, req rpcRequest) error {
	var p promptParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return c.conn.writeError(req.ID, codeInvalidParams, err.Error())
	}
	sess := c.sessionByID(p.SessionID)
	if sess == nil {
		return c.conn.writeError(req.ID, codeInvalidParams, "unknown session: "+p.SessionID)
	}

	sess.mu.Lock()
	sess.promptID = req.ID
	sess.resolved = false
	sess.mu.Unlock()

	msg := c.promptToMessage(sess.threadID, p.Prompt)
	if err := c.Enqueue(ctx, channel.Inbound{Addr: msg.Addr, Message: msg}); err != nil {
		// Could not hand off the turn — answer the prompt so the client is not
		// left waiting on a response that will never come.
		c.resolvePrompt(sess, stopRefusal)
		return nil
	}
	return nil
}

// handleCancel resolves the session's in-flight prompt with stopReason
// "cancelled" and invokes the orchestrator's cancel hook (if wired). session/
// cancel is a notification, so there is no response to write.
func (c *Channel) handleCancel(req rpcRequest) {
	var p cancelParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return
	}
	sess := c.sessionByID(p.SessionID)
	if sess == nil {
		return
	}
	sess.mu.Lock()
	hook := sess.cancelHook
	sess.mu.Unlock()
	// TODO(acp): the orchestrator wires this hook to turncancel so the in-flight
	// turn is actually interrupted; for now we only resolve the prompt.
	if hook != nil {
		hook()
	}
	c.resolvePrompt(sess, stopCancelled)
}

// SetCancelHook registers a callback invoked when the client sends
// session/cancel for sessionID. It is the seam the orchestrator uses to bridge
// ACP cancellation to the harness turncancel.Canceller.
func (c *Channel) SetCancelHook(sessionID string, fn func()) {
	if sess := c.sessionByID(sessionID); sess != nil {
		sess.mu.Lock()
		sess.cancelHook = fn
		sess.mu.Unlock()
	}
}

// ─── egress: channel.Publisher ───────────────────────────────────────────

// PublishEvent implements channel.Publisher: translate a turn stream event into
// the matching ACP session/update notification for the event's thread/session.
// Events for a thread this channel does not own (e.g. a background sub-agent)
// are dropped.
func (c *Channel) PublishEvent(_ context.Context, e channel.Event) error {
	sess := c.sessionByThread(e.Addr.ThreadID)
	if sess == nil {
		return nil
	}
	switch e.Type {
	case channel.EventMessageFinal:
		// Terminal: stop streaming and resolve the pending session/prompt.
		c.resolvePrompt(sess, stopEndTurn)
		return nil
	case channel.EventTokenDelta:
		if e.Delta == "" {
			return nil
		}
		return c.sendUpdate(sess, agentTextChunk(e.MessageID, e.Delta))
	case channel.EventPartAppended:
		return c.publishPart(sess, e)
	case channel.EventToolLifecycle:
		return c.publishToolLifecycle(sess, e)
	case channel.EventToolResult:
		if e.Part != nil && e.Part.Type() == protocol.ContentToolResult {
			return c.sendUpdate(sess, toolCallUpdateFromResultPart(*e.Part))
		}
		return nil
	case channel.EventError:
		return c.publishError(sess, e)
	default:
		// message_started / part_updated are not surfaced in this slice.
		return nil
	}
}

// publishPart maps a part_appended event to the ACP variant for its part type:
// text/image -> agent_message_chunk, tool_call -> tool_call, tool_result ->
// tool_call_update, todo -> plan.
func (c *Channel) publishPart(sess *session, e channel.Event) error {
	if e.Part == nil {
		return nil
	}
	p := *e.Part
	switch p.Type() {
	case protocol.ContentText:
		if body, ok := p.AsText(); ok && body.Text != "" {
			return c.sendUpdate(sess, agentTextChunk(e.MessageID, body.Text))
		}
	case protocol.ContentImage:
		if cb, ok := partToContentBlock(p); ok {
			return c.sendUpdate(sess, agentMessageChunk{SessionUpdate: updateAgentMessageChunk, Content: cb, MessageID: e.MessageID})
		}
	case protocol.ContentToolCall:
		return c.sendUpdate(sess, toolCallFromPart(p))
	case protocol.ContentToolResult:
		return c.sendUpdate(sess, toolCallUpdateFromResultPart(p))
	case protocol.ContentPartType(spec.PartTodo):
		if body, ok := p.AsTodo(); ok {
			return c.sendUpdate(sess, planFromTodo(body))
		}
	}
	return nil
}

// toolLifecyclePayload is the subset of the harness tool lifecycle event
// (turn/tools ToolEvent JSON) this channel needs. Decoded locally to keep the
// package self-contained (no import of pkg/tools).
type toolLifecyclePayload struct {
	Status       string `json:"status"` // started | finished | aborted
	CallID       string `json:"call_id"`
	ToolName     string `json:"tool_name"`
	IsError      bool   `json:"is_error"`
	ErrorMessage string `json:"error"`
}

// publishToolLifecycle maps a tool_lifecycle event: started -> tool_call,
// finished/aborted -> tool_call_update{completed|failed}.
func (c *Channel) publishToolLifecycle(sess *session, e channel.Event) error {
	if len(e.Payload) == 0 {
		return nil
	}
	var pl toolLifecyclePayload
	if err := json.Unmarshal(e.Payload, &pl); err != nil {
		return nil
	}
	switch pl.Status {
	case "started":
		return c.sendUpdate(sess, toolCall{
			SessionUpdate: updateToolCall,
			ToolCallID:    pl.CallID,
			Title:         pl.ToolName,
			Kind:          toolKindFor(pl.ToolName),
			Status:        toolStatusInProgress,
		})
	case "finished", "aborted":
		status := toolStatusCompleted
		if pl.IsError || pl.Status == "aborted" {
			status = toolStatusFailed
		}
		u := toolCallUpdate{SessionUpdate: updateToolCallUpdate, ToolCallID: pl.CallID, Status: status}
		if pl.ErrorMessage != "" {
			u.Content = []toolCallContent{{Type: "content", Content: contentBlock{Type: "text", Text: pl.ErrorMessage}}}
		}
		return c.sendUpdate(sess, u)
	}
	return nil
}

// publishError surfaces a bare error event as an agent_message_chunk. A bare
// error carries no tool call id, so it cannot be a tool_call_update.
// TODO(acp): when an error is tied to a tool call, emit tool_call_update{failed}.
func (c *Channel) publishError(sess *session, e channel.Event) error {
	msg := "error"
	if len(e.Payload) > 0 {
		var p struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(e.Payload, &p)
		switch {
		case p.Error != "":
			msg = p.Error
		case p.Message != "":
			msg = p.Message
		}
	}
	return c.sendUpdate(sess, agentTextChunk(e.MessageID, "[error] "+msg))
}

// sendUpdate marshals a sessionUpdate variant and writes it as a session/update
// notification for the session.
func (c *Channel) sendUpdate(sess *session, update any) error {
	raw, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("acp: marshal session update: %w", err)
	}
	return c.conn.notify(methodSessionUpdate, sessionNotification{SessionID: sess.id, Update: raw})
}

// ─── turn-completion correlation (v1/v2 isolation point) ─────────────────

// resolvePrompt completes the session's in-flight session/prompt request with a
// terminal stop reason, at most once per prompt. It is the single place turn
// completion is signalled to the client, deliberately isolating the ACP v1 wire
// shape so a future v2 variant is a localized change.
//
// v1 (this implementation): the stop reason rides the session/prompt *response*.
// v2 divergence: v2 moves the stop reason OFF the prompt response and onto a
// session/update "state_update" notification, replying to session/prompt with an
// empty result. To target v2, change ONLY this method — emit the state_update
// notification, then writeResponse(id, struct{}{}).
func (c *Channel) resolvePrompt(sess *session, reason string) {
	sess.mu.Lock()
	if sess.resolved || sess.promptID == nil {
		sess.mu.Unlock()
		return
	}
	id := sess.promptID
	sess.resolved = true
	sess.promptID = nil
	sess.mu.Unlock()
	// Best-effort: a closed stream simply drops the response.
	_ = c.conn.writeResponse(id, promptResult{StopReason: reason})
}

// ─── helpers ─────────────────────────────────────────────────────────────

func (c *Channel) newSession() *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID("acp-sess-")
	thread := c.nextID("acp-thread-")
	s := &session{id: id, threadID: thread}
	c.byID[id] = s
	c.byThread[thread] = s
	return s
}

func (c *Channel) sessionByID(id string) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byID[id]
}

func (c *Channel) sessionByThread(threadID string) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byThread[threadID]
}

func (c *Channel) nextID(prefix string) string {
	return prefix + strconv.FormatUint(c.seq.Add(1), 10)
}

// promptToMessage builds a user ChatMessage addressed to threadID from ACP
// prompt content blocks, minting a fresh task id for the turn.
func (c *Channel) promptToMessage(threadID string, blocks []contentBlock) protocol.ChatMessage {
	taskID := c.nextID("acp-task-")
	parts := make([]protocol.ContentPart, 0, len(blocks))
	for _, b := range blocks {
		if part, ok := contentBlockToPart(b); ok {
			parts = append(parts, part)
		}
	}
	addr := protocol.MessageAddress{ThreadID: threadID, TaskID: taskID}
	return protocol.ChatMessage{
		SchemaVersion: spec.MessagingSchemaVersion,
		ID:            "user-" + taskID,
		Role:          protocol.RoleUser,
		Addr:          addr,
		Content:       parts,
	}
}

// agentTextChunk builds an agent_message_chunk carrying text.
func agentTextChunk(msgID, text string) agentMessageChunk {
	return agentMessageChunk{
		SessionUpdate: updateAgentMessageChunk,
		Content:       contentBlock{Type: "text", Text: text},
		MessageID:     msgID,
	}
}

// toolCallFromPart builds a tool_call from a spec tool_call content part.
func toolCallFromPart(p protocol.ContentPart) toolCall {
	body, _ := p.AsToolCall()
	return toolCall{
		SessionUpdate: updateToolCall,
		ToolCallID:    body.ID,
		Title:         body.Name,
		Kind:          toolKindFor(body.Name),
		Status:        mapToolStatus(body.Status),
		RawInput:      protocol.ArgsToRaw(body.Args),
	}
}

// toolCallUpdateFromResultPart builds a tool_call_update from a spec tool_result
// content part, carrying the result text (or raw output) as content.
func toolCallUpdateFromResultPart(p protocol.ContentPart) toolCallUpdate {
	body, _ := p.AsToolResult()
	status := toolStatusCompleted
	if body.IsError {
		status = toolStatusFailed
	}
	u := toolCallUpdate{SessionUpdate: updateToolCallUpdate, ToolCallID: body.CallID, Status: status}
	switch {
	case body.OutputText != "":
		u.Content = []toolCallContent{{Type: "content", Content: contentBlock{Type: "text", Text: body.OutputText}}}
	case len(body.Output) > 0:
		u.Content = []toolCallContent{{Type: "content", Content: contentBlock{Type: "text", Text: string(body.Output)}}}
	}
	return u
}

// planFromTodo builds a plan update from a spec todo content part.
func planFromTodo(body spec.TodoPart) planUpdate {
	entries := make([]planEntry, 0, len(body.Items))
	for _, item := range body.Items {
		entries = append(entries, planEntry{
			Content:  item.Content,
			Priority: "medium",
			Status:   mapPlanStatus(item.Status),
		})
	}
	return planUpdate{SessionUpdate: updatePlan, Entries: entries}
}

var (
	_ channel.Channel  = (*Channel)(nil)
	_ channel.Enqueuer = (*Channel)(nil)
)
