// Package acp is a reference Agent Client Protocol (ACP) v1 input channel for
// the OSS agent-harness. It speaks JSON-RPC 2.0 over a stdio reader/writer pair
// (an editor/IDE launches the harness and drives it over the ACP wire): client
// session/prompt requests become inbound turns (Receiver) and turn stream
// events are translated into ACP session/update notifications (Publisher). It
// is stdlib + messaging-spec only (no external deps); pair it with a turn.Runner
// whose Publisher is this Channel and drive it with runtime.RunLoop, running
// Serve in a goroutine.
//
// Scope of this slice: text + tool-call streaming + session lifecycle + the
// permission round-trip (session/request_permission bridged to the harness
// approval broker) + client delegation (fs/* and terminal/*): when the client
// advertised fs/terminal capabilities, TurnContext routes the harness file/shell
// tools out to the editor (see delegate.go).
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

	"github.com/scitrera/agent-harness-go/pkg/approval"
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
	// cwd is the session's absolute working directory (from session/new). ACP fs
	// paths are ABSOLUTE, so the fileDelegate resolves a workspace-relative tool
	// path against it before calling the client.
	cwd string

	mu         sync.Mutex
	promptID   json.RawMessage     // in-flight session/prompt request id (nil = none)
	resolved   bool                // whether the current prompt has been answered
	taskID     string              // in-flight turn's task id (minted by promptToMessage); the runtime keys turncancel by it
	cancelHook func()              // orchestrator-supplied cancellation callback
	prompted   map[string]struct{} // approval request ids already sent to the client (dedupe)
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
	// caps is the client's advertised fs/terminal support (from initialize);
	// TurnContext reads it to decide which client delegates to attach per turn.
	caps clientCapabilities
	// resolver bridges a client permission decision back to the harness approval
	// broker; nil disables the round-trip (*approval.Broker satisfies it).
	resolver ApprovalResolver
	// canceller aborts the in-flight turn's context via the runtime turncancel,
	// keyed by task id; nil leaves session/cancel as prompt-resolution only.
	canceller func(id string)

	seq atomic.Uint64
}

// ApprovalResolver delivers a client permission decision to the harness approval
// broker, unblocking the turn awaiting it. *approval.Broker satisfies it.
type ApprovalResolver interface {
	Resolve(taskID, requestID string, d approval.Decision) bool
}

// SetApprovalResolver wires the broker that turns awaiting approval; once set,
// pending approval_request parts drive a session/request_permission round-trip.
func (c *Channel) SetApprovalResolver(r ApprovalResolver) {
	c.mu.Lock()
	c.resolver = r
	c.mu.Unlock()
}

// SetCanceller wires a channel-level cancel func invoked on session/cancel with
// the in-flight turn's task id, so the runtime turncancel.Canceller aborts the
// running turn's context (the runtime keys turns by task id). It is the seam the
// orchestrator uses to bridge ACP cancellation to the harness canceller.
func (c *Channel) SetCanceller(cancel func(id string)) {
	c.mu.Lock()
	c.canceller = cancel
	c.mu.Unlock()
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
		// TODO(acp): authenticate, session/load, session/set_mode. (fs/* + terminal/*
		// client-delegation is implemented via TurnContext — see delegate.go.)
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
	// Capture client fs/terminal capabilities so TurnContext can attach the
	// matching client delegates for this session's turns (see delegate.go).
	var caps clientCapabilities
	if len(p.ClientCapabilities) > 0 {
		_ = json.Unmarshal(p.ClientCapabilities, &caps)
	}
	c.mu.Lock()
	c.caps = caps
	c.mu.Unlock()
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
	// p.Cwd is stored on the session so the fs/terminal delegates can resolve
	// workspace-relative tool paths to the client's absolute paths.
	// TODO(acp): honor p.MCPServers / p.AdditionalDirectories.
	sess := c.newSession(p.Cwd)
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

	msg := c.promptToMessage(sess.threadID, p.Prompt)

	sess.mu.Lock()
	sess.promptID = req.ID
	sess.resolved = false
	// Record the turn's task id so session/cancel can key the runtime canceller.
	sess.taskID = msg.Addr.TaskID
	sess.mu.Unlock()

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
	taskID := sess.taskID
	sess.mu.Unlock()
	c.mu.Lock()
	cancel := c.canceller
	c.mu.Unlock()
	// Interrupt the in-flight turn: the per-session hook (if any) plus the runtime
	// canceller keyed by the turn's task id, which cancels the turn's context.
	if hook != nil {
		hook()
	}
	if cancel != nil && taskID != "" {
		cancel(taskID)
	}
	// Resolve the pending prompt so the client still gets its terminal response.
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
	case protocol.ContentPartType(spec.PartApprovalRequest):
		return c.handleApprovalRequest(sess, e)
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

// ─── approval bridge (session/request_permission) ────────────────────────

// rejectOptionID is the sentinel option id for the synthesized "Reject" choice;
// it maps a "selected" outcome to a deny decision.
const rejectOptionID = "__reject__"

// handleApprovalRequest bridges a pending approval_request part to an ACP
// session/request_permission round-trip. The client's reply is mapped to an
// approval.Decision and delivered to the broker (unblocking the turn). It runs
// on the turn streaming path, so the blocking round-trip is spawned on its own
// goroutine and never stalls PublishEvent. Non-pending upserts (resolved status)
// and repeats for an already-prompted id are ignored.
func (c *Channel) handleApprovalRequest(sess *session, e channel.Event) error {
	if e.Part == nil {
		return nil
	}
	c.mu.Lock()
	resolver := c.resolver
	c.mu.Unlock()
	body, ok := e.Part.AsApprovalRequest()
	if !ok || body.Status != spec.ApprovalPending || resolver == nil {
		return nil
	}
	sess.mu.Lock()
	if _, seen := sess.prompted[body.ID]; seen {
		sess.mu.Unlock()
		return nil
	}
	sess.prompted[body.ID] = struct{}{}
	sess.mu.Unlock()

	go c.requestPermission(sess, resolver, e.Addr.TaskID, body)
	return nil
}

// requestPermission performs the outbound session/request_permission call and
// resolves the broker with the mapped decision. A transport/call error maps to a
// deny so the awaiting turn never hangs on a dead client.
func (c *Channel) requestPermission(sess *session, resolver ApprovalResolver, taskID string, body spec.ApprovalRequestPart) {
	params := requestPermissionParams{
		SessionID: sess.id,
		ToolCall: permToolCall{
			ToolCallID: body.ID,
			Title:      summaryOr(body),
			Kind:       toolKindFor(body.Tool),
		},
		Options: buildPermissionOptions(body.Options),
	}
	decision := approval.Decision{Granted: false}
	if raw, err := c.conn.call(context.Background(), methodRequestPermission, params); err == nil {
		var res requestPermissionResult
		if json.Unmarshal(raw, &res) == nil {
			decision = permissionDecision(res.Outcome)
		}
	}
	resolver.Resolve(taskID, body.ID, decision)
}

// summaryOr returns the approval summary, falling back to a tool-named prompt.
func summaryOr(body spec.ApprovalRequestPart) string {
	if body.Summary != "" {
		return body.Summary
	}
	return "Use the " + body.Tool + " tool"
}

// buildPermissionOptions maps grant scopes to ACP permission options and appends
// a synthesized reject option. Each scope's OptionID echoes back as the granted
// scope; "once" is allow_once, every other grant is allow_always.
func buildPermissionOptions(scopes []string) []permissionOption {
	opts := make([]permissionOption, 0, len(scopes)+1)
	for _, s := range scopes {
		kind := optAllowAlways
		if s == "once" {
			kind = optAllowOnce
		}
		opts = append(opts, permissionOption{OptionID: s, Name: humanScope(s), Kind: kind})
	}
	return append(opts, permissionOption{OptionID: rejectOptionID, Name: "Reject", Kind: optRejectOnce})
}

// humanScope renders a grant scope as a client-facing option label.
func humanScope(scope string) string {
	switch scope {
	case "once":
		return "Allow once"
	case "session":
		return "Allow for session"
	case "always":
		return "Always allow"
	default:
		return scope
	}
}

// permissionDecision maps a client permission outcome to an approval decision:
// a selected non-reject option grants with that scope; the reject sentinel and
// any non-selected outcome (e.g. "cancelled") deny.
func permissionDecision(o permissionOutcome) approval.Decision {
	if o.Outcome != "selected" || o.OptionID == rejectOptionID {
		return approval.Decision{Granted: false}
	}
	return approval.Decision{Granted: true, Scope: o.OptionID}
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

func (c *Channel) newSession(cwd string) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID("acp-sess-")
	thread := c.nextID("acp-thread-")
	s := &session{id: id, threadID: thread, cwd: cwd, prompted: map[string]struct{}{}}
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
