// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/aetherwire"
	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// eventBuffer bounds decoded stream events awaiting the UI. Mirrors the
// in-process channels' egress buffer.
const eventBuffer = 512

// ClientConfig configures the frontend-side Aether transport.
type ClientConfig struct {
	// ServerAddr is the gateway address. Required.
	ServerAddr string
	// Workspace is the Aether workspace the target agent lives in.
	Workspace string
	// SessionWorkspace is the default application-level workspace stamped on
	// turns. Empty falls back to the Aether routing Workspace.
	SessionWorkspace string
	// AgentImplementation and AgentSpecifier address the agent: together with
	// Workspace they form ag::<workspace>::<implementation>::<specifier>.
	AgentImplementation string
	AgentSpecifier      string
	// UserID and WindowID identify this client session. WindowID distinguishes
	// two frontends run by the same user, and is what the agent replies to.
	UserID   string
	WindowID string

	// Credentials. All optional; a dev-mode gateway accepts an unauthenticated
	// connection.
	APIKey string
	Token  string
	Tenant string

	TLSEnabled            bool
	TLSInsecureSkipVerify bool
	TLSServerName         string
	RootCAs               []byte
	ClientCert            []byte
	ClientKey             []byte
}

// Client is the frontend-side Aether transport. It presents the surface the
// UIs already consume from the in-process channels — Enqueue, Events,
// DroppedEvents — but the turn crosses a gateway to an agent worker instead of
// a Go channel to a local runner. It additionally carries cancel and approval
// decisions to that agent, so a UI can drive a remote turn exactly as it drives
// a local one.
type Client struct {
	client           *sdk.UserClient
	workspace        string
	sessionWorkspace string
	impl             string
	specifier        string
	userID           string
	windowID         string

	events        chan channel.Event
	sessionEvents chan spec.SessionEvent
	sessionErrors chan SessionRemoteError
	dropped       atomic.Int64

	// threadTask remembers the task a thread's turn was submitted under, so
	// events that carry no address of their own (token deltas, part updates)
	// can still be attributed to the right thread and turn. Without it the UI
	// treats every delta as background activity for another thread.
	mu              sync.Mutex
	threadTask      map[workspaceThreadKey]string
	taskAddr        map[string]protocol.MessageAddress
	pendingAttach   map[string]chan sessionAttachResponse
	activeToolCalls map[clientToolCallKey]*activeClientToolCall

	closeOnce sync.Once
	closed    chan struct{}

	// projection, when set, keeps a local copy of the conversation so the UI can
	// render a thread it did not just watch happen (a restart, a thread switch).
	// The agent holds the authoritative transcript; this is only what this client
	// witnessed, which is why it is a projection and not a store.
	projectionMu        sync.Mutex
	projection          HistoryProjection
	workspaceProjection WorkspaceHistoryProjection
	toolHost            *ClientToolHost

	// sendToAgent is the egress seam, so ingress/egress are testable without a
	// live gateway.
	sendToAgent        func(payload []byte) error
	sendCheckedToAgent func(payload []byte, access *pb.ResourceAccessRequest) error
	sendToolMessage    func(topic string, payload []byte) error
}

type sessionAttachResponse struct {
	result *spec.SessionAttachResult
	err    *SessionRemoteError
}

type workspaceThreadKey struct {
	workspaceID string
	threadID    string
}

// SessionRemoteError is a structured error returned by the remote session
// service. RequestID correlates attach failures and post-attach gap notices.
type SessionRemoteError struct {
	RequestID string
	Code      string
	Message   string
	Retryable bool
}

func (e SessionRemoteError) Error() string {
	return fmt.Sprintf("aether: session %s: %s", e.Code, e.Message)
}

// HistoryProjection is the local transcript copy a client may keep. It is the
// read/write pair of the UI's history store.
type HistoryProjection interface {
	LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error)
	SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error
}

// WorkspaceHistoryProjection is the explicit multi-workspace projection
// contract. Dynamic clients use it instead of adapting unscoped methods.
type WorkspaceHistoryProjection interface {
	HistoryProjection
	LoadWorkspaceHistory(ctx context.Context, workspaceID, threadID string) ([]protocol.ChatMessage, error)
	SaveWorkspaceHistory(ctx context.Context, workspaceID, threadID string, messages []protocol.ChatMessage) error
}

// NewClient constructs (but does not connect) the frontend transport.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.ServerAddr == "" {
		return nil, fmt.Errorf("aether: server address required")
	}
	if cfg.UserID == "" {
		return nil, fmt.Errorf("aether: user id required")
	}
	if cfg.WindowID == "" {
		return nil, fmt.Errorf("aether: window id required")
	}
	impl := cfg.AgentImplementation
	if impl == "" {
		impl = defaultImplementation
	}
	opts := sdk.UserOptions{
		ClientOptions: sdk.ClientOptions{ServerAddr: cfg.ServerAddr},
		UserID:        cfg.UserID,
		WindowID:      cfg.WindowID,
		Workspace:     cfg.Workspace,
	}
	if cfg.TLSEnabled {
		opts.TLS = &sdk.TLSConfig{
			Enabled:            true,
			RootCAs:            cfg.RootCAs,
			ClientCert:         cfg.ClientCert,
			ClientKey:          cfg.ClientKey,
			ServerName:         cfg.TLSServerName,
			InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
		}
	}
	if creds := clientCredentials(cfg); len(creds) > 0 {
		opts.Credentials = creds
	}
	user, err := sdk.NewUserClient(opts)
	if err != nil {
		return nil, fmt.Errorf("aether: new user client: %w", err)
	}
	c := &Client{
		client:           user,
		workspace:        cfg.Workspace,
		sessionWorkspace: cfg.SessionWorkspace,
		impl:             impl,
		specifier:        cfg.AgentSpecifier,
		userID:           cfg.UserID,
		windowID:         cfg.WindowID,
		events:           make(chan channel.Event, eventBuffer),
		sessionEvents:    make(chan spec.SessionEvent, eventBuffer),
		sessionErrors:    make(chan SessionRemoteError, eventBuffer),
		threadTask:       map[workspaceThreadKey]string{},
		taskAddr:         map[string]protocol.MessageAddress{},
		pendingAttach:    map[string]chan sessionAttachResponse{},
		activeToolCalls:  map[clientToolCallKey]*activeClientToolCall{},
		closed:           make(chan struct{}),
	}
	if c.sessionWorkspace == "" {
		c.sessionWorkspace = c.workspace
	}
	c.sendToAgent = func(payload []byte) error {
		return user.SendToAgent(c.workspace, c.impl, c.specifier, payload)
	}
	c.sendCheckedToAgent = func(payload []byte, access *pb.ResourceAccessRequest) error {
		return user.SendWithOptions(sdk.SendMessageOptions{
			TargetTopic: c.AgentTopic(), Payload: payload,
			MessageType: sdk.MessageTypeChat, CheckedAccess: access,
		})
	}
	c.sendToolMessage = user.SendToolCallMessage
	user.OnMessage(c.onMessage)
	user.OnToolCallMessage(c.onToolCallMessage)
	return c, nil
}

func clientCredentials(cfg ClientConfig) map[string]string {
	creds := map[string]string{}
	if cfg.APIKey != "" {
		creds["api_key"] = cfg.APIKey
	}
	if cfg.Token != "" {
		creds["token"] = cfg.Token
	}
	if cfg.Tenant != "" {
		creds["tenant_id"] = cfg.Tenant
	}
	return creds
}

// SetHistoryProjection wires the local transcript copy. Without it the UI still
// renders a live turn, but a restart or a thread switch shows nothing for a
// conversation this process did not witness.
func (c *Client) SetHistoryProjection(p HistoryProjection) {
	c.projectionMu.Lock()
	c.projection = p
	c.workspaceProjection = nil
	c.projectionMu.Unlock()
}

// SetWorkspaceHistoryProjection wires a projection that preserves composite
// workspace/thread identity for a client that can change logical projects.
func (c *Client) SetWorkspaceHistoryProjection(p WorkspaceHistoryProjection) {
	c.projectionMu.Lock()
	c.projection = p
	c.workspaceProjection = p
	c.projectionMu.Unlock()
}

// recordProjection appends one message to the local transcript. Best-effort: a
// projection failure must never fail the turn, since the agent's copy is the
// authoritative one.
func (c *Client) recordProjection(ctx context.Context, msg protocol.ChatMessage) {
	if msg.Addr.ThreadID == "" || shellcontext.IsCommitAck(msg) {
		return
	}
	c.projectionMu.Lock()
	defer c.projectionMu.Unlock()
	if c.projection == nil {
		return
	}
	var history []protocol.ChatMessage
	var err error
	if c.workspaceProjection != nil {
		history, err = c.workspaceProjection.LoadWorkspaceHistory(ctx, msg.Addr.WorkspaceID, msg.Addr.ThreadID)
	} else {
		history, err = c.projection.LoadHistory(ctx, msg.Addr.ThreadID)
	}
	if err != nil {
		return
	}
	for _, existing := range history {
		if existing.ID != "" && existing.ID == msg.ID {
			return // already recorded (a re-delivered final)
		}
	}
	if c.workspaceProjection != nil {
		_ = c.workspaceProjection.SaveWorkspaceHistory(ctx, msg.Addr.WorkspaceID, msg.Addr.ThreadID, append(history, msg))
		return
	}
	_ = c.projection.SaveHistory(ctx, msg.Addr.ThreadID, append(history, msg))
}

// Start connects and runs the receive loop in the background.
func (c *Client) Start(ctx context.Context) error {
	if err := c.client.Connect(ctx); err != nil {
		return fmt.Errorf("aether: connect: %w", err)
	}
	go func() { _ = c.client.Run(ctx) }()
	go func() {
		select {
		case <-ctx.Done():
			c.cancelAllActiveToolCalls()
		case <-c.closed:
		}
	}()
	return nil
}

// Close shuts down the client.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		c.cancelAllActiveToolCalls()
		err = c.client.Close()
	})
	return err
}

// AgentTopic reports the agent this client submits turns to.
func (c *Client) AgentTopic() string {
	return sdk.AgentTopic(c.workspace, c.impl, c.specifier)
}

// ToolHostID is the gateway-routable identity bound to this exact frontend
// window. The gateway-provided SourceTopic must match it on the worker.
func (c *Client) ToolHostID() string { return sdk.UserTopic(c.userID, c.windowID) }

// SetToolHost enables exact-host workspace tool execution on this client.
func (c *Client) SetToolHost(host *ClientToolHost) {
	c.mu.Lock()
	previous := c.toolHost
	var cancels []context.CancelFunc
	if previous != nil && previous != host {
		cancels = c.activeToolCancelsLocked()
	}
	c.toolHost = host
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// ProxyHTTP routes a request over the frontend's existing Aether connection.
// Service integrations can therefore share its authenticated user session
// without opening a second identity or connection.
func (c *Client) ProxyHTTP(ctx context.Context, target string, req *http.Request, opts ...sdk.ProxyOpt) (*http.Response, error) {
	return c.client.ProxyHTTP(ctx, target, req, opts...)
}

// AttachSession requests a coherent snapshot and optional replay over the same
// Aether connection used for turns. The stable client_id belongs to the caller;
// when omitted, this frontend window is used as the stable reconnect identity.
func (c *Client) AttachSession(ctx context.Context, request spec.SessionAttachRequest) (spec.SessionAttachResult, error) {
	if request.ClientID == "" {
		request.ClientID = c.windowID
	}
	if err := request.Validate(); err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("aether: session attach request: %w", err)
	}
	requestID, err := ids.New("attach-")
	if err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("aether: session attach id: %w", err)
	}
	frame, err := spec.NewSessionFrame(spec.SessionFrameAttachRequest, requestID, request)
	if err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("aether: session attach frame: %w", err)
	}
	payload, err := aetherwire.SessionFramePayload(frame)
	if err != nil {
		return spec.SessionAttachResult{}, err
	}
	response := make(chan sessionAttachResponse, 1)
	c.mu.Lock()
	c.pendingAttach[requestID] = response
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.pendingAttach[requestID] == response {
			delete(c.pendingAttach, requestID)
		}
		c.mu.Unlock()
	}()
	if err := c.sendToAgent(payload); err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("aether: send session attach: %w", err)
	}
	select {
	case <-ctx.Done():
		return spec.SessionAttachResult{}, ctx.Err()
	case received := <-response:
		if received.err != nil {
			return spec.SessionAttachResult{}, *received.err
		}
		if received.result == nil {
			return spec.SessionAttachResult{}, fmt.Errorf("aether: empty session attach response")
		}
		if err := received.result.Validate(request); err != nil {
			return spec.SessionAttachResult{}, fmt.Errorf("aether: invalid session attach result: %w", err)
		}
		return *received.result, nil
	}
}

// SessionEvents exposes cursor-bearing events for sessions attached by this
// client. They are distinct from the legacy per-turn Events stream.
func (c *Client) SessionEvents() <-chan spec.SessionEvent { return c.sessionEvents }

// SessionErrors exposes asynchronous gap/not-supported notices that no longer
// have a waiting AttachSession caller.
func (c *Client) SessionErrors() <-chan SessionRemoteError { return c.sessionErrors }

// Enqueue submits a turn to the agent. It stamps this session's identity on the
// address so the agent can route the reply back even if the transport does not
// report a source topic.
func (c *Client) Enqueue(ctx context.Context, in channel.Inbound) error {
	msg := in.Message
	msg.Addr = in.Addr
	if msg.Addr.UserID == "" {
		msg.Addr.UserID = c.userID
	}
	if msg.Addr.RequestID == "" {
		msg.Addr.RequestID = c.windowID
	}
	if msg.Addr.WorkspaceID == "" {
		msg.Addr.WorkspaceID = c.sessionWorkspace
	}
	if msg.Addr.ThreadID != "" && msg.Addr.TaskID != "" {
		c.mu.Lock()
		c.threadTask[workspaceThreadKey{workspaceID: msg.Addr.WorkspaceID, threadID: msg.Addr.ThreadID}] = msg.Addr.TaskID
		c.taskAddr[msg.Addr.TaskID] = msg.Addr
		c.mu.Unlock()
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("aether: encode turn: %w", err)
	}
	scope, err := workspacepkg.GetExecutionScope(msg)
	if err != nil {
		return fmt.Errorf("aether: execution scope: %w", err)
	}
	var sendErr error
	if scope != nil && scope.Binding.ToolHostID != c.ToolHostID() {
		access, accessErr := ExecutionBindingAccessRequest(*scope, msg.Addr.TaskID)
		if accessErr != nil {
			return fmt.Errorf("aether: shared execution scope: %w", accessErr)
		}
		sendErr = c.sendCheckedToAgent(payload, access)
	} else {
		sendErr = c.sendToAgent(payload)
	}
	if sendErr != nil {
		return fmt.Errorf("aether: send turn: %w", sendErr)
	}
	c.recordProjection(ctx, msg)
	return nil
}

// Events exposes the decoded stream events.
func (c *Client) Events() <-chan channel.Event { return c.events }

// DroppedEvents counts token deltas shed because the UI could not keep up.
func (c *Client) DroppedEvents() int64 { return c.dropped.Load() }

// Cancel aborts the in-flight turn by sending a cancel control to the agent.
// It reports whether the control was sent, not whether the turn had already
// finished — the agent decides that.
func (c *Client) Cancel(taskID string) bool {
	if taskID == "" {
		return false
	}
	return c.sendControl(taskID, map[string]any{
		"type":    "control",
		"kind":    spec.ControlCancel,
		"task_id": taskID,
	})
}

// Resolve settles a pending tool approval by sending an approve/deny control to
// the agent holding the prompt.
func (c *Client) Resolve(taskID, requestID string, d approval.Decision) bool {
	if requestID == "" {
		return false
	}
	kind := spec.ControlDeny
	if d.Granted {
		kind = spec.ControlApprove
	}
	body := map[string]any{
		"type":       "control",
		"kind":       kind,
		"request_id": requestID,
	}
	if taskID != "" {
		body["task_id"] = taskID
	}
	if d.Scope != "" {
		body["scope"] = d.Scope
	}
	return c.sendControl(taskID, body)
}

// ClearThread tells the agent to drop a thread's stored history, so a clear
// issued in the UI reaches the side that actually owns the transcript.
func (c *Client) ClearThread(threadID string) bool {
	if threadID == "" {
		return false
	}
	return c.sendControlForThread(threadID, "", map[string]any{
		"type": "control",
		"kind": spec.ControlClear,
	})
}

func (c *Client) sendControl(taskID string, body map[string]any) bool {
	c.mu.Lock()
	addr := c.taskAddr[taskID]
	c.mu.Unlock()
	return c.sendControlForAddress(addr, taskID, body)
}

func (c *Client) sendControlForThread(threadID, taskID string, body map[string]any) bool {
	addr := protocol.MessageAddress{
		ThreadID:    threadID,
		TaskID:      taskID,
		UserID:      c.userID,
		RequestID:   c.windowID,
		WorkspaceID: c.sessionWorkspace,
	}
	return c.sendControlForAddress(addr, taskID, body)
}

func (c *Client) sendControlForAddress(addr protocol.MessageAddress, taskID string, body map[string]any) bool {
	addr.TaskID = taskID
	addr.UserID = c.userID
	addr.RequestID = c.windowID
	if addr.WorkspaceID == "" {
		addr.WorkspaceID = c.sessionWorkspace
	}
	payload, err := json.Marshal(map[string]any{
		"id":      "control-" + c.windowID,
		"role":    "user",
		"addr":    addr,
		"content": []any{body},
	})
	if err != nil {
		return false
	}
	return c.sendToAgent(payload) == nil
}

// onMessage decodes an inbound envelope into a stream event for the UI.
func (c *Client) onMessage(_ context.Context, msg *sdk.Message) error {
	if msg == nil || len(msg.Payload) == 0 {
		return nil
	}
	if frame, ok, frameErr := aetherwire.ParseSessionFrame(msg.Payload); ok {
		if frameErr == nil {
			c.handleSessionFrame(frame)
		}
		return nil
	}
	meta, streamEvent, ok, err := aetherwire.ParseStreamEnvelope(msg.Payload)
	if err != nil || !ok {
		// Not a stream envelope (or undecodable): this topic carries other
		// traffic too, so ignore rather than fail the receive loop.
		return nil
	}
	event, ok := c.toChannelEvent(meta, streamEvent)
	if !ok {
		return nil
	}
	if event.Type == channel.EventMessageFinal && event.Message != nil {
		c.recordProjection(context.Background(), *event.Message)
		c.forgetTurn(event.Addr)
	}
	// Structural events must not be dropped — losing a message_finalized or a
	// part_appended corrupts the transcript — so only token deltas are shed
	// when the UI falls behind.
	if event.Type == channel.EventTokenDelta {
		select {
		case c.events <- event:
		default:
			c.dropped.Add(1)
		}
		return nil
	}
	c.events <- event
	return nil
}

func (c *Client) forgetTurn(addr protocol.MessageAddress) {
	if addr.TaskID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := workspaceThreadKey{workspaceID: addr.WorkspaceID, threadID: addr.ThreadID}
	if c.threadTask[key] == addr.TaskID {
		delete(c.threadTask, key)
	}
	delete(c.taskAddr, addr.TaskID)
}

func (c *Client) handleSessionFrame(frame spec.SessionFrame) {
	switch frame.Type {
	case spec.SessionFrameAttachResult:
		result, err := frame.DecodeAttachResult()
		if err != nil || result == nil {
			return
		}
		c.mu.Lock()
		pending := c.pendingAttach[frame.RequestID]
		c.mu.Unlock()
		if pending != nil {
			select {
			case pending <- sessionAttachResponse{result: result}:
			default:
			}
		}
	case spec.SessionFrameError:
		payload, err := frame.DecodeError()
		if err != nil || payload == nil {
			return
		}
		remoteErr := SessionRemoteError{
			RequestID: frame.RequestID,
			Code:      payload.Code,
			Message:   payload.Message,
			Retryable: payload.Retryable,
		}
		c.mu.Lock()
		pending := c.pendingAttach[frame.RequestID]
		c.mu.Unlock()
		if pending != nil {
			select {
			case pending <- sessionAttachResponse{err: &remoteErr}:
			default:
			}
			return
		}
		select {
		case c.sessionErrors <- remoteErr:
		default:
		}
	case spec.SessionFrameEvent:
		event, err := frame.DecodeEvent()
		if err == nil && event != nil {
			c.sessionEvents <- *event
		}
	}
}

// toChannelEvent maps a spec stream event onto the harness event the UIs
// consume, filling in the address the UI needs to attribute it to a thread.
func (c *Client) toChannelEvent(meta aetherwire.TurnMeta, streamEvent spec.StreamEvent) (channel.Event, bool) {
	event := channel.Event{Addr: c.addrFor(meta, streamEvent)}
	switch ev := streamEvent.(type) {
	case spec.MessageStartedEvent:
		message := ev.Message
		event.Type = channel.EventMessageStarted
		event.Message = &message
		event.MessageID = message.ID
	case spec.PartAppendedEvent:
		part := ev.Part
		event.Type = channel.EventPartAppended
		event.MessageID = ev.MessageID
		event.Index = ev.Index
		event.Part = &part
	case spec.TokenDeltaEvent:
		event.Type = channel.EventTokenDelta
		event.MessageID = ev.MessageID
		event.Index = ev.Index
		event.Delta = ev.Text
	case spec.PartUpdatedEvent:
		event.Type = channel.EventPartUpdated
		event.MessageID = ev.MessageID
		event.Index = ev.Index
		event.Patch = ev.Patch
	case spec.MessageFinalizedEvent:
		message := ev.Message
		event.Type = channel.EventMessageFinal
		event.Message = &message
		event.MessageID = message.ID
	default:
		return channel.Event{}, false
	}
	return event, true
}

// addrFor resolves the address for an inbound event: the enclosed message's own
// address when it has one, else the envelope's thread plus the task this client
// last submitted for that thread.
func (c *Client) addrFor(meta aetherwire.TurnMeta, streamEvent spec.StreamEvent) protocol.MessageAddress {
	switch ev := streamEvent.(type) {
	case spec.MessageStartedEvent:
		if ev.Message.Addr.ThreadID != "" {
			return ev.Message.Addr
		}
	case spec.MessageFinalizedEvent:
		if ev.Message.Addr.ThreadID != "" {
			return ev.Message.Addr
		}
	}
	addr := protocol.MessageAddress{
		ThreadID:    meta.ThreadID,
		WorkspaceID: meta.AppWorkspace,
		UserID:      c.userID,
		RequestID:   meta.WindowID,
	}
	c.mu.Lock()
	addr.TaskID = c.threadTask[workspaceThreadKey{workspaceID: meta.AppWorkspace, threadID: meta.ThreadID}]
	c.mu.Unlock()
	return addr
}
