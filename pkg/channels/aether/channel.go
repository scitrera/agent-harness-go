// Package aether is the Aether transport for the harness: the agent-side half of
// a channel whose two halves are split across a broker instead of a Go channel.
//
// A frontend (TUI/web/ACP/…) publishes a spec ChatMessage to this agent's topic
// and this Channel yields it from FetchTask; the turn's stream events go back to
// whichever client sent the turn. That makes Aether a multiplexer in front of the
// harness: several frontends, on several machines, driving one agent.
//
// This is the OSS transport, so it assumes nothing beyond an Aether gateway: no
// app-server, no task lifecycle, no on-behalf-of authority, no sidecar relay. A
// distribution layers those on by decorating the turn executor, not by forking
// this file.
package aether

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/aetherwire"
	"github.com/scitrera/agent-harness-go/pkg/approval"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

const (
	// defaultImplementation is the agent-topic implementation segment:
	// ag::<workspace>::<implementation>::<specifier>.
	defaultImplementation = "agent-harness"
	// inboxBuffer bounds turns accepted from the gateway before FetchTask
	// consumes them. Mirrors the in-process channels' inbox.
	inboxBuffer = 16
	// sessionAttachBuffer bounds live events retained while an attach snapshot
	// is being assembled and its result is sent.
	sessionAttachBuffer = 512
)

// SessionService is the transport-neutral attach surface implemented by
// sessionlog.Coordinator.
type SessionService interface {
	Attach(ctx context.Context, request spec.SessionAttachRequest) (spec.SessionAttachResult, error)
}

// WorkspaceResolver applies the host's application-level default and
// visibility policy independently from Aether's transport workspace.
type WorkspaceResolver interface {
	ResolveWorkspace(ctx context.Context, requested string) (string, error)
}

// Config configures the agent-side Aether transport.
type Config struct {
	// ServerAddr is the gateway address, e.g. "127.0.0.1:50051". Required.
	ServerAddr string
	// Workspace is the Aether workspace this agent joins.
	Workspace string
	// Implementation and Specifier complete the agent topic. Implementation
	// defaults to "agent-harness"; Specifier distinguishes instances.
	Implementation string
	Specifier      string
	// SourceAgent labels outbound envelopes, e.g. "agent-harness:<host>".
	SourceAgent string
	// AgentName resolves the assistant's display persona, stamped on outbound
	// messages at meta.scitrera.agent_name. Invoked per message so a rename mid
	// session takes effect without a restart. Nil, or "" , omits the stamp.
	AgentName func() string
	// SessionWorkspace is the application-level default used when an attach
	// request omits workspace_id. It may differ from the Aether routing
	// workspace. Empty falls back to Workspace.
	SessionWorkspace  string
	WorkspaceResolver WorkspaceResolver
	// PreferTaskMessageLanes routes turn stream envelopes to
	// tk::<workspace>::<task>::msg when both identities are present. Enable this
	// only when task IDs name real Aether tasks whose recipients are subscribed;
	// the default direct-reply path supports task-less OSS deployments.
	PreferTaskMessageLanes bool

	// Credentials. All optional: an aetherlite gateway in dev mode accepts an
	// unauthenticated connection, which is the zero-setup path.
	APIKey string
	Token  string
	Tenant string

	// TLS. Disabled by default (plaintext), which is what a local dev gateway
	// speaks.
	TLSEnabled            bool
	TLSInsecureSkipVerify bool
	TLSServerName         string
	RootCAs               []byte
	ClientCert            []byte
	ClientKey             []byte
}

// Channel is the agent-side Aether transport. It bridges the SDK's push-based
// OnMessage delivery into the harness's pull-based FetchTask, and publishes a
// turn's stream events back to the client that sent the turn.
type Channel struct {
	client                 *sdk.AgentClient
	sourceAgent            string
	agentNameFn            func() string
	workspace              string
	sessionWorkspace       string
	preferTaskMessageLanes bool
	workspaceResolver      WorkspaceResolver
	assignmentRouter       *TaskAssignmentRouter
	goalContinuations      *AssignedContinuationExecutor

	tasks  chan channel.Inbound
	runErr chan error

	canceller   *turncancel.Canceller
	approvals   *approval.Broker
	clearThread func(addr protocol.MessageAddress) error

	// replyTo maps an in-flight turn's task id to the topic the turn arrived
	// from, so stream events go back to that client. Captured at ingress and
	// dropped when the turn finalizes.
	mu      sync.Mutex
	replyTo map[string]string

	sessionMu          sync.Mutex
	sessionService     SessionService
	sessionSubscribers map[string]map[string]*sessionSubscriber

	closeOnce sync.Once

	// sendMessage sends a CHAT payload to a topic. A seam so egress is testable
	// without a live connection.
	sendMessage func(topic string, payload []byte) error
}

type sessionSubscriber struct {
	mu          sync.Mutex
	requestID   string
	clientID    string
	workspaceID string
	sessionID   string
	topic       string
	ready       bool
	overflow    bool
	pending     []spec.SessionEvent
}

var (
	_ channel.Channel  = (*Channel)(nil)
	_ channel.Enqueuer = (*Channel)(nil)
)

// New constructs (but does not connect) the transport. Call Start to connect and
// begin pumping messages.
func New(cfg Config) (*Channel, error) {
	if cfg.ServerAddr == "" {
		return nil, fmt.Errorf("aether: server address required")
	}
	implementation := cfg.Implementation
	if implementation == "" {
		implementation = defaultImplementation
	}
	opts := sdk.AgentOptions{
		ClientOptions:  sdk.ClientOptions{ServerAddr: cfg.ServerAddr},
		Workspace:      cfg.Workspace,
		Implementation: implementation,
		Specifier:      cfg.Specifier,
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
	if creds := credentials(cfg); len(creds) > 0 {
		opts.Credentials = creds
	}
	client, err := sdk.NewAgentClient(opts)
	if err != nil {
		return nil, fmt.Errorf("aether: new agent client: %w", err)
	}
	c := &Channel{
		client:                 client,
		sourceAgent:            cfg.SourceAgent,
		agentNameFn:            cfg.AgentName,
		workspace:              cfg.Workspace,
		sessionWorkspace:       cfg.SessionWorkspace,
		preferTaskMessageLanes: cfg.PreferTaskMessageLanes,
		workspaceResolver:      cfg.WorkspaceResolver,
		tasks:                  make(chan channel.Inbound, inboxBuffer),
		runErr:                 make(chan error, 1),
		replyTo:                map[string]string{},
		sessionSubscribers:     map[string]map[string]*sessionSubscriber{},
		assignmentRouter:       NewTaskAssignmentRouter(),
	}
	if c.sessionWorkspace == "" {
		c.sessionWorkspace = c.workspace
	}
	c.sendMessage = client.SendChatMessage
	client.OnMessage(c.onMessage)
	client.OnTaskAssignment(c.assignmentRouter.HandleAssignment)
	return c, nil
}

// credentials assembles the optional credential map. Empty (an unauthenticated
// connection) is valid against a dev-mode gateway.
func credentials(cfg Config) map[string]string {
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

// SetCanceller wires the turn canceller so an inbound cancel control aborts the
// in-flight turn. Must be the same instance the runtime uses.
func (c *Channel) SetCanceller(tc *turncancel.Canceller) { c.canceller = tc }

// SetApprovalBroker wires the broker so inbound approve/deny controls resolve an
// in-flight turn's pending tool-approval prompt. Must be the same instance the
// turn runner awaits on.
func (c *Channel) SetApprovalBroker(b *approval.Broker) { c.approvals = b }

// SetThreadClearer wires the handler invoked on an inbound `clear` control to
// drop a thread's persisted/cached history.
func (c *Channel) SetThreadClearer(fn func(addr protocol.MessageAddress) error) { c.clearThread = fn }

// SetSessionService enables framed attach/snapshot/replay requests on the agent
// topic. It may be set after construction, before Start.
func (c *Channel) SetSessionService(service SessionService) {
	c.sessionMu.Lock()
	c.sessionService = service
	c.sessionMu.Unlock()
}

// Topic reports the agent topic this channel receives turns on.
func (c *Channel) Topic() string { return c.client.Topic() }

// Start connects and runs the SDK receive loop in the background. The loop exits
// when ctx is cancelled or the connection drops.
func (c *Channel) Start(ctx context.Context) error {
	if err := c.client.Connect(ctx); err != nil {
		return fmt.Errorf("aether: connect: %w", err)
	}
	go func() { c.runErr <- c.client.Run(ctx) }()
	return nil
}

// Close shuts down the client.
func (c *Channel) Close() error {
	var err error
	c.closeOnce.Do(func() { err = c.client.Close() })
	return err
}

// currentAgentName resolves the display persona for an outbound message.
func (c *Channel) currentAgentName() string {
	if c.agentNameFn == nil {
		return ""
	}
	return c.agentNameFn()
}

// onMessage parses an inbound payload and hands a turn to FetchTask. Control
// messages are applied here and are not turns.
func (c *Channel) onMessage(ctx context.Context, msg *sdk.Message) error {
	if msg == nil || len(msg.Payload) == 0 {
		return nil
	}
	if frame, ok, frameErr := aetherwire.ParseSessionFrame(msg.Payload); ok {
		// SDK message callbacks run on the receive loop. Attach may read Aether KV
		// synchronously, so handling it inline would prevent that same loop from
		// dispatching the correlated KV response and deadlock until timeout.
		go c.handleSessionFrame(context.WithoutCancel(ctx), msg.SourceTopic, frame, frameErr)
		return nil
	}
	chatMsg, ctrl, err := aetherwire.ParseInbound(msg.Payload)
	if err != nil {
		// Non-turn / unparseable payloads are ignored, not fatal: this topic can
		// carry traffic we are not the intended reader of.
		return nil
	}
	if c.workspaceResolver != nil {
		requestedWorkspace := chatMsg.Addr.WorkspaceID
		// Older clients stamped the Aether routing workspace into the logical
		// address. Preserve that path when the host now separates the two by
		// treating the transport workspace as an omitted/default request.
		if requestedWorkspace == c.workspace && c.workspace != c.sessionWorkspace {
			requestedWorkspace = ""
		}
		workspaceID, resolveErr := c.workspaceResolver.ResolveWorkspace(ctx, requestedWorkspace)
		if resolveErr != nil {
			slog.WarnContext(ctx, "aether: inbound workspace unavailable",
				slog.String("workspace", chatMsg.Addr.WorkspaceID))
			return nil
		}
		chatMsg.Addr.WorkspaceID = workspaceID
	}
	if ctrl != nil {
		c.applyControl(ctx, ctrl, chatMsg.Addr)
		return nil
	}
	// Unlike the platform — where the app-server mints a chat task and a turn
	// without one is an identity-less artifact to drop — an OSS frontend may
	// simply not use Aether tasks. A turn with no task id is therefore normal
	// here; mint a local id so cancel and approval correlation still work.
	if chatMsg.Addr.TaskID == "" {
		taskID, idErr := ids.New("task-")
		if idErr != nil {
			return fmt.Errorf("aether: mint task id: %w", idErr)
		}
		chatMsg.Addr.TaskID = taskID
	}
	if msg.SourceTopic != "" {
		c.mu.Lock()
		c.replyTo[chatMsg.Addr.TaskID] = msg.SourceTopic
		c.mu.Unlock()
	}
	select {
	case c.tasks <- channel.Inbound{Addr: chatMsg.Addr, Message: chatMsg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// applyControl handles an in-band control signal (cancel/clear/approve/deny).
func (c *Channel) applyControl(ctx context.Context, ctrl *aetherwire.InboundControl, addr protocol.MessageAddress) {
	switch ctrl.Kind {
	case spec.ControlCancel:
		taskID := ctrl.TaskID
		if taskID == "" {
			taskID = addr.TaskID
		}
		if taskID == "" || c.canceller == nil {
			return
		}
		c.canceller.Cancel(taskID)
	case spec.ControlClear:
		if c.clearThread == nil || addr.ThreadID == "" {
			return
		}
		clearThread := c.clearThread
		go func() {
			if err := clearThread(addr); err != nil {
				slog.WarnContext(ctx, "aether: clear thread history failed",
					slog.String("workspace", addr.WorkspaceID), slog.String("thread", addr.ThreadID), slog.Any("err", err))
			}
		}()
	case spec.ControlApprove, spec.ControlDeny:
		if c.approvals == nil {
			return
		}
		c.approvals.Resolve(addr.TaskID, ctrl.RequestID, approval.Decision{
			Granted: ctrl.Kind == spec.ControlApprove,
			Scope:   ctrl.Scope,
		})
	}
}

// FetchTask blocks until a turn arrives, the receive loop fails, or ctx ends.
func (c *Channel) FetchTask(ctx context.Context) (channel.Inbound, error) {
	select {
	case in := <-c.tasks:
		return in, nil
	case err := <-c.runErr:
		if err != nil {
			return channel.Inbound{}, fmt.Errorf("aether: receive loop: %w", err)
		}
		return channel.Inbound{}, channel.ErrNoTask
	case <-ctx.Done():
		return channel.Inbound{}, ctx.Err()
	}
}

// Enqueue implements channel.Enqueuer: inject a turn locally, without a round
// trip through the gateway. Used for background pushes (a detached sub-agent
// notifying its parent thread), which originate inside this process.
func (c *Channel) Enqueue(ctx context.Context, in channel.Inbound) error {
	select {
	case c.tasks <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishEvent maps a harness stream event onto the wire and sends it to the
// client that submitted the turn.
func (c *Channel) PublishEvent(_ context.Context, event channel.Event) error {
	topic, payload, ok, err := c.streamMessage(event)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := c.sendMessage(topic, payload); err != nil {
		return fmt.Errorf("aether: send stream event: %w", err)
	}
	if event.Type == channel.EventMessageFinal {
		c.mu.Lock()
		delete(c.replyTo, event.Addr.TaskID)
		c.mu.Unlock()
	}
	return nil
}

// streamMessage builds the destination topic and envelope for an event. ok is
// false when the event is not routable (no known reply target) or is not a wire
// event.
func (c *Channel) streamMessage(event channel.Event) (topic string, payload []byte, ok bool, err error) {
	topic = c.streamTopic(event.Addr)
	if topic == "" {
		return "", nil, false, nil
	}
	streamEvent, ok := aetherwire.StreamEventFor(event, c.currentAgentName())
	if !ok {
		return "", nil, false, nil
	}
	meta := aetherwire.TurnMeta{
		AppWorkspace: c.workspaceFor(event.Addr),
		TaskID:       event.Addr.TaskID,
		ThreadID:     event.Addr.ThreadID,
		WindowID:     event.Addr.RequestID,
		SourceAgent:  c.sourceAgent,
	}
	body, err := aetherwire.StreamEnvelope(meta, streamEvent)
	if err != nil {
		return "", nil, false, err
	}
	return topic, body, true, nil
}

func (c *Channel) streamTopic(addr protocol.MessageAddress) string {
	if c.preferTaskMessageLanes && addr.TaskID != "" {
		// Task topics belong to the Aether routing workspace in which the task
		// was created. addr.WorkspaceID is the logical application workspace and
		// remains in the envelope; using it in the broker topic breaks optional
		// multi-workspace deployments that share one Aether worker identity.
		workspaceID := c.workspace
		if workspaceID == "" {
			workspaceID = c.workspaceFor(addr)
		}
		if workspaceID != "" {
			return aetherwire.TaskMessageTopic(workspaceID, addr.TaskID)
		}
	}
	return c.replyTopic(addr)
}

// replyTopic resolves where a turn's events go: the topic the turn arrived from,
// falling back to the originating user session when the address names one. An
// empty result means the event is undeliverable and is dropped.
func (c *Channel) replyTopic(addr protocol.MessageAddress) string {
	c.mu.Lock()
	topic := c.replyTo[addr.TaskID]
	c.mu.Unlock()
	if topic != "" {
		return topic
	}
	if addr.UserID != "" && addr.RequestID != "" {
		return sdk.UserTopic(addr.UserID, addr.RequestID)
	}
	if addr.UserID != "" {
		return sdk.UserBroadcastTopic(addr.UserID)
	}
	return ""
}

// workspaceFor prefers the turn's workspace, falling back to the channel's.
func (c *Channel) workspaceFor(addr protocol.MessageAddress) string {
	if addr.WorkspaceID != "" {
		return addr.WorkspaceID
	}
	return c.workspace
}

func (c *Channel) handleSessionFrame(ctx context.Context, topic string, frame spec.SessionFrame, frameErr error) {
	if frameErr != nil {
		c.sendSessionError(topic, frame.RequestID, "invalid_request", "invalid session request", false)
		return
	}
	if frame.Type != spec.SessionFrameAttachRequest {
		return
	}
	request, err := frame.DecodeAttachRequest()
	if err != nil || request == nil {
		c.sendSessionError(topic, frame.RequestID, "invalid_request", "invalid session attach request", false)
		return
	}
	if topic == "" {
		return
	}
	c.sessionMu.Lock()
	service := c.sessionService
	c.sessionMu.Unlock()
	if service == nil {
		c.sendSessionError(topic, frame.RequestID, "not_supported", "session attachment is not configured", false)
		return
	}
	workspaceID := request.WorkspaceID
	if c.workspaceResolver != nil {
		workspaceID, err = c.workspaceResolver.ResolveWorkspace(ctx, request.WorkspaceID)
		if err != nil {
			c.sendSessionError(topic, frame.RequestID, "workspace_unavailable", "workspace is not visible", false)
			return
		}
	} else if workspaceID == "" {
		workspaceID = c.sessionWorkspace
	}
	subscriber := &sessionSubscriber{
		requestID:   frame.RequestID,
		clientID:    request.ClientID,
		workspaceID: workspaceID,
		sessionID:   request.SessionID,
		topic:       topic,
	}
	c.addSessionSubscriber(subscriber)

	result, err := service.Attach(ctx, *request)
	if err != nil {
		c.removeSessionSubscriber(subscriber)
		slog.WarnContext(ctx, "aether: session attach failed",
			slog.String("workspace", workspaceID), slog.String("session", request.SessionID), slog.Any("err", err))
		c.sendSessionError(topic, frame.RequestID, "attach_failed", "session attach failed", true)
		return
	}
	if result.WorkspaceID != workspaceID {
		c.removeSessionSubscriber(subscriber)
		c.sendSessionError(topic, frame.RequestID, "attach_failed", "session attach resolved an inconsistent workspace", false)
		return
	}
	resultFrame, err := spec.NewSessionFrame(spec.SessionFrameAttachResult, frame.RequestID, result)
	if err != nil {
		c.removeSessionSubscriber(subscriber)
		c.sendSessionError(topic, frame.RequestID, "attach_failed", "session attach failed", true)
		return
	}
	resultPayload, err := aetherwire.SessionFramePayload(resultFrame)
	if err != nil {
		c.removeSessionSubscriber(subscriber)
		c.sendSessionError(topic, frame.RequestID, "attach_failed", "session attach failed", true)
		return
	}
	if err := c.activateSessionSubscriber(subscriber, result.WorkspaceID, resultPayload); err != nil {
		c.removeSessionSubscriber(subscriber)
		slog.WarnContext(ctx, "aether: deliver session attach failed", slog.Any("err", err))
		return
	}
	subscriber.mu.Lock()
	ready := subscriber.ready
	subscriber.mu.Unlock()
	if !ready {
		c.removeSessionSubscriber(subscriber)
		return
	}
	c.replacePriorSessionSubscriber(subscriber)
}

func (c *Channel) addSessionSubscriber(subscriber *sessionSubscriber) {
	c.sessionMu.Lock()
	byRequest := c.sessionSubscribers[subscriber.sessionID]
	if byRequest == nil {
		byRequest = map[string]*sessionSubscriber{}
		c.sessionSubscribers[subscriber.sessionID] = byRequest
	}
	byRequest[subscriber.requestID] = subscriber
	c.sessionMu.Unlock()
}

func (c *Channel) removeSessionSubscriber(subscriber *sessionSubscriber) {
	c.sessionMu.Lock()
	byRequest := c.sessionSubscribers[subscriber.sessionID]
	if byRequest[subscriber.requestID] == subscriber {
		delete(byRequest, subscriber.requestID)
	}
	if len(byRequest) == 0 {
		delete(c.sessionSubscribers, subscriber.sessionID)
	}
	c.sessionMu.Unlock()
}

func (c *Channel) replacePriorSessionSubscriber(current *sessionSubscriber) {
	c.sessionMu.Lock()
	for requestID, subscriber := range c.sessionSubscribers[current.sessionID] {
		if subscriber == current {
			continue
		}
		subscriber.mu.Lock()
		sameClient := subscriber.clientID == current.clientID && subscriber.workspaceID == current.workspaceID
		subscriber.mu.Unlock()
		if sameClient {
			delete(c.sessionSubscribers[current.sessionID], requestID)
		}
	}
	c.sessionMu.Unlock()
}

func (c *Channel) activateSessionSubscriber(subscriber *sessionSubscriber, resolvedWorkspace string, resultPayload []byte) error {
	subscriber.mu.Lock()
	defer subscriber.mu.Unlock()
	subscriber.workspaceID = resolvedWorkspace
	if subscriber.overflow {
		c.sendSessionError(subscriber.topic, subscriber.requestID, "session_gap", "live session events exceeded the attach buffer; attach again", true)
		return nil
	}
	if err := c.sendMessage(subscriber.topic, resultPayload); err != nil {
		return err
	}
	for _, event := range subscriber.pending {
		if event.WorkspaceID != resolvedWorkspace {
			continue
		}
		if err := c.sendSessionEvent(subscriber.topic, event); err != nil {
			return err
		}
	}
	subscriber.pending = nil
	subscriber.ready = true
	return nil
}

// PublishSessionEvent implements sessionlog.SessionEventPublisher. An attach
// result is always delivered before events captured while it was assembled;
// each subscriber's subsequent sends remain cursor ordered.
func (c *Channel) PublishSessionEvent(ctx context.Context, event spec.SessionEvent) error {
	c.sessionMu.Lock()
	subscribers := make([]*sessionSubscriber, 0, len(c.sessionSubscribers[event.SessionID]))
	for _, subscriber := range c.sessionSubscribers[event.SessionID] {
		subscribers = append(subscribers, subscriber)
	}
	c.sessionMu.Unlock()
	for _, subscriber := range subscribers {
		subscriber.mu.Lock()
		if subscriber.workspaceID != event.WorkspaceID {
			subscriber.mu.Unlock()
			continue
		}
		if !subscriber.ready {
			if len(subscriber.pending) < sessionAttachBuffer {
				subscriber.pending = append(subscriber.pending, event)
			} else {
				subscriber.overflow = true
			}
			subscriber.mu.Unlock()
			continue
		}
		err := c.sendSessionEvent(subscriber.topic, event)
		subscriber.mu.Unlock()
		if err != nil {
			c.removeSessionSubscriber(subscriber)
			slog.WarnContext(ctx, "aether: publish live session event failed", slog.Any("err", err))
		}
	}
	return nil
}

func (c *Channel) sendSessionEvent(topic string, event spec.SessionEvent) error {
	frame, err := spec.NewSessionFrame(spec.SessionFrameEvent, "", event)
	if err != nil {
		return err
	}
	payload, err := aetherwire.SessionFramePayload(frame)
	if err != nil {
		return err
	}
	return c.sendMessage(topic, payload)
}

func (c *Channel) sendSessionError(topic, requestID, code, message string, retryable bool) {
	if topic == "" || requestID == "" {
		return
	}
	frame, err := spec.NewSessionFrame(spec.SessionFrameError, requestID, spec.SessionErrorPayload{
		Code: code, Message: message, Retryable: retryable,
	})
	if err != nil {
		return
	}
	payload, err := aetherwire.SessionFramePayload(frame)
	if err != nil {
		return
	}
	if err := c.sendMessage(topic, payload); err != nil {
		slog.Warn("aether: send session error failed", slog.Any("err", err))
	}
}
