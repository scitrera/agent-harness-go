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
)

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
	client      *sdk.AgentClient
	sourceAgent string
	agentNameFn func() string
	workspace   string

	tasks  chan channel.Inbound
	runErr chan error

	canceller   *turncancel.Canceller
	approvals   *approval.Broker
	clearThread func(threadID string) error

	// replyTo maps an in-flight turn's task id to the topic the turn arrived
	// from, so stream events go back to that client. Captured at ingress and
	// dropped when the turn finalizes.
	mu      sync.Mutex
	replyTo map[string]string

	closeOnce sync.Once

	// sendMessage sends a CHAT payload to a topic. A seam so egress is testable
	// without a live connection.
	sendMessage func(topic string, payload []byte) error
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
		client:      client,
		sourceAgent: cfg.SourceAgent,
		agentNameFn: cfg.AgentName,
		workspace:   cfg.Workspace,
		tasks:       make(chan channel.Inbound, inboxBuffer),
		runErr:      make(chan error, 1),
		replyTo:     map[string]string{},
	}
	c.sendMessage = client.SendChatMessage
	client.OnMessage(c.onMessage)
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
func (c *Channel) SetThreadClearer(fn func(threadID string) error) { c.clearThread = fn }

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
	chatMsg, ctrl, err := aetherwire.ParseInbound(msg.Payload)
	if err != nil {
		// Non-turn / unparseable payloads are ignored, not fatal: this topic can
		// carry traffic we are not the intended reader of.
		return nil
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
		if err := c.clearThread(addr.ThreadID); err != nil {
			slog.WarnContext(ctx, "aether: clear thread history failed",
				slog.String("thread", addr.ThreadID), slog.Any("err", err))
		}
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
	topic = c.replyTopic(event.Addr)
	if topic == "" {
		return "", nil, false, nil
	}
	var streamEvent spec.StreamEvent
	switch event.Type {
	case channel.EventMessageStarted:
		if event.Message == nil {
			return "", nil, false, nil
		}
		streamEvent = spec.MessageStartedEvent{Message: aetherwire.WithAgentName(*event.Message, c.currentAgentName())}
	case channel.EventPartAppended:
		if event.Part == nil {
			return "", nil, false, nil
		}
		streamEvent = spec.PartAppendedEvent{MessageID: event.MessageID, Index: event.Index, Part: *event.Part}
	case channel.EventTokenDelta:
		streamEvent = spec.TokenDeltaEvent{MessageID: event.MessageID, Index: event.Index, Text: event.Delta}
	case channel.EventPartUpdated:
		streamEvent = spec.PartUpdatedEvent{MessageID: event.MessageID, Index: event.Index, Patch: event.Patch}
	case channel.EventMessageFinal:
		if event.Message == nil {
			return "", nil, false, nil
		}
		final := aetherwire.WithAgentName(*event.Message, c.currentAgentName())
		streamEvent = spec.MessageFinalizedEvent{MessageID: final.ID, Message: final}
	default:
		// EventToolResult / EventToolLifecycle / EventError are not wire events;
		// tool activity reaches clients as part_appended.
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
