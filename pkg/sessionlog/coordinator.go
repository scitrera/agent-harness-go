package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

var ErrWorkspaceUnavailable = errors.New("sessionlog: workspace unavailable")

// WorkspaceResolver resolves an omitted workspace to a default and rejects
// explicit workspaces outside the host's configured visibility.
type WorkspaceResolver interface {
	ResolveWorkspace(ctx context.Context, requested string) (string, error)
}

// StaticWorkspaceResolver is the OSS reference visibility policy. Proprietary
// hosts can replace it with an authority-aware resolver without changing attach.
type StaticWorkspaceResolver struct {
	defaultID string
	visible   map[string]struct{}
}

// NewStaticWorkspaceResolver creates a resolver. defaultID is always visible;
// visible contains any additional explicitly addressable workspaces.
func NewStaticWorkspaceResolver(defaultID string, visible []string) (*StaticWorkspaceResolver, error) {
	defaultID = strings.TrimSpace(defaultID)
	if defaultID == "" {
		return nil, fmt.Errorf("%w: default workspace is required", ErrWorkspaceUnavailable)
	}
	resolver := &StaticWorkspaceResolver{defaultID: defaultID, visible: map[string]struct{}{defaultID: {}}}
	for _, workspaceID := range visible {
		if workspaceID = strings.TrimSpace(workspaceID); workspaceID != "" {
			resolver.visible[workspaceID] = struct{}{}
		}
	}
	return resolver, nil
}

func (r *StaticWorkspaceResolver) ResolveWorkspace(ctx context.Context, requested string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if requested == "" {
		return r.defaultID, nil
	}
	if _, ok := r.visible[requested]; !ok {
		return "", fmt.Errorf("%w: %q", ErrWorkspaceUnavailable, requested)
	}
	return requested, nil
}

// SnapshotStateProvider projects additional capability-gated durable state at
// attach time. A nil provider yields an empty state object.
type SnapshotStateProvider interface {
	SnapshotState(ctx context.Context, workspaceID, sessionID string, cursor spec.SessionCursor, messages []protocol.ChatMessage) (map[string]json.RawMessage, error)
}

// WorkspaceHistoryStore is the attach-safe history surface. Requiring both the
// compatibility and scoped methods prevents a multi-workspace resolver from
// accidentally reading a legacy store keyed only by session ID.
type WorkspaceHistoryStore interface {
	harness.HistoryStore
	harness.WorkspaceHistoryStore
}

// CoordinatorConfig wires the shared protocol to OSS history and event stores.
type CoordinatorConfig struct {
	Workspaces   WorkspaceResolver
	History      WorkspaceHistoryStore
	Events       EventStore
	State        SnapshotStateProvider
	Capabilities []spec.SessionCapability
}

// Coordinator produces validated, deterministic attach results.
type Coordinator struct {
	workspaces   WorkspaceResolver
	history      WorkspaceHistoryStore
	events       EventStore
	state        SnapshotStateProvider
	capabilities []spec.SessionCapability
}

// NewCoordinator validates dependencies and normalizes server capabilities.
func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.Workspaces == nil {
		return nil, errors.New("sessionlog: workspace resolver is required")
	}
	if config.History == nil {
		return nil, errors.New("sessionlog: history store is required")
	}
	if config.Events == nil {
		return nil, errors.New("sessionlog: event store is required")
	}
	capabilities := config.Capabilities
	if capabilities == nil {
		capabilities = spec.DefaultSessionCapabilities()
	}
	return &Coordinator{
		workspaces:   config.Workspaces,
		history:      config.History,
		events:       config.Events,
		state:        config.State,
		capabilities: spec.NormalizeSessionCapabilities(capabilities),
	}, nil
}

// Attach resolves identity, loads the authoritative history snapshot, captures
// the current event boundary, and returns replay when it was requested and
// negotiated.
func (c *Coordinator) Attach(ctx context.Context, request spec.SessionAttachRequest) (spec.SessionAttachResult, error) {
	if err := request.Validate(); err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("sessionlog: attach request: %w", err)
	}
	workspaceID, err := c.workspaces.ResolveWorkspace(ctx, request.WorkspaceID)
	if err != nil {
		return spec.SessionAttachResult{}, err
	}
	ref := Ref{WorkspaceID: workspaceID, SessionID: request.SessionID}
	capabilities := spec.NegotiateSessionCapabilities(request.Capabilities, c.capabilities)
	messages, err := loadHistory(ctx, c.history, ref)
	if err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("sessionlog: snapshot history: %w", err)
	}
	var resumeAfter *spec.SessionCursor
	if request.ResumeAfter != nil && spec.SupportsSessionCapability(capabilities, spec.SessionCapabilityReplay) {
		resumeAfter = request.ResumeAfter
	}
	capture, err := c.events.Capture(ctx, ref, resumeAfter)
	if err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("sessionlog: capture event boundary: %w", err)
	}
	messages, err = normalizeSnapshotMessages(ref, mergeSnapshotMessages(messages, capture.Messages))
	if err != nil {
		return spec.SessionAttachResult{}, err
	}
	state := map[string]json.RawMessage{}
	if c.state != nil {
		if provider, ok := c.state.(NegotiatedSnapshotStateProvider); ok {
			state, err = provider.SnapshotStateForCapabilities(
				ctx, workspaceID, request.SessionID, capture.Cursor, messages, capabilities,
			)
		} else {
			state, err = c.state.SnapshotState(ctx, workspaceID, request.SessionID, capture.Cursor, messages)
		}
		if err != nil {
			return spec.SessionAttachResult{}, fmt.Errorf("sessionlog: snapshot state: %w", err)
		}
		if state == nil {
			state = map[string]json.RawMessage{}
		}
		// Standard capability-owned namespaces use their capability string as
		// their state key. Build the complete provider projection, then omit
		// namespaces this client did not negotiate.
		if provider, ok := c.state.(CapabilityStateProvider); ok {
			for _, capability := range provider.Capabilities() {
				if !spec.SupportsSessionCapability(capabilities, capability) {
					delete(state, string(capability))
				}
			}
		}
	}
	for key, value := range state {
		if key == "" || len(value) == 0 || !json.Valid(value) {
			return spec.SessionAttachResult{}, fmt.Errorf("sessionlog: snapshot state %q is not valid JSON", key)
		}
	}
	result := spec.SessionAttachResult{
		ProtocolVersion: spec.SessionProtocolVersion,
		SchemaRevision:  spec.NegotiateSessionSchemaRevision(request.SchemaRevision, spec.SessionSchemaRevision),
		WorkspaceID:     workspaceID,
		SessionID:       request.SessionID,
		Capabilities:    capabilities,
		Snapshot: spec.SessionSnapshot{
			WorkspaceID: workspaceID,
			SessionID:   request.SessionID,
			Cursor:      capture.Cursor,
			Messages:    cloneMessages(messages),
			State:       cloneState(state),
		},
		Replay: capture.Replay,
	}
	if err := result.Validate(request); err != nil {
		return spec.SessionAttachResult{}, fmt.Errorf("sessionlog: attach result: %w", err)
	}
	return result, nil
}

// ResetSession begins a new event generation for a resolved workspace session.
// Transcript deletion remains the host's responsibility so it can preserve its
// own ordering and error policy.
func (c *Coordinator) ResetSession(ctx context.Context, requestedWorkspace, sessionID string) (spec.SessionCursor, error) {
	workspaceID, err := c.workspaces.ResolveWorkspace(ctx, requestedWorkspace)
	if err != nil {
		return spec.SessionCursor{}, err
	}
	ref := Ref{WorkspaceID: workspaceID, SessionID: sessionID}
	if err := ref.validate(); err != nil {
		return spec.SessionCursor{}, err
	}
	cursor, err := c.events.Reset(ctx, ref)
	if err != nil {
		return spec.SessionCursor{}, fmt.Errorf("sessionlog: reset session: %w", err)
	}
	return cursor, nil
}

// mergeSnapshotMessages adds messages from the deterministic live stream
// projection when durable history does not contain them yet. Durable messages
// win because persistence may add metadata after message_finalized is emitted.
func mergeSnapshotMessages(history, projected []protocol.ChatMessage) []protocol.ChatMessage {
	out := cloneMessages(history)
	positions := make(map[string]int, len(out))
	for i, message := range out {
		if message.ID != "" {
			positions[message.ID] = i
		}
	}
	for _, message := range projected {
		if _, ok := positions[message.ID]; message.ID != "" && ok {
			continue
		}
		if message.ID != "" {
			positions[message.ID] = len(out)
		}
		out = append(out, message.Clone())
	}
	return out
}

func normalizeSnapshotMessages(ref Ref, messages []protocol.ChatMessage) ([]protocol.ChatMessage, error) {
	out := cloneMessages(messages)
	for i := range out {
		address := &out[i].Addr
		if address.WorkspaceID != "" && address.WorkspaceID != ref.WorkspaceID {
			return nil, fmt.Errorf("sessionlog: snapshot message %q belongs to workspace %q, not %q", out[i].ID, address.WorkspaceID, ref.WorkspaceID)
		}
		if address.ThreadID != "" && address.ThreadID != ref.SessionID {
			return nil, fmt.Errorf("sessionlog: snapshot message %q belongs to session %q, not %q", out[i].ID, address.ThreadID, ref.SessionID)
		}
		address.WorkspaceID = ref.WorkspaceID
		address.ThreadID = ref.SessionID
	}
	return out, nil
}

func loadHistory(ctx context.Context, history WorkspaceHistoryStore, ref Ref) ([]protocol.ChatMessage, error) {
	return history.LoadWorkspaceHistory(ctx, ref.WorkspaceID, ref.SessionID)
}

func cloneMessages(messages []protocol.ChatMessage) []protocol.ChatMessage {
	if messages == nil {
		return []protocol.ChatMessage{}
	}
	out := make([]protocol.ChatMessage, len(messages))
	for i, message := range messages {
		out[i] = message.Clone()
	}
	return out
}

func cloneState(state map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(state))
	for key, value := range state {
		out[key] = cloneRaw(value)
	}
	return out
}
