// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package sessionlog provides the OSS reference implementation of resumable
// session state. Wire contracts live in ecosystem-messaging-spec; this package
// owns only bounded storage, snapshot coordination, and channel adaptation.
package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/ids"
)

const defaultMaxEvents = 512

var (
	ErrInvalidSessionRef = errors.New("sessionlog: invalid session reference")
	ErrSequenceExhausted = errors.New("sessionlog: JSON-safe sequence range exhausted")
)

// Ref is the composite identity for all session-log operations.
type Ref struct {
	WorkspaceID string
	SessionID   string
}

func (r Ref) validate() error {
	if r.WorkspaceID == "" {
		return fmt.Errorf("%w: workspace ID is required", ErrInvalidSessionRef)
	}
	if r.SessionID == "" {
		return fmt.Errorf("%w: session ID is required", ErrInvalidSessionRef)
	}
	return nil
}

// EventStore is the transport-independent append/replay surface used by the
// attach coordinator and recording publisher.
type EventStore interface {
	Append(ctx context.Context, ref Ref, kind spec.SessionEventKind, payload json.RawMessage) (spec.SessionEvent, error)
	Cursor(ctx context.Context, ref Ref) (spec.SessionCursor, error)
	Replay(ctx context.Context, ref Ref, after spec.SessionCursor) (spec.SessionReplayResult, error)
	Projection(ctx context.Context, ref Ref) (spec.MessageState, spec.SessionCursor, error)
	Capture(ctx context.Context, ref Ref, after *spec.SessionCursor) (SessionCapture, error)
	Reset(ctx context.Context, ref Ref) (spec.SessionCursor, error)
}

// SessionCapture is the atomic event-side boundary used by attach. Messages
// contains the materialized stream projection in deterministic start order;
// Replay, when requested, ends at exactly Cursor.
type SessionCapture struct {
	Cursor   spec.SessionCursor
	Messages []spec.ChatMessage
	Replay   *spec.SessionReplayResult
}

// MemoryEventLogConfig configures the bounded in-memory reference store.
type MemoryEventLogConfig struct {
	MaxEvents     int
	NewGeneration func() (string, error)
}

// MemoryEventLog retains a contiguous suffix per (workspace, session) and a
// materialized stream-message projection. It is concurrency-safe within one
// process; durable/distributed backends implement EventStore directly.
type MemoryEventLog struct {
	maxEvents     int
	newGeneration func() (string, error)

	mu       sync.Mutex
	sessions map[Ref]*memorySession
}

type memorySession struct {
	mu         sync.Mutex
	generation string
	sequence   uint64
	events     []spec.SessionEvent
	projection spec.MessageState
	messageIDs []string
}

// NewMemoryEventLog returns an empty bounded event log.
func NewMemoryEventLog(config MemoryEventLogConfig) *MemoryEventLog {
	maxEvents := config.MaxEvents
	if maxEvents <= 0 {
		maxEvents = defaultMaxEvents
	}
	newGeneration := config.NewGeneration
	if newGeneration == nil {
		newGeneration = func() (string, error) { return ids.New("gen-") }
	}
	return &MemoryEventLog{
		maxEvents:     maxEvents,
		newGeneration: newGeneration,
		sessions:      make(map[Ref]*memorySession),
	}
}

// lockSession returns a locked session while holding the index lock long enough
// to prevent Reset from replacing the entry between lookup and acquisition.
// Every caller must unlock session.mu.
func (l *MemoryEventLog) lockSession(ctx context.Context, ref Ref) (*memorySession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ref.validate(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if session := l.sessions[ref]; session != nil {
		session.mu.Lock()
		l.mu.Unlock()
		if err := ctx.Err(); err != nil {
			session.mu.Unlock()
			return nil, err
		}
		return session, nil
	}
	generation, err := l.newGeneration()
	if err != nil {
		l.mu.Unlock()
		return nil, fmt.Errorf("sessionlog: create generation: %w", err)
	}
	if generation == "" {
		l.mu.Unlock()
		return nil, errors.New("sessionlog: generation factory returned an empty ID")
	}
	session := &memorySession{generation: generation, projection: spec.MessageState{}}
	session.mu.Lock()
	l.sessions[ref] = session
	l.mu.Unlock()
	return session, nil
}

// Append assigns the next cursor and retains the event. Chat-stream payloads
// also update the materialized message projection.
func (l *MemoryEventLog) Append(ctx context.Context, ref Ref, kind spec.SessionEventKind, payload json.RawMessage) (spec.SessionEvent, error) {
	if err := ref.validate(); err != nil {
		return spec.SessionEvent{}, err
	}
	if kind == "" {
		return spec.SessionEvent{}, errors.New("sessionlog: event kind is required")
	}
	if len(payload) == 0 || !json.Valid(payload) {
		return spec.SessionEvent{}, errors.New("sessionlog: event payload must be valid JSON")
	}
	var streamEvent spec.StreamEvent
	if kind == spec.SessionEventChatStream {
		decoded, err := spec.DecodeStreamEvent(payload)
		if err != nil {
			return spec.SessionEvent{}, fmt.Errorf("sessionlog: decode chat stream event: %w", err)
		}
		if decoded != nil {
			if err := validateStreamEvent(decoded); err != nil {
				return spec.SessionEvent{}, err
			}
			if err := validateStreamEventRef(decoded, ref); err != nil {
				return spec.SessionEvent{}, err
			}
		}
		streamEvent = decoded
	}
	session, err := l.lockSession(ctx, ref)
	if err != nil {
		return spec.SessionEvent{}, err
	}
	defer session.mu.Unlock()
	if session.sequence >= spec.SessionMaxSequence {
		return spec.SessionEvent{}, ErrSequenceExhausted
	}
	next := session.sequence + 1
	event := spec.SessionEvent{
		ProtocolVersion: spec.SessionProtocolVersion,
		SchemaRevision:  spec.SessionSchemaRevision,
		WorkspaceID:     ref.WorkspaceID,
		SessionID:       ref.SessionID,
		Cursor:          spec.SessionCursor{Generation: session.generation, Sequence: next},
		Kind:            kind,
		Payload:         cloneRaw(payload),
	}
	if err := event.Validate(); err != nil {
		return spec.SessionEvent{}, fmt.Errorf("sessionlog: construct event: %w", err)
	}
	session.sequence = next
	if len(session.events) == l.maxEvents {
		copy(session.events, session.events[1:])
		session.events[len(session.events)-1] = event
	} else {
		session.events = append(session.events, event)
	}
	if streamEvent != nil {
		messageID := streamMessageID(streamEvent)
		if messageID != "" {
			if _, exists := session.projection[messageID]; !exists {
				session.messageIDs = append(session.messageIDs, messageID)
			}
		}
		session.projection = spec.ApplyEvent(session.projection, streamEvent)
		for len(session.messageIDs) > l.maxEvents {
			delete(session.projection, session.messageIDs[0])
			session.messageIDs = session.messageIDs[1:]
		}
	}
	return cloneEvent(event), nil
}

// Cursor returns the current session boundary, creating the initial generation
// at sequence zero when the session has not been observed before.
func (l *MemoryEventLog) Cursor(ctx context.Context, ref Ref) (spec.SessionCursor, error) {
	session, err := l.lockSession(ctx, ref)
	if err != nil {
		return spec.SessionCursor{}, err
	}
	defer session.mu.Unlock()
	return spec.SessionCursor{Generation: session.generation, Sequence: session.sequence}, nil
}

// Replay returns a complete retained interval, a partial retained suffix when a
// gap was evicted, or unavailable when the cursor names another generation or
// lies ahead of the current boundary.
func (l *MemoryEventLog) Replay(ctx context.Context, ref Ref, after spec.SessionCursor) (spec.SessionReplayResult, error) {
	if err := after.Validate(); err != nil {
		return spec.SessionReplayResult{}, err
	}
	session, err := l.lockSession(ctx, ref)
	if err != nil {
		return spec.SessionReplayResult{}, err
	}
	defer session.mu.Unlock()
	return replayLocked(session, after)
}

func replayLocked(session *memorySession, after spec.SessionCursor) (spec.SessionReplayResult, error) {
	through := spec.SessionCursor{Generation: session.generation, Sequence: session.sequence}
	if after.Generation != session.generation || after.Sequence > session.sequence {
		return spec.SessionReplayResult{Status: spec.SessionReplayUnavailable, Through: through}, nil
	}
	if after.Sequence == session.sequence {
		return spec.SessionReplayResult{Status: spec.SessionReplayComplete, Through: through}, nil
	}

	status := spec.SessionReplayComplete
	if len(session.events) > 0 && after.Sequence+1 < session.events[0].Cursor.Sequence {
		status = spec.SessionReplayPartial
	}
	events := make([]spec.SessionEvent, 0, len(session.events))
	for _, event := range session.events {
		if event.Cursor.Sequence > after.Sequence {
			events = append(events, cloneEvent(event))
		}
	}
	result := spec.SessionReplayResult{Status: status, Events: events, Through: through}
	if err := result.Validate(after); err != nil {
		return spec.SessionReplayResult{}, fmt.Errorf("sessionlog: invalid replay result: %w", err)
	}
	return result, nil
}

// Projection returns a copy of the materialized chat-stream message state and
// the exact cursor through which it was reduced.
func (l *MemoryEventLog) Projection(ctx context.Context, ref Ref) (spec.MessageState, spec.SessionCursor, error) {
	session, err := l.lockSession(ctx, ref)
	if err != nil {
		return nil, spec.SessionCursor{}, err
	}
	defer session.mu.Unlock()
	state := cloneProjection(session.projection)
	for id, message := range state {
		message.Addr.WorkspaceID = ref.WorkspaceID
		message.Addr.ThreadID = ref.SessionID
		state[id] = message
	}
	return state, spec.SessionCursor{Generation: session.generation, Sequence: session.sequence}, nil
}

// Capture atomically copies the projection, cursor, and optional replay result
// so attach cannot return a replay boundary that races ahead of its snapshot.
func (l *MemoryEventLog) Capture(ctx context.Context, ref Ref, after *spec.SessionCursor) (SessionCapture, error) {
	if after != nil {
		if err := after.Validate(); err != nil {
			return SessionCapture{}, err
		}
	}
	session, err := l.lockSession(ctx, ref)
	if err != nil {
		return SessionCapture{}, err
	}
	defer session.mu.Unlock()

	capture := SessionCapture{
		Cursor:   spec.SessionCursor{Generation: session.generation, Sequence: session.sequence},
		Messages: make([]spec.ChatMessage, 0, len(session.messageIDs)),
	}
	for _, messageID := range session.messageIDs {
		if message, ok := session.projection[messageID]; ok {
			message = message.Clone()
			message.Addr.WorkspaceID = ref.WorkspaceID
			message.Addr.ThreadID = ref.SessionID
			capture.Messages = append(capture.Messages, message)
		}
	}
	if after != nil {
		replay, err := replayLocked(session, *after)
		if err != nil {
			return SessionCapture{}, err
		}
		capture.Replay = &replay
	}
	return capture, nil
}

// Reset starts a new incomparable generation and drops retained/live state.
func (l *MemoryEventLog) Reset(ctx context.Context, ref Ref) (spec.SessionCursor, error) {
	if err := ctx.Err(); err != nil {
		return spec.SessionCursor{}, err
	}
	if err := ref.validate(); err != nil {
		return spec.SessionCursor{}, err
	}
	l.mu.Lock()
	old := l.sessions[ref]
	if old != nil {
		old.mu.Lock()
	}
	if err := ctx.Err(); err != nil {
		if old != nil {
			old.mu.Unlock()
		}
		l.mu.Unlock()
		return spec.SessionCursor{}, err
	}
	generation, err := l.newGeneration()
	if err != nil {
		if old != nil {
			old.mu.Unlock()
		}
		l.mu.Unlock()
		return spec.SessionCursor{}, fmt.Errorf("sessionlog: create generation: %w", err)
	}
	if generation == "" || old != nil && generation == old.generation {
		if old != nil {
			old.mu.Unlock()
		}
		l.mu.Unlock()
		if generation == "" {
			return spec.SessionCursor{}, errors.New("sessionlog: generation factory returned an empty ID")
		}
		return spec.SessionCursor{}, errors.New("sessionlog: generation factory reused the current ID")
	}
	l.sessions[ref] = &memorySession{generation: generation, projection: spec.MessageState{}}
	if old != nil {
		old.mu.Unlock()
	}
	l.mu.Unlock()
	return spec.SessionCursor{Generation: generation}, nil
}

func cloneProjection(projection spec.MessageState) spec.MessageState {
	state := make(spec.MessageState, len(projection))
	for id, message := range projection {
		state[id] = message.Clone()
	}
	return state
}

func streamMessageID(event spec.StreamEvent) string {
	switch event := event.(type) {
	case spec.MessageStartedEvent:
		return event.Message.ID
	case spec.MessageFinalizedEvent:
		return event.MessageID
	default:
		return ""
	}
}

func validateStreamEventRef(event spec.StreamEvent, ref Ref) error {
	var message *spec.ChatMessage
	switch event := event.(type) {
	case spec.MessageStartedEvent:
		message = &event.Message
	case spec.MessageFinalizedEvent:
		message = &event.Message
	}
	if message == nil {
		return nil
	}
	if message.Addr.WorkspaceID != "" && message.Addr.WorkspaceID != ref.WorkspaceID {
		return errors.New("sessionlog: stream message and event workspace IDs differ")
	}
	if message.Addr.ThreadID != "" && message.Addr.ThreadID != ref.SessionID {
		return errors.New("sessionlog: stream message and event session IDs differ")
	}
	return nil
}

func cloneEvent(event spec.SessionEvent) spec.SessionEvent {
	event.Payload = cloneRaw(event.Payload)
	if event.Extra != nil {
		extra := make(map[string]json.RawMessage, len(event.Extra))
		for key, value := range event.Extra {
			extra[key] = cloneRaw(value)
		}
		event.Extra = extra
	}
	return event
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}
