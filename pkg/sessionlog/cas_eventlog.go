// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	"github.com/scitrera/agent-harness-go/pkg/ids"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	defaultCASEventLogPrefix  = "agent-harness/sessionlog/v1"
	defaultCASEventLogRetries = 32
)

var ErrCASRetryLimit = errors.New("sessionlog: distributed event update retry limit exceeded")

// CASBlobStore remains as an alias for compatibility. New distributed stores
// use the neutral casblob.Store contract directly.
type CASBlobStore = casblob.Store

// CASEventLogConfig configures a bounded distributed event log. Prefix
// namespaces keys inside the supplied blob store; an empty value uses the
// stable agent-harness v1 prefix.
type CASEventLogConfig struct {
	Blobs         CASBlobStore
	Prefix        string
	MaxEvents     int
	MaxRetries    int
	NewGeneration func() (string, error)
}

// CASEventLog persists one complete bounded session state per composite
// workspace/session key. Appends and resets use optimistic compare-and-swap so
// multiple replicas cannot assign the same cursor or overwrite one another.
type CASEventLog struct {
	blobs         CASBlobStore
	prefix        string
	maxEvents     int
	maxRetries    int
	newGeneration func() (string, error)
}

func NewCASEventLog(config CASEventLogConfig) (*CASEventLog, error) {
	if config.Blobs == nil {
		return nil, errors.New("sessionlog: CAS event log blob store is required")
	}
	prefix := strings.Trim(strings.TrimSpace(config.Prefix), "/")
	if prefix == "" {
		prefix = defaultCASEventLogPrefix
	}
	maxEvents := config.MaxEvents
	if maxEvents <= 0 {
		maxEvents = defaultMaxEvents
	}
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultCASEventLogRetries
	}
	newGeneration := config.NewGeneration
	if newGeneration == nil {
		newGeneration = func() (string, error) { return ids.New("gen-") }
	}
	return &CASEventLog{
		blobs:         config.Blobs,
		prefix:        prefix,
		maxEvents:     maxEvents,
		maxRetries:    maxRetries,
		newGeneration: newGeneration,
	}, nil
}

func (l *CASEventLog) key(ref Ref) string {
	return strings.Join([]string{
		l.prefix,
		"workspaces", workspacepkg.PathSegment(ref.WorkspaceID),
		"sessions", workspacepkg.PathSegment(ref.SessionID),
	}, "/")
}

func (l *CASEventLog) read(ctx context.Context, ref Ref) (raw []byte, session *memorySession, found bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	if err := ref.validate(); err != nil {
		return nil, nil, false, err
	}
	key := l.key(ref)
	raw, found, err = l.blobs.Read(ctx, key)
	if err != nil {
		return nil, nil, false, fmt.Errorf("sessionlog: read distributed event log: %w", err)
	}
	if !found {
		return nil, nil, false, nil
	}
	if len(raw) == 0 {
		return nil, nil, false, fmt.Errorf("%w: distributed key %q contains an empty value", ErrCorruptEventLog, key)
	}
	var persisted fileEventLogState
	if err := json.Unmarshal(raw, &persisted); err != nil {
		return nil, nil, false, fmt.Errorf("%w: decode distributed key %q: %v", ErrCorruptEventLog, key, err)
	}
	session, err = decodeEventLogState(ref, persisted, l.maxEvents)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: validate distributed key %q: %v", ErrCorruptEventLog, key, err)
	}
	return append([]byte(nil), raw...), session, true, nil
}

func (l *CASEventLog) memory(ref Ref, session *memorySession) *MemoryEventLog {
	memory := NewMemoryEventLog(MemoryEventLogConfig{
		MaxEvents:     l.maxEvents,
		NewGeneration: l.newGeneration,
	})
	if session != nil {
		memory.sessions[ref] = cloneMemorySession(session)
	}
	return memory
}

func (l *CASEventLog) commit(ctx context.Context, ref Ref, previous []byte, existed bool, session *memorySession) (bool, error) {
	encoded, err := encodeEventLogState(ref, session)
	if err != nil {
		return false, err
	}
	if existed {
		swapped, err := l.blobs.CompareAndSwap(ctx, l.key(ref), previous, encoded)
		if err != nil {
			return false, fmt.Errorf("sessionlog: compare-and-swap distributed event log: %w", err)
		}
		return swapped, nil
	}
	created, err := l.blobs.Create(ctx, l.key(ref), encoded)
	if err != nil {
		return false, fmt.Errorf("sessionlog: create distributed event log: %w", err)
	}
	return created, nil
}

func (l *CASEventLog) readOrCreate(ctx context.Context, ref Ref) (*memorySession, error) {
	for attempt := 0; attempt < l.maxRetries; attempt++ {
		_, session, found, err := l.read(ctx, ref)
		if err != nil {
			return nil, err
		}
		if found {
			return session, nil
		}
		memory := l.memory(ref, nil)
		if _, err := memory.Cursor(ctx, ref); err != nil {
			return nil, err
		}
		session = snapshotMemorySession(memory, ref)
		created, err := l.commit(ctx, ref, nil, false, session)
		if err != nil {
			return nil, err
		}
		if created {
			return session, nil
		}
	}
	return nil, fmt.Errorf("%w: initialize %q after %d attempts", ErrCASRetryLimit, l.key(ref), l.maxRetries)
}

// Append assigns a cursor and atomically commits the resulting bounded state.
func (l *CASEventLog) Append(ctx context.Context, ref Ref, kind spec.SessionEventKind, payload json.RawMessage) (spec.SessionEvent, error) {
	for attempt := 0; attempt < l.maxRetries; attempt++ {
		previous, session, found, err := l.read(ctx, ref)
		if err != nil {
			return spec.SessionEvent{}, err
		}
		memory := l.memory(ref, session)
		event, err := memory.Append(ctx, ref, kind, payload)
		if err != nil {
			return spec.SessionEvent{}, err
		}
		committed, err := l.commit(ctx, ref, previous, found, snapshotMemorySession(memory, ref))
		if err != nil {
			return spec.SessionEvent{}, err
		}
		if committed {
			return event, nil
		}
	}
	return spec.SessionEvent{}, fmt.Errorf("%w: append %q after %d attempts", ErrCASRetryLimit, l.key(ref), l.maxRetries)
}

// Cursor returns the durable session boundary, creating its initial generation
// atomically when this is the first observation.
func (l *CASEventLog) Cursor(ctx context.Context, ref Ref) (spec.SessionCursor, error) {
	session, err := l.readOrCreate(ctx, ref)
	if err != nil {
		return spec.SessionCursor{}, err
	}
	return l.memory(ref, session).Cursor(ctx, ref)
}

func (l *CASEventLog) Replay(ctx context.Context, ref Ref, after spec.SessionCursor) (spec.SessionReplayResult, error) {
	session, err := l.readOrCreate(ctx, ref)
	if err != nil {
		return spec.SessionReplayResult{}, err
	}
	return l.memory(ref, session).Replay(ctx, ref, after)
}

func (l *CASEventLog) Projection(ctx context.Context, ref Ref) (spec.MessageState, spec.SessionCursor, error) {
	session, err := l.readOrCreate(ctx, ref)
	if err != nil {
		return nil, spec.SessionCursor{}, err
	}
	return l.memory(ref, session).Projection(ctx, ref)
}

func (l *CASEventLog) Capture(ctx context.Context, ref Ref, after *spec.SessionCursor) (SessionCapture, error) {
	session, err := l.readOrCreate(ctx, ref)
	if err != nil {
		return SessionCapture{}, err
	}
	return l.memory(ref, session).Capture(ctx, ref, after)
}

// Reset atomically replaces the current state with a new incomparable
// generation. A concurrent append/reset causes a retry against the new value.
func (l *CASEventLog) Reset(ctx context.Context, ref Ref) (spec.SessionCursor, error) {
	for attempt := 0; attempt < l.maxRetries; attempt++ {
		previous, session, found, err := l.read(ctx, ref)
		if err != nil {
			return spec.SessionCursor{}, err
		}
		memory := l.memory(ref, session)
		cursor, err := memory.Reset(ctx, ref)
		if err != nil {
			return spec.SessionCursor{}, err
		}
		committed, err := l.commit(ctx, ref, previous, found, snapshotMemorySession(memory, ref))
		if err != nil {
			return spec.SessionCursor{}, err
		}
		if committed {
			return cursor, nil
		}
	}
	return spec.SessionCursor{}, fmt.Errorf("%w: reset %q after %d attempts", ErrCASRetryLimit, l.key(ref), l.maxRetries)
}

var _ EventStore = (*CASEventLog)(nil)
