package sessionlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const fileEventLogSchemaVersion = 1

var ErrCorruptEventLog = errors.New("sessionlog: corrupt file event log")

var _ EventStore = (*FileEventLog)(nil)

// FileEventLogConfig configures a bounded, atomically persisted event store.
// StateDir is the shared harness state directory; workspace and session
// identities are encoded as child path segments by FileEventLog.
type FileEventLogConfig struct {
	StateDir      string
	MaxEvents     int
	NewGeneration func() (string, error)
}

// FileEventLog persists the same bounded event suffix and materialized message
// projection as MemoryEventLog. Successful mutations are written atomically
// before they become observable to another EventStore call or live publisher.
//
// A FileEventLog is concurrency-safe within one process. The state directory
// must have a single writing process; a later process can reopen it to resume.
type FileEventLog struct {
	stateDir string
	memory   *MemoryEventLog

	mu     sync.Mutex
	loaded map[Ref]bool
}

type fileEventLogState struct {
	SchemaVersion uint32              `json:"schema_version"`
	WorkspaceID   string              `json:"workspace_id"`
	SessionID     string              `json:"session_id"`
	Generation    string              `json:"generation"`
	Sequence      uint64              `json:"sequence"`
	Events        []spec.SessionEvent `json:"events"`
	Messages      []spec.ChatMessage  `json:"messages"`
}

// NewFileEventLog returns a durable event log rooted below config.StateDir.
func NewFileEventLog(config FileEventLogConfig) (*FileEventLog, error) {
	if config.StateDir == "" {
		return nil, errors.New("sessionlog: file event log state directory is required")
	}
	return &FileEventLog{
		stateDir: filepath.Clean(config.StateDir),
		memory: NewMemoryEventLog(MemoryEventLogConfig{
			MaxEvents:     config.MaxEvents,
			NewGeneration: config.NewGeneration,
		}),
		loaded: make(map[Ref]bool),
	}, nil
}

func (l *FileEventLog) eventPath(ref Ref) string {
	return filepath.Join(
		l.stateDir,
		"sessionlog",
		"workspaces",
		workspacepkg.PathSegment(ref.WorkspaceID),
		workspacepkg.PathSegment(ref.SessionID)+".json",
	)
}

func (l *FileEventLog) loadLocked(ref Ref) error {
	if l.loaded[ref] {
		return nil
	}
	data, err := os.ReadFile(l.eventPath(ref))
	if errors.Is(err, os.ErrNotExist) {
		l.loaded[ref] = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("sessionlog: read file event log: %w", err)
	}
	var persisted fileEventLogState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return fmt.Errorf("%w: decode %s: %v", ErrCorruptEventLog, l.eventPath(ref), err)
	}
	session, err := l.decodeState(ref, persisted)
	if err != nil {
		return fmt.Errorf("%w: validate %s: %v", ErrCorruptEventLog, l.eventPath(ref), err)
	}
	l.memory.mu.Lock()
	l.memory.sessions[ref] = session
	l.memory.mu.Unlock()
	l.loaded[ref] = true
	return nil
}

func (l *FileEventLog) decodeState(ref Ref, persisted fileEventLogState) (*memorySession, error) {
	if persisted.SchemaVersion != fileEventLogSchemaVersion {
		return nil, fmt.Errorf("unsupported schema version %d", persisted.SchemaVersion)
	}
	if persisted.WorkspaceID != ref.WorkspaceID || persisted.SessionID != ref.SessionID {
		return nil, errors.New("stored identity does not match requested session")
	}
	if persisted.Generation == "" {
		return nil, errors.New("generation is required")
	}
	if persisted.Sequence > spec.SessionMaxSequence {
		return nil, errors.New("sequence exceeds the JSON-safe maximum")
	}
	if len(persisted.Events) > l.memory.maxEvents {
		return nil, fmt.Errorf("retained event count %d exceeds configured maximum %d", len(persisted.Events), l.memory.maxEvents)
	}
	if uint64(len(persisted.Events)) > persisted.Sequence {
		return nil, errors.New("retained event count exceeds sequence")
	}
	if persisted.Sequence > 0 && len(persisted.Events) == 0 {
		return nil, errors.New("nonzero sequence has no retained event suffix")
	}
	firstSequence := persisted.Sequence - uint64(len(persisted.Events)) + 1
	for i, event := range persisted.Events {
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		if event.WorkspaceID != ref.WorkspaceID || event.SessionID != ref.SessionID {
			return nil, fmt.Errorf("event %d identity does not match stored session", i)
		}
		if event.Cursor.Generation != persisted.Generation || event.Cursor.Sequence != firstSequence+uint64(i) {
			return nil, fmt.Errorf("event %d cursor is not a contiguous retained suffix", i)
		}
		if event.Kind == spec.SessionEventChatStream {
			streamEvent, err := event.DecodeChatStreamEvent()
			if err != nil {
				return nil, fmt.Errorf("event %d chat stream payload: %w", i, err)
			}
			if streamEvent != nil {
				if err := validateStreamEvent(streamEvent); err != nil {
					return nil, fmt.Errorf("event %d: %w", i, err)
				}
				if err := validateStreamEventRef(streamEvent, ref); err != nil {
					return nil, fmt.Errorf("event %d: %w", i, err)
				}
			}
		}
	}
	if len(persisted.Messages) > l.memory.maxEvents {
		return nil, fmt.Errorf("projected message count %d exceeds configured maximum %d", len(persisted.Messages), l.memory.maxEvents)
	}
	session := &memorySession{
		generation: persisted.Generation,
		sequence:   persisted.Sequence,
		events:     make([]spec.SessionEvent, len(persisted.Events)),
		projection: make(spec.MessageState, len(persisted.Messages)),
		messageIDs: make([]string, 0, len(persisted.Messages)),
	}
	for i, event := range persisted.Events {
		session.events[i] = cloneEvent(event)
	}
	for i, message := range persisted.Messages {
		if message.ID == "" {
			return nil, fmt.Errorf("projected message %d has an empty ID", i)
		}
		if _, exists := session.projection[message.ID]; exists {
			return nil, fmt.Errorf("projected message %d duplicates ID %q", i, message.ID)
		}
		if message.Addr.WorkspaceID != "" && message.Addr.WorkspaceID != ref.WorkspaceID {
			return nil, fmt.Errorf("projected message %d workspace does not match stored session", i)
		}
		if message.Addr.ThreadID != "" && message.Addr.ThreadID != ref.SessionID {
			return nil, fmt.Errorf("projected message %d thread does not match stored session", i)
		}
		session.projection[message.ID] = message.Clone()
		session.messageIDs = append(session.messageIDs, message.ID)
	}
	return session, nil
}

func (l *FileEventLog) snapshotLocked(ref Ref) *memorySession {
	l.memory.mu.Lock()
	session := l.memory.sessions[ref]
	if session != nil {
		session.mu.Lock()
	}
	l.memory.mu.Unlock()
	if session == nil {
		return nil
	}
	defer session.mu.Unlock()
	return cloneMemorySession(session)
}

func cloneMemorySession(session *memorySession) *memorySession {
	cloned := &memorySession{
		generation: session.generation,
		sequence:   session.sequence,
		events:     make([]spec.SessionEvent, len(session.events)),
		projection: cloneProjection(session.projection),
		messageIDs: append([]string(nil), session.messageIDs...),
	}
	for i, event := range session.events {
		cloned.events[i] = cloneEvent(event)
	}
	return cloned
}

func (l *FileEventLog) restoreLocked(ref Ref, snapshot *memorySession) {
	l.memory.mu.Lock()
	if snapshot == nil {
		delete(l.memory.sessions, ref)
	} else {
		l.memory.sessions[ref] = snapshot
	}
	l.memory.mu.Unlock()
}

func (l *FileEventLog) persistLocked(ref Ref) error {
	session := l.snapshotLocked(ref)
	if session == nil {
		return errors.New("sessionlog: cannot persist an uninitialized session")
	}
	persisted := fileEventLogState{
		SchemaVersion: fileEventLogSchemaVersion,
		WorkspaceID:   ref.WorkspaceID,
		SessionID:     ref.SessionID,
		Generation:    session.generation,
		Sequence:      session.sequence,
		Events:        make([]spec.SessionEvent, len(session.events)),
		Messages:      make([]spec.ChatMessage, 0, len(session.messageIDs)),
	}
	for i, event := range session.events {
		persisted.Events[i] = cloneEvent(event)
	}
	for _, messageID := range session.messageIDs {
		if message, ok := session.projection[messageID]; ok {
			persisted.Messages = append(persisted.Messages, message.Clone())
		}
	}
	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return fmt.Errorf("sessionlog: encode file event log: %w", err)
	}
	data = append(data, '\n')
	if err := atomicfile.Write(l.eventPath(ref), data, 0o600); err != nil {
		return fmt.Errorf("sessionlog: persist file event log: %w", err)
	}
	return nil
}

func (l *FileEventLog) prepareLocked(ref Ref) (*memorySession, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}
	if err := l.loadLocked(ref); err != nil {
		return nil, err
	}
	return l.snapshotLocked(ref), nil
}

func (l *FileEventLog) persistNewSessionLocked(ref Ref, before *memorySession, opErr error) error {
	if opErr != nil || before != nil || l.snapshotLocked(ref) == nil {
		return opErr
	}
	if err := l.persistLocked(ref); err != nil {
		l.restoreLocked(ref, nil)
		delete(l.loaded, ref)
		return err
	}
	return nil
}

// Append assigns, atomically persists, and returns the next session event.
func (l *FileEventLog) Append(ctx context.Context, ref Ref, kind spec.SessionEventKind, payload json.RawMessage) (spec.SessionEvent, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before, err := l.prepareLocked(ref)
	if err != nil {
		return spec.SessionEvent{}, err
	}
	event, err := l.memory.Append(ctx, ref, kind, payload)
	if err != nil {
		return spec.SessionEvent{}, err
	}
	if err := l.persistLocked(ref); err != nil {
		l.restoreLocked(ref, before)
		if before == nil {
			delete(l.loaded, ref)
		}
		return spec.SessionEvent{}, err
	}
	return event, nil
}

// Cursor returns a restart-stable session boundary, persisting the initial
// generation when the session has not previously been observed.
func (l *FileEventLog) Cursor(ctx context.Context, ref Ref) (spec.SessionCursor, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before, err := l.prepareLocked(ref)
	if err != nil {
		return spec.SessionCursor{}, err
	}
	cursor, err := l.memory.Cursor(ctx, ref)
	if err := l.persistNewSessionLocked(ref, before, err); err != nil {
		return spec.SessionCursor{}, err
	}
	return cursor, nil
}

// Replay returns a restart-stable retained interval after the supplied cursor.
func (l *FileEventLog) Replay(ctx context.Context, ref Ref, after spec.SessionCursor) (spec.SessionReplayResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before, err := l.prepareLocked(ref)
	if err != nil {
		return spec.SessionReplayResult{}, err
	}
	replay, err := l.memory.Replay(ctx, ref, after)
	if err := l.persistNewSessionLocked(ref, before, err); err != nil {
		return spec.SessionReplayResult{}, err
	}
	return replay, nil
}

// Projection returns the persisted materialized chat-stream state.
func (l *FileEventLog) Projection(ctx context.Context, ref Ref) (spec.MessageState, spec.SessionCursor, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before, err := l.prepareLocked(ref)
	if err != nil {
		return nil, spec.SessionCursor{}, err
	}
	projection, cursor, err := l.memory.Projection(ctx, ref)
	if err := l.persistNewSessionLocked(ref, before, err); err != nil {
		return nil, spec.SessionCursor{}, err
	}
	return projection, cursor, nil
}

// Capture atomically copies one persisted attach boundary.
func (l *FileEventLog) Capture(ctx context.Context, ref Ref, after *spec.SessionCursor) (SessionCapture, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before, err := l.prepareLocked(ref)
	if err != nil {
		return SessionCapture{}, err
	}
	capture, err := l.memory.Capture(ctx, ref, after)
	if err := l.persistNewSessionLocked(ref, before, err); err != nil {
		return SessionCapture{}, err
	}
	return capture, nil
}

// Reset atomically persists a new incomparable generation and empty state.
func (l *FileEventLog) Reset(ctx context.Context, ref Ref) (spec.SessionCursor, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before, err := l.prepareLocked(ref)
	if err != nil {
		return spec.SessionCursor{}, err
	}
	cursor, err := l.memory.Reset(ctx, ref)
	if err != nil {
		return spec.SessionCursor{}, err
	}
	if err := l.persistLocked(ref); err != nil {
		l.restoreLocked(ref, before)
		if before == nil {
			delete(l.loaded, ref)
		}
		return spec.SessionCursor{}, err
	}
	return cursor, nil
}
