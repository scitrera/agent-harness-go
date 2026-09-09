// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package memorylayer

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

var errNotFound = errors.New("memorylayer: not found")

const defaultTitle = "New chat"

var (
	_ harness.WorkspaceHistoryStore = (*Store)(nil)
	_ threadindex.WorkspaceStore    = (*Store)(nil)
	_ threadindex.WorkspaceLookup   = (*Store)(nil)
)

// Store keeps chat threads and transcripts in MemoryLayer. It satisfies both
// the history-store seam (LoadHistory/SaveHistory/DeleteHistory) and the
// thread-index seam (List/Create/Touch/Rename/Delete), so a UI wired to it
// shares one conversation with every other process on the same MemoryLayer.
type Store struct {
	client *client
	now    func() time.Time

	mu sync.Mutex
	// appended tracks which message ids a thread already holds, because
	// MemoryLayer's message API appends while the harness's SaveHistory hands
	// over the whole transcript each turn. Without the diff every turn would
	// re-append the entire history.
	appended map[workspaceThreadRef]map[string]bool
	// threads caches the thread list. List() is called from UI render paths
	// that cannot block on a network round trip, so it serves this snapshot and
	// mutations refresh it.
	threads map[string]map[string]threadindex.Session
	// ensured avoids a workspace existence round trip on every transcript save.
	ensured map[string]bool
	lanes   map[workspaceThreadRef]*workspaceThreadLane
}

type workspaceThreadRef struct {
	workspaceID string
	threadID    string
}

type workspaceThreadLane struct {
	mu    sync.Mutex
	users int
}

// New builds a MemoryLayer-backed store.
func New(cfg Config) (*Store, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Store{
		client:   c,
		now:      time.Now,
		appended: map[workspaceThreadRef]map[string]bool{},
		threads:  map[string]map[string]threadindex.Session{},
		ensured:  map[string]bool{},
		lanes:    map[workspaceThreadRef]*workspaceThreadLane{},
	}, nil
}

// ─── history ─────────────────────────────────────────────────────────────────

// LoadHistory returns the thread's transcript in chronological order. A thread
// MemoryLayer does not know about is empty rather than an error, matching the
// filesystem store: the first turn of a new thread reads before it writes.
func (s *Store) LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error) {
	return s.LoadWorkspaceHistory(ctx, s.client.workspace, threadID)
}

// LoadWorkspaceHistory loads a transcript under the composite workspace/thread
// identity. The explicit workspace wins over the configured default.
func (s *Store) LoadWorkspaceHistory(ctx context.Context, workspaceID, threadID string) ([]protocol.ChatMessage, error) {
	if threadID == "" {
		return nil, nil
	}
	workspaceID = s.client.resolveWorkspace(workspaceID)
	if err := s.ensureWorkspace(ctx, workspaceID); err != nil {
		return nil, err
	}
	ref := workspaceThreadRef{workspaceID: workspaceID, threadID: threadID}
	unlock := s.lockThread(ref)
	defer unlock()
	raw, err := s.client.getMessages(ctx, workspaceID, threadID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}
	messages := make([]protocol.ChatMessage, 0, len(raw))
	seen := make(map[string]bool, len(raw))
	for _, item := range raw {
		msg, err := spec.FromMemoryLayerMessage(item)
		if err != nil {
			// One unreadable row must not cost the whole transcript.
			continue
		}
		messages = append(messages, msg)
		if msg.ID != "" {
			seen[msg.ID] = true
		}
	}
	// Loading tells us what the remote already holds, which is what the append
	// diff needs to know.
	s.mu.Lock()
	s.appended[ref] = seen
	s.mu.Unlock()
	return messages, nil
}

// SaveHistory persists any messages the thread does not already hold. The
// harness hands over the full transcript each turn while MemoryLayer's API
// appends, so this diffs by message id and sends only what is new.
func (s *Store) SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error {
	return s.SaveWorkspaceHistory(ctx, s.client.workspace, threadID, messages)
}

// SaveWorkspaceHistory appends only messages not already observed in the
// composite workspace/thread cache.
func (s *Store) SaveWorkspaceHistory(ctx context.Context, workspaceID, threadID string, messages []protocol.ChatMessage) error {
	if threadID == "" || len(messages) == 0 {
		return nil
	}
	workspaceID = s.client.resolveWorkspace(workspaceID)
	if err := s.ensureWorkspace(ctx, workspaceID); err != nil {
		return err
	}
	ref := workspaceThreadRef{workspaceID: workspaceID, threadID: threadID}
	unlock := s.lockThread(ref)
	defer unlock()
	s.mu.Lock()
	seen := s.appended[ref]
	if seen == nil {
		seen = map[string]bool{}
		s.appended[ref] = seen
	}
	fresh := make([]protocol.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		if msg.ID != "" && seen[msg.ID] {
			continue
		}
		fresh = append(fresh, msg)
	}
	s.mu.Unlock()
	if len(fresh) == 0 {
		return nil
	}

	// No explicit thread create: appending to an unknown thread id creates it
	// server-side. Creating it here would not help anyway — the server mints its
	// own id and ignores the one supplied, so an explicit create would strand a
	// second, empty thread under a different id.
	payloads, err := spec.ToMemoryLayerPayloads(fresh)
	if err != nil {
		return err
	}
	if err := s.client.appendMessages(ctx, workspaceID, threadID, payloads); err != nil {
		return err
	}

	s.mu.Lock()
	for _, msg := range fresh {
		if msg.ID != "" {
			s.appended[ref][msg.ID] = true
		}
	}
	workspaceThreads := s.threads[workspaceID]
	if session, ok := workspaceThreads[threadID]; ok {
		session.Updated = s.now().UnixMilli()
		workspaceThreads[threadID] = session
	}
	s.mu.Unlock()
	return nil
}

// DeleteHistory clears a thread's transcript, message by message. Deleting the
// thread instead would drop it from the switcher, and "clear" means an empty
// conversation rather than a vanished one.
func (s *Store) DeleteHistory(ctx context.Context, threadID string) error {
	return s.DeleteWorkspaceHistory(ctx, s.client.workspace, threadID)
}

// DeleteWorkspaceHistory clears only the addressed workspace/thread transcript.
func (s *Store) DeleteWorkspaceHistory(ctx context.Context, workspaceID, threadID string) error {
	if threadID == "" {
		return nil
	}
	workspaceID = s.client.resolveWorkspace(workspaceID)
	ref := workspaceThreadRef{workspaceID: workspaceID, threadID: threadID}
	unlock := s.lockThread(ref)
	defer unlock()
	s.mu.Lock()
	delete(s.appended, ref)
	s.mu.Unlock()

	ids, err := s.client.messageIDs(ctx, workspaceID, threadID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil
		}
		return err
	}
	for _, id := range ids {
		if err := s.client.deleteMessage(ctx, workspaceID, threadID, id); err != nil {
			return err
		}
	}
	return nil
}

// ─── thread index ────────────────────────────────────────────────────────────

// Refresh ensures the workspace exists and reloads the thread list. Call it
// once at startup; List serves the cached snapshot afterwards.
func (s *Store) Refresh(ctx context.Context) error {
	return s.RefreshWorkspace(ctx, s.client.workspace)
}

// RefreshWorkspace ensures and reloads one workspace's thread-list cache.
func (s *Store) RefreshWorkspace(ctx context.Context, workspaceID string) error {
	workspaceID = s.client.resolveWorkspace(workspaceID)
	if err := s.ensureWorkspace(ctx, workspaceID); err != nil {
		return err
	}
	threads, err := s.client.listThreads(ctx, workspaceID, 200)
	if err != nil {
		return err
	}
	s.mu.Lock()
	workspaceThreads := make(map[string]threadindex.Session, len(threads))
	for _, t := range threads {
		workspaceThreads[t.ID] = sessionFromThread(t)
	}
	s.threads[workspaceID] = workspaceThreads
	s.mu.Unlock()
	return nil
}

// List returns the known threads, most recently updated first. It serves the
// cached snapshot: it is called from UI render paths that must not block on the
// network.
func (s *Store) List() []threadindex.Session {
	return s.ListWorkspace(s.client.workspace)
}

// ListWorkspace returns the cached threads for workspaceID without network I/O.
func (s *Store) ListWorkspace(workspaceID string) []threadindex.Session {
	workspaceID = s.client.resolveWorkspace(workspaceID)
	s.mu.Lock()
	workspaceThreads := s.threads[workspaceID]
	out := make([]threadindex.Session, 0, len(workspaceThreads))
	for _, session := range workspaceThreads {
		out = append(out, session)
	}
	s.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Updated != out[j].Updated {
			return out[i].Updated > out[j].Updated
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// LookupWorkspaceThread reads one thread directly from MemoryLayer and updates
// the local list cache when it exists. A missing thread is not an error: a
// scheduled task can be durable in Aether before its first transcript write has
// caused MemoryLayer to create the addressed thread.
func (s *Store) LookupWorkspaceThread(ctx context.Context, workspaceID, id string) (threadindex.Session, bool, error) {
	workspaceID = s.client.resolveWorkspace(workspaceID)
	id = strings.TrimSpace(id)
	if id == "" {
		return threadindex.Session{}, false, errors.New("memorylayer: thread id is required")
	}
	remote, err := s.client.getThread(ctx, workspaceID, id)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return threadindex.Session{}, false, nil
		}
		return threadindex.Session{}, false, err
	}
	if remote.ID != id {
		return threadindex.Session{}, false, errors.New("memorylayer: thread lookup returned a different id")
	}
	session := sessionFromThread(remote)
	s.mu.Lock()
	s.workspaceThreadsLocked(workspaceID)[session.ID] = session
	s.mu.Unlock()
	return session, true, nil
}

// Create makes a new thread. The id comes from the server, which mints its own
// and ignores any the client supplies.
func (s *Store) Create() (threadindex.Session, error) {
	return s.CreateWorkspaceThread(s.client.workspace)
}

// CreateWorkspaceThread creates and caches a server-minted thread in one workspace.
func (s *Store) CreateWorkspaceThread(workspaceID string) (threadindex.Session, error) {
	workspaceID = s.client.resolveWorkspace(workspaceID)
	if err := s.ensureWorkspace(context.Background(), workspaceID); err != nil {
		return threadindex.Session{}, err
	}
	created, err := s.client.createThread(context.Background(), workspaceID, defaultTitle)
	if err != nil {
		return threadindex.Session{}, err
	}
	if created.ID == "" {
		return threadindex.Session{}, errors.New("memorylayer: server returned a thread with no id")
	}
	session := sessionFromThread(created)
	s.mu.Lock()
	s.workspaceThreadsLocked(workspaceID)[session.ID] = session
	s.mu.Unlock()
	return session, nil
}

// Touch bumps the thread's updated time and derives an initial title from the
// first user message, mirroring the filesystem index.
func (s *Store) Touch(id, firstUserText string) error {
	return s.TouchWorkspaceThread(s.client.workspace, id, firstUserText)
}

// TouchWorkspaceThread updates one workspace's cached/server thread metadata.
func (s *Store) TouchWorkspaceThread(workspaceID, id, firstUserText string) error {
	if id == "" {
		return nil
	}
	workspaceID = s.client.resolveWorkspace(workspaceID)
	unlock := s.lockThread(workspaceThreadRef{workspaceID: workspaceID, threadID: id})
	defer unlock()
	s.mu.Lock()
	workspaceThreads := s.workspaceThreadsLocked(workspaceID)
	session, ok := workspaceThreads[id]
	if !ok {
		session = threadindex.Session{ID: id, Title: defaultTitle, Created: s.now().UnixMilli()}
	}
	needsTitle := (session.Title == "" || session.Title == defaultTitle) && strings.TrimSpace(firstUserText) != ""
	if needsTitle {
		session.Title = titleFrom(firstUserText)
	}
	session.Updated = s.now().UnixMilli()
	workspaceThreads[id] = session
	s.mu.Unlock()

	if !needsTitle {
		return nil
	}
	// Only a title change is worth a round trip; the updated time rides along
	// with the next append.
	if err := s.client.updateThread(context.Background(), workspaceID, id, session.Title); err != nil && !errors.Is(err, errNotFound) {
		return err
	}
	return nil
}

// Rename sets a stable display title.
func (s *Store) Rename(id, titleText string) error {
	return s.RenameWorkspaceThread(s.client.workspace, id, titleText)
}

// RenameWorkspaceThread sets a title within one workspace.
func (s *Store) RenameWorkspaceThread(workspaceID, id, titleText string) error {
	if id == "" {
		return nil
	}
	workspaceID = s.client.resolveWorkspace(workspaceID)
	unlock := s.lockThread(workspaceThreadRef{workspaceID: workspaceID, threadID: id})
	defer unlock()
	titleText = titleFrom(titleText)
	if titleText == "" {
		titleText = defaultTitle
	}
	if err := s.client.updateThread(context.Background(), workspaceID, id, titleText); err != nil {
		return err
	}
	s.mu.Lock()
	workspaceThreads := s.workspaceThreadsLocked(workspaceID)
	session := workspaceThreads[id]
	session.ID = id
	session.Title = titleText
	session.Updated = s.now().UnixMilli()
	workspaceThreads[id] = session
	s.mu.Unlock()
	return nil
}

// Delete removes the thread and its transcript.
func (s *Store) Delete(id string) error {
	return s.DeleteWorkspaceThread(s.client.workspace, id)
}

// DeleteWorkspaceThread removes only the addressed workspace's thread.
func (s *Store) DeleteWorkspaceThread(workspaceID, id string) error {
	if id == "" {
		return nil
	}
	workspaceID = s.client.resolveWorkspace(workspaceID)
	ref := workspaceThreadRef{workspaceID: workspaceID, threadID: id}
	unlock := s.lockThread(ref)
	defer unlock()
	if err := s.client.deleteThread(context.Background(), workspaceID, id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.threads[workspaceID], id)
	delete(s.appended, ref)
	s.mu.Unlock()
	return nil
}

func (s *Store) ensureWorkspace(ctx context.Context, workspaceID string) error {
	workspaceID = s.client.resolveWorkspace(workspaceID)
	s.mu.Lock()
	ensured := s.ensured[workspaceID]
	s.mu.Unlock()
	if ensured {
		return nil
	}
	if err := s.client.ensureWorkspace(ctx, workspaceID); err != nil {
		return err
	}
	s.mu.Lock()
	s.ensured[workspaceID] = true
	s.mu.Unlock()
	return nil
}

// EnsureWorkspace makes a logical workspace available for adjacent typed
// resources (for example workspace views) without requiring a chat write first.
func (s *Store) EnsureWorkspace(ctx context.Context, workspaceID string) error {
	return s.ensureWorkspace(ctx, workspaceID)
}

func (s *Store) workspaceThreadsLocked(workspaceID string) map[string]threadindex.Session {
	threads := s.threads[workspaceID]
	if threads == nil {
		threads = make(map[string]threadindex.Session)
		s.threads[workspaceID] = threads
	}
	return threads
}

func (s *Store) lockThread(ref workspaceThreadRef) func() {
	s.mu.Lock()
	lane := s.lanes[ref]
	if lane == nil {
		lane = &workspaceThreadLane{}
		s.lanes[ref] = lane
	}
	lane.users++
	s.mu.Unlock()

	lane.mu.Lock()
	return func() {
		lane.mu.Unlock()
		s.mu.Lock()
		lane.users--
		if lane.users == 0 {
			delete(s.lanes, ref)
		}
		s.mu.Unlock()
	}
}

func sessionFromThread(t thread) threadindex.Session {
	session := threadindex.Session{ID: t.ID, Title: defaultTitle}
	if t.Title != nil && strings.TrimSpace(*t.Title) != "" {
		session.Title = *t.Title
	}
	session.Created = parseMillis(t.CreatedAt)
	session.Updated = parseMillis(t.UpdatedAt)
	if session.Updated == 0 {
		session.Updated = session.Created
	}
	return session
}

// parseMillis converts MemoryLayer's ISO-8601 timestamps to the epoch millis
// the thread index sorts on. An unparseable value sorts as unknown (0) rather
// than failing the whole listing.
func parseMillis(value string) int64 {
	if value == "" {
		return 0
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func titleFrom(s string) string {
	s = strings.TrimSpace(strings.Join(strings.Fields(s), " "))
	const max = 60
	if len(s) > max {
		return s[:max]
	}
	return s
}
