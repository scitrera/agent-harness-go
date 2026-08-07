package memorylayer

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

var errNotFound = errors.New("memorylayer: not found")

const defaultTitle = "New chat"

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
	appended map[string]map[string]bool
	// threads caches the thread list. List() is called from UI render paths
	// that cannot block on a network round trip, so it serves this snapshot and
	// mutations refresh it.
	threads map[string]threadindex.Session
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
		appended: map[string]map[string]bool{},
		threads:  map[string]threadindex.Session{},
	}, nil
}

// ─── history ─────────────────────────────────────────────────────────────────

// LoadHistory returns the thread's transcript in chronological order. A thread
// MemoryLayer does not know about is empty rather than an error, matching the
// filesystem store: the first turn of a new thread reads before it writes.
func (s *Store) LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error) {
	if threadID == "" {
		return nil, nil
	}
	raw, err := s.client.getMessages(ctx, threadID)
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
	s.appended[threadID] = seen
	s.mu.Unlock()
	return messages, nil
}

// SaveHistory persists any messages the thread does not already hold. The
// harness hands over the full transcript each turn while MemoryLayer's API
// appends, so this diffs by message id and sends only what is new.
func (s *Store) SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error {
	if threadID == "" || len(messages) == 0 {
		return nil
	}
	s.mu.Lock()
	seen := s.appended[threadID]
	if seen == nil {
		seen = map[string]bool{}
		s.appended[threadID] = seen
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
	if err := s.client.appendMessages(ctx, threadID, payloads); err != nil {
		return err
	}

	s.mu.Lock()
	for _, msg := range fresh {
		if msg.ID != "" {
			s.appended[threadID][msg.ID] = true
		}
	}
	if session, ok := s.threads[threadID]; ok {
		session.Updated = s.now().UnixMilli()
		s.threads[threadID] = session
	}
	s.mu.Unlock()
	return nil
}

// DeleteHistory clears a thread's transcript, message by message. Deleting the
// thread instead would drop it from the switcher, and "clear" means an empty
// conversation rather than a vanished one.
func (s *Store) DeleteHistory(ctx context.Context, threadID string) error {
	if threadID == "" {
		return nil
	}
	s.mu.Lock()
	delete(s.appended, threadID)
	s.mu.Unlock()

	ids, err := s.client.messageIDs(ctx, threadID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil
		}
		return err
	}
	for _, id := range ids {
		if err := s.client.deleteMessage(ctx, threadID, id); err != nil {
			return err
		}
	}
	return nil
}

// ─── thread index ────────────────────────────────────────────────────────────

// Refresh ensures the workspace exists and reloads the thread list. Call it
// once at startup; List serves the cached snapshot afterwards.
func (s *Store) Refresh(ctx context.Context) error {
	if err := s.client.ensureWorkspace(ctx); err != nil {
		return err
	}
	threads, err := s.client.listThreads(ctx, 200)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.threads = make(map[string]threadindex.Session, len(threads))
	for _, t := range threads {
		s.threads[t.ID] = sessionFromThread(t)
	}
	s.mu.Unlock()
	return nil
}

// List returns the known threads, most recently updated first. It serves the
// cached snapshot: it is called from UI render paths that must not block on the
// network.
func (s *Store) List() []threadindex.Session {
	s.mu.Lock()
	out := make([]threadindex.Session, 0, len(s.threads))
	for _, session := range s.threads {
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

// Create makes a new thread. The id comes from the server, which mints its own
// and ignores any the client supplies.
func (s *Store) Create() (threadindex.Session, error) {
	created, err := s.client.createThread(context.Background(), defaultTitle)
	if err != nil {
		return threadindex.Session{}, err
	}
	if created.ID == "" {
		return threadindex.Session{}, errors.New("memorylayer: server returned a thread with no id")
	}
	session := sessionFromThread(created)
	s.mu.Lock()
	s.threads[session.ID] = session
	s.mu.Unlock()
	return session, nil
}

// Touch bumps the thread's updated time and derives an initial title from the
// first user message, mirroring the filesystem index.
func (s *Store) Touch(id, firstUserText string) error {
	if id == "" {
		return nil
	}
	s.mu.Lock()
	session, ok := s.threads[id]
	if !ok {
		session = threadindex.Session{ID: id, Title: defaultTitle, Created: s.now().UnixMilli()}
	}
	needsTitle := (session.Title == "" || session.Title == defaultTitle) && strings.TrimSpace(firstUserText) != ""
	if needsTitle {
		session.Title = titleFrom(firstUserText)
	}
	session.Updated = s.now().UnixMilli()
	s.threads[id] = session
	s.mu.Unlock()

	if !needsTitle {
		return nil
	}
	// Only a title change is worth a round trip; the updated time rides along
	// with the next append.
	if err := s.client.updateThread(context.Background(), id, session.Title); err != nil && !errors.Is(err, errNotFound) {
		return err
	}
	return nil
}

// Rename sets a stable display title.
func (s *Store) Rename(id, titleText string) error {
	if id == "" {
		return nil
	}
	titleText = titleFrom(titleText)
	if titleText == "" {
		titleText = defaultTitle
	}
	if err := s.client.updateThread(context.Background(), id, titleText); err != nil {
		return err
	}
	s.mu.Lock()
	session := s.threads[id]
	session.ID = id
	session.Title = titleText
	session.Updated = s.now().UnixMilli()
	s.threads[id] = session
	s.mu.Unlock()
	return nil
}

// Delete removes the thread and its transcript.
func (s *Store) Delete(id string) error {
	if id == "" {
		return nil
	}
	if err := s.client.deleteThread(context.Background(), id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.threads, id)
	delete(s.appended, id)
	s.mu.Unlock()
	return nil
}

func (s *Store) cacheThread(t thread) {
	if t.ID == "" {
		return
	}
	s.mu.Lock()
	s.threads[t.ID] = sessionFromThread(t)
	s.mu.Unlock()
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
