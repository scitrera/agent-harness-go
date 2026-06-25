package web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Session is a chat thread's display metadata for the switcher. Transcripts live
// in the FileStore (keyed by ID); this index only tracks title and ordering.
type Session struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Created int64  `json:"created"` // unix millis
	Updated int64  `json:"updated"` // unix millis
}

const defaultTitle = "New chat"

// Index is a small JSON-file-backed registry of web chat sessions, persisted at
// <stateDir>/web-sessions.json. It is safe for concurrent use.
type Index struct {
	path string
	now  func() time.Time

	mu       sync.Mutex
	sessions map[string]*Session
}

// NewIndex loads (or initializes) the session index under stateDir.
func NewIndex(stateDir string, now func() time.Time) (*Index, error) {
	if now == nil {
		now = time.Now
	}
	x := &Index{
		path:     filepath.Join(stateDir, "web-sessions.json"),
		now:      now,
		sessions: make(map[string]*Session),
	}
	if err := x.load(); err != nil {
		return nil, err
	}
	return x, nil
}

func (x *Index) load() error {
	data, err := os.ReadFile(x.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read sessions: %w", err)
	}
	var list []*Session
	if err := json.Unmarshal(data, &list); err != nil {
		return fmt.Errorf("decode sessions: %w", err)
	}
	for _, s := range list {
		x.sessions[s.ID] = s
	}
	return nil
}

// saveLocked persists the index; the caller holds x.mu.
func (x *Index) saveLocked() error {
	data, err := json.Marshal(x.sortedLocked())
	if err != nil {
		return fmt.Errorf("encode sessions: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(x.path), 0o755); err != nil {
		return fmt.Errorf("mkdir state: %w", err)
	}
	tmp := x.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write sessions: %w", err)
	}
	if err := os.Rename(tmp, x.path); err != nil {
		return fmt.Errorf("commit sessions: %w", err)
	}
	return nil
}

// sortedLocked returns sessions newest-updated first; the caller holds x.mu.
func (x *Index) sortedLocked() []*Session {
	out := make([]*Session, 0, len(x.sessions))
	for _, s := range x.sessions {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Updated > out[j].Updated })
	return out
}

// List returns sessions newest-updated first (value copies).
func (x *Index) List() []Session {
	x.mu.Lock()
	defer x.mu.Unlock()
	sorted := x.sortedLocked()
	out := make([]Session, len(sorted))
	for i, s := range sorted {
		out[i] = *s
	}
	return out
}

// Create makes a new session with a fresh random id.
func (x *Index) Create() (Session, error) {
	id, err := randID("web-")
	if err != nil {
		return Session{}, err
	}
	now := x.now().UnixMilli()
	s := &Session{ID: id, Title: defaultTitle, Created: now, Updated: now}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.sessions[id] = s
	if err := x.saveLocked(); err != nil {
		return Session{}, err
	}
	return *s, nil
}

// Touch bumps the session's updated time and, if its title is still the default,
// derives one from firstUserText. The session is created on demand if missing.
func (x *Index) Touch(id, firstUserText string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	now := x.now().UnixMilli()
	s := x.sessions[id]
	if s == nil {
		s = &Session{ID: id, Title: defaultTitle, Created: now}
		x.sessions[id] = s
	}
	if (s.Title == "" || s.Title == defaultTitle) && strings.TrimSpace(firstUserText) != "" {
		s.Title = title(firstUserText)
	}
	s.Updated = now
	return x.saveLocked()
}

// Delete removes a session from the index (no-op if absent).
func (x *Index) Delete(id string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, ok := x.sessions[id]; !ok {
		return nil
	}
	delete(x.sessions, id)
	return x.saveLocked()
}

// randID returns prefix + a random hex token.
func randID(prefix string) (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	return prefix + hex.EncodeToString(b), nil
}

// title collapses whitespace and truncates on a rune boundary for a session label.
func title(s string) string {
	s = strings.TrimSpace(strings.Join(strings.Fields(s), " "))
	const max = 60
	r := []rune(s)
	if len(r) > max {
		return strings.TrimSpace(string(r[:max])) + "…"
	}
	return s
}
