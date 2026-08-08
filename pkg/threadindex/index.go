// Package threadindex persists lightweight chat-thread metadata shared by UI
// transports. Transcript content remains in the history store.
package threadindex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
	"github.com/scitrera/agent-harness-go/pkg/ids"
)

const defaultTitle = "New chat"

var _ Store = (*Index)(nil)

// Session is a chat thread's display metadata for switchers.
type Session struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Created int64  `json:"created"`
	Updated int64  `json:"updated"`
}

// Store is the thread registry a UI needs: list the threads, make one, and
// keep its display metadata current. Index is the filesystem implementation;
// a backend that owns threads durably (e.g. MemoryLayer) implements the same
// surface so the UI can be pointed at either.
//
// List returns a snapshot and is called from render paths, so an implementation
// must not block on a network round trip inside it.
type Store interface {
	List() []Session
	Create() (Session, error)
	Touch(id, firstUserText string) error
	Rename(id, titleText string) error
	Delete(id string) error
}

// WorkspaceStore is the additive multi-workspace thread registry surface used
// by hosts that serve more than one logical project from a single backend.
// Legacy UI instances continue to use Store against their selected default.
type WorkspaceStore interface {
	Store
	RefreshWorkspace(ctx context.Context, workspaceID string) error
	ListWorkspace(workspaceID string) []Session
	CreateWorkspaceThread(workspaceID string) (Session, error)
	TouchWorkspaceThread(workspaceID, id, firstUserText string) error
	RenameWorkspaceThread(workspaceID, id, titleText string) error
	DeleteWorkspaceThread(workspaceID, id string) error
}

// Index is a JSON-file-backed registry of chat sessions.
type Index struct {
	path string
	now  func() time.Time

	mu       sync.Mutex
	sessions map[string]*Session
}

// NewIndex loads or initializes the shared chat-thread index under stateDir.
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

func (x *Index) saveLocked() error {
	data, err := json.Marshal(x.sortedLocked())
	if err != nil {
		return fmt.Errorf("encode sessions: %w", err)
	}
	if err := atomicfile.Write(x.path, data, 0o644); err != nil {
		return fmt.Errorf("save sessions: %w", err)
	}
	return nil
}

func (x *Index) sortedLocked() []*Session {
	out := make([]*Session, 0, len(x.sessions))
	for _, s := range x.sessions {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Updated > out[j].Updated })
	return out
}

// List returns sessions newest-updated first.
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
	id, err := NewID("web-")
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

// Touch bumps updated time and derives an initial title from firstUserText.
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

// Rename sets a stable display title for an existing session.
func (x *Index) Rename(id, titleText string) error {
	titleText = title(titleText)
	if titleText == "" {
		titleText = defaultTitle
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	s := x.sessions[id]
	if s == nil {
		now := x.now().UnixMilli()
		s = &Session{ID: id, Created: now}
		x.sessions[id] = s
	}
	s.Title = titleText
	s.Updated = x.now().UnixMilli()
	return x.saveLocked()
}

// Delete drops id from the persisted registry.
func (x *Index) Delete(id string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, ok := x.sessions[id]; !ok {
		return nil
	}
	delete(x.sessions, id)
	return x.saveLocked()
}

// NewID returns prefix plus a random hex token.
func NewID(prefix string) (string, error) {
	return ids.New(prefix)
}

func title(s string) string {
	s = strings.TrimSpace(strings.Join(strings.Fields(s), " "))
	const max = 60
	r := []rune(s)
	if len(r) > max {
		return strings.TrimSpace(string(r[:max])) + "..."
	}
	return s
}
