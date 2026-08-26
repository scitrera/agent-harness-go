package tui

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
)

// ShellPreferenceResolution describes the winning idle-shell response policy.
// Thread overrides user overrides global.
type ShellPreferenceResolution struct {
	Effective bool
	Global    bool
	User      *bool
	Thread    *bool
	Source    string
}

// ShellPreferenceStore holds the persistent per-user and per-thread layers.
// The process-wide default is supplied at resolution time so flags/env remain
// the overall configuration authority.
type ShellPreferenceStore interface {
	Resolve(userID, workspaceID, threadID string, global bool) ShellPreferenceResolution
	SetUser(userID string, value *bool) error
	SetThread(userID, workspaceID, threadID string, value *bool) error
}

type shellPreferenceFile struct {
	Version int                             `json:"version"`
	Users   map[string]*shellUserPreference `json:"users,omitempty"`
}

type shellUserPreference struct {
	TriggerAgent *bool           `json:"trigger_agent,omitempty"`
	Threads      map[string]bool `json:"threads,omitempty"`
}

type fileShellPreferenceStore struct {
	mu   sync.RWMutex
	path string
	data shellPreferenceFile
}

// NewFileShellPreferenceStore opens a persistent shell preference file. A
// missing file is treated as an empty override set and is created on first edit.
func NewFileShellPreferenceStore(path string) (ShellPreferenceStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("shell preference file path is empty")
	}
	store := &fileShellPreferenceStore{path: path, data: shellPreferenceFile{Version: 1, Users: map[string]*shellUserPreference{}}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read shell preferences: %w", err)
	}
	if err := json.Unmarshal(raw, &store.data); err != nil {
		return nil, fmt.Errorf("decode shell preferences: %w", err)
	}
	if store.data.Version != 1 {
		return nil, fmt.Errorf("unsupported shell preference version %d", store.data.Version)
	}
	if store.data.Users == nil {
		store.data.Users = map[string]*shellUserPreference{}
	}
	return store, nil
}

func (s *fileShellPreferenceStore) Resolve(userID, workspaceID, threadID string, global bool) ShellPreferenceResolution {
	resolution := ShellPreferenceResolution{Effective: global, Global: global, Source: "global"}
	if s == nil {
		return resolution
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	user := s.data.Users[strings.TrimSpace(userID)]
	if user == nil {
		return resolution
	}
	if user.TriggerAgent != nil {
		value := *user.TriggerAgent
		resolution.User = &value
		resolution.Effective = value
		resolution.Source = "user"
	}
	if value, ok := user.Threads[shellThreadPreferenceKey(workspaceID, threadID)]; ok {
		resolution.Thread = boolPointer(value)
		resolution.Effective = value
		resolution.Source = "thread"
	}
	return resolution
}

func (s *fileShellPreferenceStore) SetUser(userID string, value *bool) error {
	if s == nil {
		return errors.New("shell preference store is unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	userID = strings.TrimSpace(userID)
	user := s.ensureUser(userID)
	user.TriggerAgent = cloneBoolPointer(value)
	s.pruneUser(userID)
	return s.saveLocked()
}

func (s *fileShellPreferenceStore) SetThread(userID, workspaceID, threadID string, value *bool) error {
	if s == nil {
		return errors.New("shell preference store is unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	userID = strings.TrimSpace(userID)
	user := s.ensureUser(userID)
	key := shellThreadPreferenceKey(workspaceID, threadID)
	if value == nil {
		delete(user.Threads, key)
	} else {
		user.Threads[key] = *value
	}
	s.pruneUser(userID)
	return s.saveLocked()
}

func (s *fileShellPreferenceStore) ensureUser(userID string) *shellUserPreference {
	user := s.data.Users[userID]
	if user == nil {
		user = &shellUserPreference{Threads: map[string]bool{}}
		s.data.Users[userID] = user
	}
	if user.Threads == nil {
		user.Threads = map[string]bool{}
	}
	return user
}

func (s *fileShellPreferenceStore) pruneUser(userID string) {
	user := s.data.Users[userID]
	if user != nil && user.TriggerAgent == nil && len(user.Threads) == 0 {
		delete(s.data.Users, userID)
	}
}

func (s *fileShellPreferenceStore) saveLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode shell preferences: %w", err)
	}
	raw = append(raw, '\n')
	if err := atomicfile.Write(s.path, raw, 0o600); err != nil {
		return fmt.Errorf("save shell preferences: %w", err)
	}
	return nil
}

func shellThreadPreferenceKey(workspaceID, threadID string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	return strconv.Itoa(len(workspaceID)) + ":" + workspaceID + strings.TrimSpace(threadID)
}

func boolPointer(value bool) *bool { return &value }

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	return boolPointer(*value)
}
