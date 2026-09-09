// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package team

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/atomicfile"
)

var graphFileLocks sync.Map

type graphState struct {
	Agents map[AgentID]AgentNode `json:"agents"`
	Edges  []AgentEdge           `json:"edges,omitempty"`
}

type FileGraphStore struct {
	path     string
	lockPath string
	now      func() time.Time
	mu       sync.Mutex
}

func NewFileGraphStore(path string) *FileGraphStore {
	return &FileGraphStore{path: path, lockPath: canonicalGraphPath(path), now: time.Now}
}

func (s *FileGraphStore) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now == nil {
		s.now = time.Now
		return
	}
	s.now = now
}

func (s *FileGraphStore) currentTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func (s *FileGraphStore) update(ctx context.Context, mutate func(*graphState, time.Time) error) error {
	lock := s.stateLock()
	lock.Lock()
	defer lock.Unlock()
	state, err := s.readState(ctx)
	if err != nil {
		return err
	}
	if err := mutate(&state, s.currentTime()); err != nil {
		return err
	}
	return s.writeState(ctx, state)
}

func (s *FileGraphStore) load(ctx context.Context) (graphState, error) {
	lock := s.stateLock()
	lock.Lock()
	defer lock.Unlock()
	return s.readState(ctx)
}

func (s *FileGraphStore) stateLock() *sync.Mutex {
	lockPath := s.lockPath
	if lockPath == "" {
		lockPath = canonicalGraphPath(s.path)
	}
	lock, _ := graphFileLocks.LoadOrStore(lockPath, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (s *FileGraphStore) readState(ctx context.Context) (graphState, error) {
	if err := ctx.Err(); err != nil {
		return graphState{}, err
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return newGraphState(), nil
		}
		return graphState{}, fmt.Errorf("read team graph: %w", err)
	}
	var state graphState
	if err := json.Unmarshal(data, &state); err != nil {
		return graphState{}, fmt.Errorf("%w: %v", ErrCorruptState, err)
	}
	state.init()
	if err := validateGraphState(state); err != nil {
		return graphState{}, err
	}
	return state, nil
}

func (s *FileGraphStore) writeState(ctx context.Context, state graphState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode team graph: %w", err)
	}
	if err := atomicfile.Write(s.path, data, 0o644); err != nil {
		return fmt.Errorf("write team graph: %w", err)
	}
	return nil
}

func newGraphState() graphState {
	return graphState{Agents: map[AgentID]AgentNode{}}
}

func (s *graphState) init() {
	if s.Agents == nil {
		s.Agents = map[AgentID]AgentNode{}
	}
	for id, agent := range s.Agents {
		if agent.Status == "" {
			agent.Status = AgentStatusRunning
		}
		s.Agents[id] = cloneAgent(agent)
	}
}

func canonicalGraphPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	dir, file := filepath.Dir(abs), filepath.Base(abs)
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return filepath.Clean(abs)
	}
	return filepath.Join(resolvedDir, file)
}
