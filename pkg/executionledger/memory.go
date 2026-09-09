// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package executionledger

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/ids"
)

type MemoryStoreConfig struct {
	MaxEvents int
	Now       func() time.Time
	NewID     func() (string, error)
}

type MemoryStore struct {
	maxEvents int
	now       func() time.Time
	newID     func() (string, error)
	mu        sync.Mutex
	states    map[Ref]state
}

func NewMemoryStore(config MemoryStoreConfig) *MemoryStore {
	maxEvents := config.MaxEvents
	if maxEvents <= 0 {
		maxEvents = DefaultMaxEvents
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	newID := config.NewID
	if newID == nil {
		newID = func() (string, error) { return ids.New("exec-") }
	}
	return &MemoryStore{maxEvents: maxEvents, now: now, newID: newID, states: map[Ref]state{}}
}

func (s *MemoryStore) Append(ctx context.Context, ref Ref, operationID string, request AppendRequest) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	eventID, err := s.newID()
	if err != nil {
		return AppendResult{}, fmt.Errorf("executionledger: create event id: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.states[ref]
	if !ok {
		if err := ref.validate(); err != nil {
			return AppendResult{}, err
		}
		current = newState(ref)
	}
	next, result, err := prepareAppend(current, ref, operationID, request, eventID, s.now(), s.maxEvents)
	if err != nil {
		return AppendResult{}, err
	}
	s.states[ref] = next
	return result, nil
}

func (s *MemoryStore) Query(ctx context.Context, ref Ref, query Query) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	if err := ref.validate(); err != nil {
		return Page{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.states[ref]
	if !ok {
		current = newState(ref)
	}
	return queryState(current, ref, query)
}

func (s *MemoryStore) PinnedModel(ctx context.Context, ref Ref) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := ref.validate(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[ref].PinnedModel, nil
}

var _ Store = (*MemoryStore)(nil)
