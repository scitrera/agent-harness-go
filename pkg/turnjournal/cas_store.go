// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turnjournal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/casblob"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	defaultCASPrefix  = "agent-harness/turn-executions/v2"
	defaultCASRetries = 32
	casIndexVersion   = 1
)

var ErrCASRetryLimit = errors.New("turnjournal: distributed update retry limit exceeded")

type CASStoreConfig struct {
	Blobs      casblob.Store
	Prefix     string
	MaxRetries int
	Now        func() time.Time
}

// CASStore persists the same complete Record over a multi-writer CAS blob
// backend. A small CAS-maintained identity index supports owner-scoped startup
// recovery without assuming the backend can scan keys.
type CASStore struct {
	blobs      casblob.Store
	prefix     string
	maxRetries int
	now        func() time.Time
}

type casExecutionRef struct {
	WorkspaceID   string `json:"workspace_id"`
	TaskID        string `json:"task_id"`
	OwnerIdentity string `json:"owner_identity"`
}

type casExecutionIndex struct {
	SchemaVersion uint32            `json:"schema_version"`
	Refs          []casExecutionRef `json:"refs"`
}

func NewCASStore(config CASStoreConfig) (*CASStore, error) {
	if config.Blobs == nil {
		return nil, errors.New("turnjournal: CAS store requires a blob store")
	}
	prefix := strings.Trim(strings.TrimSpace(config.Prefix), "/")
	if prefix == "" {
		prefix = defaultCASPrefix
	}
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultCASRetries
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &CASStore{blobs: config.Blobs, prefix: prefix, maxRetries: maxRetries, now: now}, nil
}

func (s *CASStore) key(workspaceID, taskID string) string {
	return strings.Join([]string{
		s.prefix,
		"workspaces", workspacepkg.PathSegment(workspaceID),
		"tasks", workspacepkg.PathSegment(taskID),
	}, "/")
}

func (s *CASStore) indexKey() string { return s.prefix + "/index" }

func (s *CASStore) Create(ctx context.Context, record Record) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if record.Revision != 0 {
		return Record{}, fmt.Errorf("%w: create requires zero revision", ErrInvalidRecord)
	}
	now := s.now().UTC()
	record.Schema = Schema
	record.SchemaRevision = SchemaRevision
	record.Revision = 1
	record.CreatedAt = now
	record.UpdatedAt = now
	record.CompletedAt = nil
	record.FailureReason = ""
	if record.Phase == "" {
		record.Phase = PhasePrepared
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	ref := casExecutionRef{WorkspaceID: record.WorkspaceID, TaskID: record.TaskID, OwnerIdentity: record.OwnerIdentity}
	// Index first. A crash may leave a harmless reference to an absent record,
	// but can never leave durable execution state invisible to restart recovery.
	if err := s.ensureIndexed(ctx, ref); err != nil {
		return Record{}, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return Record{}, fmt.Errorf("turnjournal: encode distributed execution: %w", err)
	}
	created, err := s.blobs.Create(ctx, s.key(record.WorkspaceID, record.TaskID), encoded)
	if err != nil {
		return Record{}, fmt.Errorf("turnjournal: create distributed execution: %w", err)
	}
	if !created {
		return Record{}, ErrAlreadyExists
	}
	return cloneRecord(record), nil
}

func (s *CASStore) Get(ctx context.Context, workspaceID, taskID string) (Record, error) {
	_, record, found, err := s.readRecord(ctx, workspaceID, taskID)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, ErrNotFound
	}
	return cloneRecord(record), nil
}

func (s *CASStore) Update(ctx context.Context, record Record, expectedRevision uint64) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if expectedRevision == 0 || record.Revision != expectedRevision {
		return Record{}, ErrConflict
	}
	previous, current, found, err := s.readRecord(ctx, record.WorkspaceID, record.TaskID)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, ErrNotFound
	}
	if current.Revision != expectedRevision {
		return Record{}, ErrConflict
	}
	next := cloneRecord(record)
	next.Schema = Schema
	next.SchemaRevision = SchemaRevision
	next.Revision = expectedRevision + 1
	next.CreatedAt = current.CreatedAt
	next.UpdatedAt = s.now().UTC()
	if next.UpdatedAt.Before(current.UpdatedAt) {
		next.UpdatedAt = current.UpdatedAt
	}
	if next.Terminal() {
		completed := next.UpdatedAt
		next.CompletedAt = &completed
	} else {
		next.CompletedAt = nil
		if next.Phase != PhaseFailing && next.Phase != PhaseInterrupting {
			next.FailureReason = ""
		}
	}
	if err := validateTransition(current, next); err != nil {
		return Record{}, err
	}
	if err := next.Validate(); err != nil {
		return Record{}, err
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return Record{}, fmt.Errorf("turnjournal: encode distributed execution: %w", err)
	}
	swapped, err := s.blobs.CompareAndSwap(ctx, s.key(next.WorkspaceID, next.TaskID), previous, encoded)
	if err != nil {
		return Record{}, fmt.Errorf("turnjournal: compare-and-swap distributed execution: %w", err)
	}
	if !swapped {
		return Record{}, ErrConflict
	}
	return cloneRecord(next), nil
}

func (s *CASStore) ListActive(ctx context.Context, ownerIdentity string) ([]Record, error) {
	if err := validateIdentifier("owner_identity", ownerIdentity); err != nil {
		return nil, err
	}
	_, index, _, err := s.readIndex(ctx)
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, len(index.Refs))
	for _, ref := range index.Refs {
		_, record, found, err := s.readRecord(ctx, ref.WorkspaceID, ref.TaskID)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if record.OwnerIdentity != ref.OwnerIdentity {
			return nil, fmt.Errorf("%w: distributed index owner does not match record", ErrCorruptStore)
		}
		if ref.OwnerIdentity == ownerIdentity && !record.Terminal() {
			records = append(records, cloneRecord(record))
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].WorkspaceID != records[j].WorkspaceID {
			return records[i].WorkspaceID < records[j].WorkspaceID
		}
		if records[i].SessionID != records[j].SessionID {
			return records[i].SessionID < records[j].SessionID
		}
		return records[i].TaskID < records[j].TaskID
	})
	return records, nil
}

func (s *CASStore) readRecord(ctx context.Context, workspaceID, taskID string) ([]byte, Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, Record{}, false, err
	}
	if err := validateLookup(workspaceID, taskID); err != nil {
		return nil, Record{}, false, err
	}
	raw, found, err := s.blobs.Read(ctx, s.key(workspaceID, taskID))
	if err != nil {
		return nil, Record{}, false, fmt.Errorf("turnjournal: read distributed execution: %w", err)
	}
	if !found {
		return nil, Record{}, false, nil
	}
	if len(raw) == 0 {
		return nil, Record{}, false, fmt.Errorf("%w: distributed execution is empty", ErrCorruptStore)
	}
	record, err := decodeRecord(raw)
	if err != nil {
		return nil, Record{}, false, fmt.Errorf("%w: decode distributed execution: %v", ErrCorruptStore, err)
	}
	if record.WorkspaceID != workspaceID || record.TaskID != taskID {
		return nil, Record{}, false, fmt.Errorf("%w: distributed execution identity does not match", ErrCorruptStore)
	}
	return append([]byte(nil), raw...), record, true, nil
}

func compareExecutionRef(left, right casExecutionRef) int {
	if left.WorkspaceID < right.WorkspaceID {
		return -1
	}
	if left.WorkspaceID > right.WorkspaceID {
		return 1
	}
	if left.TaskID < right.TaskID {
		return -1
	}
	if left.TaskID > right.TaskID {
		return 1
	}
	return 0
}

func validateExecutionIndex(index casExecutionIndex) error {
	if index.SchemaVersion != casIndexVersion {
		return fmt.Errorf("%w: distributed execution index schema is unsupported", ErrCorruptStore)
	}
	for i, ref := range index.Refs {
		if err := validateLookup(ref.WorkspaceID, ref.TaskID); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptStore, err)
		}
		if err := validateIdentifier("owner_identity", ref.OwnerIdentity); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptStore, err)
		}
		if i > 0 && compareExecutionRef(index.Refs[i-1], ref) >= 0 {
			return fmt.Errorf("%w: distributed execution index is not strictly sorted", ErrCorruptStore)
		}
	}
	return nil
}

func (s *CASStore) readIndex(ctx context.Context) ([]byte, casExecutionIndex, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, casExecutionIndex{}, false, err
	}
	raw, found, err := s.blobs.Read(ctx, s.indexKey())
	if err != nil {
		return nil, casExecutionIndex{}, false, fmt.Errorf("turnjournal: read distributed execution index: %w", err)
	}
	if !found {
		return nil, casExecutionIndex{SchemaVersion: casIndexVersion, Refs: []casExecutionRef{}}, false, nil
	}
	if len(raw) == 0 {
		return nil, casExecutionIndex{}, false, fmt.Errorf("%w: distributed execution index is empty", ErrCorruptStore)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var index casExecutionIndex
	if err := decoder.Decode(&index); err != nil {
		return nil, casExecutionIndex{}, false, fmt.Errorf("%w: decode distributed execution index: %v", ErrCorruptStore, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, casExecutionIndex{}, false, fmt.Errorf("%w: decode distributed execution index: %v", ErrCorruptStore, err)
	}
	if err := validateExecutionIndex(index); err != nil {
		return nil, casExecutionIndex{}, false, err
	}
	return append([]byte(nil), raw...), index, true, nil
}

func (s *CASStore) ensureIndexed(ctx context.Context, ref casExecutionRef) error {
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		previous, index, found, err := s.readIndex(ctx)
		if err != nil {
			return err
		}
		position := sort.Search(len(index.Refs), func(i int) bool { return compareExecutionRef(index.Refs[i], ref) >= 0 })
		if position < len(index.Refs) && compareExecutionRef(index.Refs[position], ref) == 0 {
			if index.Refs[position].OwnerIdentity != ref.OwnerIdentity {
				return fmt.Errorf("%w: indexed execution owner differs", ErrConflict)
			}
			return nil
		}
		index.Refs = append(index.Refs, casExecutionRef{})
		copy(index.Refs[position+1:], index.Refs[position:])
		index.Refs[position] = ref
		encoded, err := json.Marshal(index)
		if err != nil {
			return fmt.Errorf("turnjournal: encode distributed execution index: %w", err)
		}
		var committed bool
		if found {
			committed, err = s.blobs.CompareAndSwap(ctx, s.indexKey(), previous, encoded)
		} else {
			committed, err = s.blobs.Create(ctx, s.indexKey(), encoded)
		}
		if err != nil {
			return fmt.Errorf("turnjournal: update distributed execution index: %w", err)
		}
		if committed {
			return nil
		}
	}
	return fmt.Errorf("%w: index %q after %d attempts", ErrCASRetryLimit, s.indexKey(), s.maxRetries)
}

var _ Store = (*CASStore)(nil)
