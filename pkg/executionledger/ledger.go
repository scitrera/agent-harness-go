// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package executionledger provides a bounded, branch-aware record of turn
// execution. It is deliberately separate from transcript/session streaming:
// MemoryLayer (or another HistoryStore) owns what was said, while this ledger
// records operational boundaries and stable references needed to explain how a
// turn ran. Distributed hosts may persist the same contract in Aether KV.
package executionledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	SchemaVersion     = 1
	DefaultMaxEvents  = 512
	DefaultQueryLimit = 20
	MaxQueryLimit     = 100
	MaxCursorBytes    = 8 << 10
	maxIdentityBytes  = 512
	maxErrorBytes     = 2 << 10
	maxBranches       = 1024
)

var (
	ErrInvalid  = errors.New("executionledger: invalid input")
	ErrConflict = errors.New("executionledger: conflict")
	ErrCorrupt  = errors.New("executionledger: corrupt store")
)

type EventType string

const (
	EventTurnStarted          EventType = "turn_started"
	EventUserPromptSubmitted  EventType = "user_prompt_submitted"
	EventModelCallStarted     EventType = "model_call_started"
	EventModelCallFinished    EventType = "model_call_finished"
	EventContextCompacted     EventType = "context_compacted"
	EventModelPinned          EventType = "model_pinned"
	EventGoalReferenced       EventType = "goal_referenced"
	EventRefinementReferenced EventType = "refinement_referenced"
	EventSkillReferenced      EventType = "skill_referenced"
	EventRecoveryMarker       EventType = "recovery_marker"
	EventTurnFinished         EventType = "turn_finished"
)

type Ref struct {
	WorkspaceID string `json:"workspace_id"`
	SessionID   string `json:"session_id"`
}

func (r Ref) validate() error {
	if err := validateIdentity("workspace_id", r.WorkspaceID); err != nil {
		return err
	}
	return validateIdentity("session_id", r.SessionID)
}

// Reference points at an authoritative record without copying it into the
// execution plane. System names the authority (for example goal-store,
// refinement-store, or turnjournal); Kind is its record type/phase.
type Reference struct {
	System string `json:"system"`
	Kind   string `json:"kind"`
	ID     string `json:"id"`
}

type AppendRequest struct {
	Type      EventType  `json:"type"`
	BranchID  string     `json:"branch_id"`
	ParentID  string     `json:"parent_id,omitempty"`
	TaskID    string     `json:"task_id,omitempty"`
	MessageID string     `json:"message_id,omitempty"`
	Iteration int        `json:"iteration,omitempty"`
	Model     string     `json:"model,omitempty"`
	Outcome   string     `json:"outcome,omitempty"`
	Error     string     `json:"error,omitempty"`
	Reference *Reference `json:"reference,omitempty"`
}

type Event struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Sequence      uint64 `json:"sequence"`
	WorkspaceID   string `json:"workspace_id"`
	SessionID     string `json:"session_id"`
	AppendRequest
	CreatedAt string `json:"created_at"`
}

type AppendResult struct {
	Event    Event
	Replayed bool
}

type Query struct {
	Types       []EventType `json:"types,omitempty"`
	BranchID    string      `json:"branch_id,omitempty"`
	TaskID      string      `json:"task_id,omitempty"`
	ReferenceID string      `json:"reference_id,omitempty"`
	Limit       int         `json:"limit,omitempty"`
	PageToken   string      `json:"page_token,omitempty"`
}

type Page struct {
	Events        []Event `json:"events"`
	NextPageToken string  `json:"next_page_token,omitempty"`
	ScannedCount  int     `json:"scanned_count"`
}

type Store interface {
	Append(ctx context.Context, ref Ref, operationID string, request AppendRequest) (AppendResult, error)
	Query(ctx context.Context, ref Ref, query Query) (Page, error)
	PinnedModel(ctx context.Context, ref Ref) (string, error)
}

type entry struct {
	OperationID string `json:"operation_id"`
	RequestHash string `json:"request_hash"`
	Event       Event  `json:"event"`
}

type state struct {
	SchemaVersion  int               `json:"schema_version"`
	WorkspaceID    string            `json:"workspace_id"`
	SessionID      string            `json:"session_id"`
	NextSequence   uint64            `json:"next_sequence"`
	Entries        []entry           `json:"entries"`
	BranchHeads    map[string]string `json:"branch_heads,omitempty"`
	TerminalHeadID string            `json:"terminal_head_id,omitempty"`
	PinnedModel    string            `json:"pinned_model,omitempty"`
}

func newState(ref Ref) state {
	return state{SchemaVersion: SchemaVersion, WorkspaceID: ref.WorkspaceID, SessionID: ref.SessionID, NextSequence: 1, Entries: []entry{}, BranchHeads: map[string]string{}}
}

func normalizeRequest(request AppendRequest) (AppendRequest, error) {
	request.BranchID = strings.TrimSpace(request.BranchID)
	request.ParentID = strings.TrimSpace(request.ParentID)
	request.TaskID = strings.TrimSpace(request.TaskID)
	request.MessageID = strings.TrimSpace(request.MessageID)
	request.Model = strings.TrimSpace(request.Model)
	request.Outcome = strings.TrimSpace(request.Outcome)
	request.Error = strings.TrimSpace(request.Error)
	for name, value := range map[string]string{"branch_id": request.BranchID, "parent_id": request.ParentID, "task_id": request.TaskID, "message_id": request.MessageID, "model": request.Model, "outcome": request.Outcome} {
		if value != "" {
			if err := validateIdentity(name, value); err != nil {
				return AppendRequest{}, err
			}
		}
	}
	if request.BranchID == "" {
		return AppendRequest{}, fmt.Errorf("%w: branch_id is required", ErrInvalid)
	}
	if request.Iteration < 0 {
		return AppendRequest{}, fmt.Errorf("%w: iteration cannot be negative", ErrInvalid)
	}
	if len(request.Error) > maxErrorBytes {
		request.Error = request.Error[:maxErrorBytes]
	}
	if !validEventType(request.Type) {
		return AppendRequest{}, fmt.Errorf("%w: unsupported event type %q", ErrInvalid, request.Type)
	}
	if request.Reference != nil {
		ref := *request.Reference
		ref.System = strings.TrimSpace(ref.System)
		ref.Kind = strings.TrimSpace(ref.Kind)
		ref.ID = strings.TrimSpace(ref.ID)
		for name, value := range map[string]string{"reference system": ref.System, "reference kind": ref.Kind, "reference id": ref.ID} {
			if err := validateIdentity(name, value); err != nil {
				return AppendRequest{}, err
			}
		}
		request.Reference = &ref
	}
	switch request.Type {
	case EventModelPinned, EventModelCallStarted, EventModelCallFinished:
		if request.Model == "" {
			return AppendRequest{}, fmt.Errorf("%w: %s requires a model", ErrInvalid, request.Type)
		}
	case EventGoalReferenced, EventRefinementReferenced, EventSkillReferenced, EventRecoveryMarker:
		if request.Reference == nil {
			return AppendRequest{}, fmt.Errorf("%w: %s requires a reference", ErrInvalid, request.Type)
		}
	}
	return request, nil
}

func validEventType(value EventType) bool {
	switch value {
	case EventTurnStarted, EventUserPromptSubmitted, EventModelCallStarted, EventModelCallFinished,
		EventContextCompacted, EventModelPinned, EventGoalReferenced, EventRefinementReferenced,
		EventSkillReferenced, EventRecoveryMarker, EventTurnFinished:
		return true
	default:
		return false
	}
}

func prepareAppend(current state, ref Ref, operationID string, request AppendRequest, eventID string, createdAt time.Time, maxEvents int) (state, AppendResult, error) {
	if err := ref.validate(); err != nil {
		return state{}, AppendResult{}, err
	}
	operationID = strings.TrimSpace(operationID)
	if err := validateIdentity("operation_id", operationID); err != nil {
		return state{}, AppendResult{}, err
	}
	request, err := normalizeRequest(request)
	if err != nil {
		return state{}, AppendResult{}, err
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return state{}, AppendResult{}, fmt.Errorf("executionledger: encode request: %w", err)
	}
	requestHash := hash(requestBytes)
	for _, existing := range current.Entries {
		if existing.OperationID != operationID {
			continue
		}
		if existing.RequestHash != requestHash {
			return state{}, AppendResult{}, fmt.Errorf("%w: operation %q was used for another event", ErrConflict, operationID)
		}
		return current, AppendResult{Event: cloneEvent(existing.Event), Replayed: true}, nil
	}
	if request.Type == EventTurnStarted {
		if head := current.BranchHeads[request.BranchID]; head != "" {
			return state{}, AppendResult{}, fmt.Errorf("%w: branch %q is already active", ErrConflict, request.BranchID)
		}
	} else if request.ParentID == "" {
		request.ParentID = current.BranchHeads[request.BranchID]
	}
	if request.ParentID == "" {
		request.ParentID = current.TerminalHeadID
	}
	if eventID = strings.TrimSpace(eventID); eventID == "" {
		return state{}, AppendResult{}, fmt.Errorf("%w: event id is required", ErrInvalid)
	}
	if request.ParentID == eventID {
		return state{}, AppendResult{}, fmt.Errorf("%w: event cannot parent itself", ErrInvalid)
	}
	for _, existing := range current.Entries {
		if existing.Event.ID == eventID {
			return state{}, AppendResult{}, fmt.Errorf("%w: event id %q already exists", ErrConflict, eventID)
		}
	}
	if eventID == current.TerminalHeadID {
		return state{}, AppendResult{}, fmt.Errorf("%w: event id %q already names the terminal head", ErrConflict, eventID)
	}
	for _, head := range current.BranchHeads {
		if eventID == head {
			return state{}, AppendResult{}, fmt.Errorf("%w: event id %q already names a branch head", ErrConflict, eventID)
		}
	}
	if maxEvents <= 0 {
		maxEvents = DefaultMaxEvents
	}
	next := cloneState(current)
	if next.NextSequence == 0 {
		next.NextSequence = 1
	}
	event := Event{SchemaVersion: SchemaVersion, ID: eventID, Sequence: next.NextSequence, WorkspaceID: ref.WorkspaceID, SessionID: ref.SessionID, AppendRequest: request, CreatedAt: createdAt.UTC().Format(time.RFC3339Nano)}
	if err := validateEvent(event, ref); err != nil {
		return state{}, AppendResult{}, err
	}
	next.NextSequence++
	next.Entries = append(next.Entries, entry{OperationID: operationID, RequestHash: requestHash, Event: event})
	if len(next.Entries) > maxEvents {
		next.Entries = append([]entry(nil), next.Entries[len(next.Entries)-maxEvents:]...)
	}
	next.BranchHeads[request.BranchID] = event.ID
	if request.Type == EventTurnFinished {
		next.TerminalHeadID = event.ID
		delete(next.BranchHeads, request.BranchID)
	}
	if request.Type == EventModelPinned {
		next.PinnedModel = request.Model
	}
	if len(next.BranchHeads) > maxBranches {
		return state{}, AppendResult{}, fmt.Errorf("%w: too many active branches", ErrConflict)
	}
	return next, AppendResult{Event: cloneEvent(event)}, nil
}

func validateEvent(event Event, ref Ref) error {
	if event.SchemaVersion != SchemaVersion || event.ID == "" || event.Sequence == 0 || event.WorkspaceID != ref.WorkspaceID || event.SessionID != ref.SessionID {
		return fmt.Errorf("%w: invalid event identity or schema", ErrCorrupt)
	}
	if err := validateIdentity("event id", event.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if _, err := time.Parse(time.RFC3339Nano, event.CreatedAt); err != nil {
		return fmt.Errorf("%w: invalid event timestamp", ErrCorrupt)
	}
	if _, err := normalizeRequest(event.AppendRequest); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return nil
}

func validateState(value state, ref Ref, maxEvents int) error {
	if value.SchemaVersion != SchemaVersion || value.WorkspaceID != ref.WorkspaceID || value.SessionID != ref.SessionID || value.NextSequence == 0 {
		return fmt.Errorf("%w: state identity or schema mismatch", ErrCorrupt)
	}
	if len(value.Entries) > maxEvents || len(value.BranchHeads) > maxBranches {
		return fmt.Errorf("%w: state exceeds configured bounds", ErrCorrupt)
	}
	if value.TerminalHeadID != "" {
		if err := validateIdentity("terminal head id", value.TerminalHeadID); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}
	if value.PinnedModel != "" {
		if err := validateIdentity("pinned model", value.PinnedModel); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}
	for branch, head := range value.BranchHeads {
		if err := validateIdentity("branch id", branch); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err := validateIdentity("branch head id", head); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}
	var last uint64
	seenOperations := map[string]struct{}{}
	seenEvents := map[string]struct{}{}
	for _, item := range value.Entries {
		if _, ok := seenOperations[item.OperationID]; ok || item.OperationID == "" || item.RequestHash == "" {
			return fmt.Errorf("%w: duplicate or incomplete operation", ErrCorrupt)
		}
		if _, ok := seenEvents[item.Event.ID]; ok {
			return fmt.Errorf("%w: duplicate event id", ErrCorrupt)
		}
		if err := validateIdentity("operation id", item.OperationID); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err := validateEvent(item.Event, ref); err != nil {
			return err
		}
		if len(item.RequestHash) != sha256.Size*2 {
			return fmt.Errorf("%w: invalid request hash", ErrCorrupt)
		}
		if _, err := hex.DecodeString(item.RequestHash); err != nil {
			return fmt.Errorf("%w: invalid request hash", ErrCorrupt)
		}
		if last != 0 && item.Event.Sequence != last+1 {
			return fmt.Errorf("%w: retained events are not contiguous", ErrCorrupt)
		}
		last = item.Event.Sequence
		seenOperations[item.OperationID] = struct{}{}
		seenEvents[item.Event.ID] = struct{}{}
	}
	if last != 0 && value.NextSequence != last+1 {
		return fmt.Errorf("%w: next sequence does not follow retained events", ErrCorrupt)
	}
	return nil
}

func normalizeQuery(query Query) (Query, error) {
	if query.Limit == 0 {
		query.Limit = DefaultQueryLimit
	}
	if query.Limit < 1 || query.Limit > MaxQueryLimit {
		return Query{}, fmt.Errorf("%w: query limit must be between 1 and %d", ErrInvalid, MaxQueryLimit)
	}
	for name, value := range map[string]*string{"branch_id": &query.BranchID, "task_id": &query.TaskID, "reference_id": &query.ReferenceID} {
		*value = strings.TrimSpace(*value)
		if *value != "" {
			if err := validateIdentity(name, *value); err != nil {
				return Query{}, err
			}
		}
	}
	if len(query.Types) > 16 {
		return Query{}, fmt.Errorf("%w: too many event type filters", ErrInvalid)
	}
	seen := map[EventType]struct{}{}
	for _, kind := range query.Types {
		if !validEventType(kind) {
			return Query{}, fmt.Errorf("%w: unsupported event type %q", ErrInvalid, kind)
		}
		seen[kind] = struct{}{}
	}
	query.Types = query.Types[:0]
	for kind := range seen {
		query.Types = append(query.Types, kind)
	}
	sort.Slice(query.Types, func(i, j int) bool { return query.Types[i] < query.Types[j] })
	if len(query.PageToken) > MaxCursorBytes {
		return Query{}, fmt.Errorf("%w: cursor exceeds %d bytes", ErrInvalid, MaxCursorBytes)
	}
	for _, r := range query.PageToken {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return Query{}, fmt.Errorf("%w: cursor contains whitespace or control characters", ErrInvalid)
		}
	}
	return query, nil
}

func queryState(current state, ref Ref, query Query) (Page, error) {
	query, err := normalizeQuery(query)
	if err != nil {
		return Page{}, err
	}
	start := len(current.Entries) - 1
	scope, err := queryScope(ref, query)
	if err != nil {
		return Page{}, err
	}
	if query.PageToken != "" {
		cursor, err := decodeCursor(query.PageToken, scope)
		if err != nil {
			return Page{}, err
		}
		found := false
		for i := len(current.Entries) - 1; i >= 0; i-- {
			if current.Entries[i].Event.ID == cursor.AfterEventID {
				start = i - 1
				found = true
				break
			}
		}
		if !found {
			return Page{}, cursorError("anchor event is unavailable")
		}
	}
	page := Page{Events: make([]Event, 0, query.Limit)}
	lastInspected := ""
	index := start
	for index >= 0 {
		event := current.Entries[index].Event
		index--
		page.ScannedCount++
		lastInspected = event.ID
		if matches(event, query) {
			page.Events = append(page.Events, cloneEvent(event))
			if len(page.Events) == query.Limit {
				break
			}
		}
	}
	if index >= 0 {
		page.NextPageToken, err = encodeCursor(queryCursor{Version: 1, Scope: scope, AfterEventID: lastInspected})
		if err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

func matches(event Event, query Query) bool {
	if len(query.Types) > 0 {
		found := false
		for _, kind := range query.Types {
			if event.Type == kind {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if query.BranchID != "" && event.BranchID != query.BranchID || query.TaskID != "" && event.TaskID != query.TaskID {
		return false
	}
	if query.ReferenceID != "" && (event.Reference == nil || event.Reference.ID != query.ReferenceID) {
		return false
	}
	return true
}

func validateIdentity(name, value string) error {
	if value = strings.TrimSpace(value); value == "" || len(value) > maxIdentityBytes {
		return fmt.Errorf("%w: %s is required and must not exceed %d bytes", ErrInvalid, name, maxIdentityBytes)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: %s contains control characters", ErrInvalid, name)
		}
	}
	return nil
}

func cloneState(value state) state {
	out := value
	out.Entries = make([]entry, len(value.Entries))
	for i, item := range value.Entries {
		item.Event = cloneEvent(item.Event)
		out.Entries[i] = item
	}
	out.BranchHeads = make(map[string]string, len(value.BranchHeads))
	for key, head := range value.BranchHeads {
		out.BranchHeads[key] = head
	}
	return out
}

func cloneEvent(event Event) Event {
	if event.Reference != nil {
		ref := *event.Reference
		event.Reference = &ref
	}
	return event
}

func hash(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
