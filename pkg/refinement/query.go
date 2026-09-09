// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package refinement

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
)

const (
	DefaultQueryLimit       = 20
	MaxQueryLimit           = 100
	MaxQueryCursorBytes     = 8 << 10
	MaxQueryTextBytes       = 200
	MaxQueryRefinementBytes = 200
)

// Query selects one newest-first page from the configured refinement audit
// authority. Values within one filter are ORed; distinct filters are ANDed.
// PageToken is authority-owned and opaque to callers.
type Query struct {
	Phases        []Phase        `json:"phases,omitempty"`
	Outcomes      []Outcome      `json:"outcomes,omitempty"`
	Scopes        []Scope        `json:"scopes,omitempty"`
	ResourceKinds []ResourceKind `json:"resource_kinds,omitempty"`
	RefinementID  string         `json:"refinement_id,omitempty"`
	Text          string         `json:"text,omitempty"`
	Limit         int            `json:"limit,omitempty"`
	PageToken     string         `json:"page_token,omitempty"`
}

// Page is one bounded authority page. ScannedCount can exceed len(Records)
// when filters are sparse. ScanTruncated means the backend hit its bounded scan
// ceiling before filling the page; NextPageToken continues from the last record
// inspected without repeating the scan.
type Page struct {
	Records       []Record `json:"records"`
	NextPageToken string   `json:"next_page_token,omitempty"`
	ScannedCount  int      `json:"scanned_count"`
	ScanTruncated bool     `json:"scan_truncated,omitempty"`
}

// NormalizeQuery validates and canonicalizes a query before it reaches a
// backend. Canonical filter ordering makes opaque cursor scopes deterministic.
func NormalizeQuery(query Query) (Query, error) {
	if query.Limit == 0 {
		query.Limit = DefaultQueryLimit
	}
	if query.Limit < 1 || query.Limit > MaxQueryLimit {
		return Query{}, fmt.Errorf("%w: query limit must be between 1 and %d", ErrInvalid, MaxQueryLimit)
	}
	query.RefinementID = strings.TrimSpace(query.RefinementID)
	if len(query.RefinementID) > MaxQueryRefinementBytes {
		return Query{}, fmt.Errorf("%w: refinement id exceeds %d bytes", ErrInvalid, MaxQueryRefinementBytes)
	}
	query.Text = strings.TrimSpace(query.Text)
	if len(query.Text) > MaxQueryTextBytes {
		return Query{}, fmt.Errorf("%w: query text exceeds %d bytes", ErrInvalid, MaxQueryTextBytes)
	}
	if err := ValidateQueryCursor(query.PageToken); err != nil {
		return Query{}, err
	}

	var err error
	query.Phases, err = normalizeFilter(query.Phases, validPhase, "phase")
	if err != nil {
		return Query{}, err
	}
	query.Outcomes, err = normalizeFilter(query.Outcomes, validOutcome, "outcome")
	if err != nil {
		return Query{}, err
	}
	query.Scopes, err = normalizeFilter(query.Scopes, validScope, "scope")
	if err != nil {
		return Query{}, err
	}
	query.ResourceKinds, err = normalizeFilter(query.ResourceKinds, validResourceKind, "resource kind")
	if err != nil {
		return Query{}, err
	}
	return query, nil
}

func ValidateQueryCursor(cursor string) error {
	if cursor == "" {
		return nil
	}
	if len(cursor) > MaxQueryCursorBytes {
		return fmt.Errorf("%w: query cursor exceeds %d bytes", ErrInvalid, MaxQueryCursorBytes)
	}
	for _, r := range cursor {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("%w: query cursor contains whitespace or control characters", ErrInvalid)
		}
	}
	return nil
}

func normalizeFilter[T ~string](values []T, valid func(T) bool, name string) ([]T, error) {
	seen := make(map[T]struct{}, len(values))
	for _, value := range values {
		if !valid(value) {
			return nil, fmt.Errorf("%w: invalid query %s %q", ErrInvalid, name, value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) > 16 {
		return nil, fmt.Errorf("%w: too many query %s filters", ErrInvalid, name)
	}
	out := make([]T, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func validPhase(phase Phase) bool {
	switch phase {
	case PhaseProposal, PhaseDecision, PhaseApplication, PhaseRollback:
		return true
	default:
		return false
	}
}

func validOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeProposed, OutcomeApproved, OutcomeRejected, OutcomeApplied, OutcomePartiallyApplied, OutcomeFailed, OutcomeRolledBack, OutcomeNoOp:
		return true
	default:
		return false
	}
}

func matchesQuery(record Record, query Query) bool {
	if !contains(query.Phases, record.Phase) || !contains(query.Outcomes, record.Outcome) || !contains(query.Scopes, record.Scope) {
		return false
	}
	if query.RefinementID != "" && record.RefinementID != query.RefinementID {
		return false
	}
	if len(query.ResourceKinds) > 0 {
		matched := false
		for _, edit := range record.Edits {
			if contains(query.ResourceKinds, edit.ResourceKind) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if query.Text == "" {
		return true
	}
	needle := strings.ToLower(query.Text)
	values := []string{
		record.ID, record.Key, record.RefinementID, string(record.Phase), string(record.Outcome), string(record.Scope),
		record.Trigger, record.Summary, record.Rationale, record.ExpectedOutcome,
	}
	for _, evidence := range record.Evidence {
		values = append(values, string(evidence.Kind), evidence.Reference, evidence.Description)
	}
	for _, edit := range record.Edits {
		values = append(values, string(edit.Action), string(edit.ResourceKind), edit.ResourceKey, edit.Reason, edit.Error)
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), needle) {
			return true
		}
	}
	return false
}

func contains[T comparable](values []T, value T) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func queryCursorError(detail string) error {
	return fmt.Errorf("%w: invalid query cursor: %s", ErrInvalid, detail)
}
