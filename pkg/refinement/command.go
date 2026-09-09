// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package refinement

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const refinementCommandUsage = "Usage: /refinements [--attention] [--phase VALUE[,VALUE...]] [--outcome VALUE[,VALUE...]] [--scope VALUE[,VALUE...]] [--resource VALUE[,VALUE...]] [--refinement ID] [--search TOKEN] [--limit 1..100] [--cursor OPAQUE_TOKEN]"

// AuditAuthorizer is an optional composition policy before operator-command
// audit reads. The MemoryLayer authority still enforces its own workspace ACL
// under the OBO context; this seam covers enterprise command exposure policy.
type AuditAuthorizer interface {
	AuthorizeRefinementAudit(context.Context, protocol.MessageAddress, protocol.ChatMessage) error
}

// RunRefinementAuditCommand implements turn's optional command provider without
// coupling the neutral turn package to refinement storage or rendering.
func (s *Service) RunRefinementAuditCommand(
	ctx context.Context,
	addr protocol.MessageAddress,
	user protocol.ChatMessage,
	args string,
) (string, error) {
	if s == nil || s.Store == nil {
		return "", errors.New("refinement audit is not configured")
	}
	if s.AuditAuthorizer != nil {
		if err := s.AuditAuthorizer.AuthorizeRefinementAudit(ctx, addr, user); err != nil {
			return "", fmt.Errorf("authorize refinement audit: %w", err)
		}
	}
	query, attention, help, err := parseRefinementCommand(args)
	if err != nil {
		return refinementCommandUsage + "\nError: " + err.Error(), nil
	}
	if help {
		return refinementCommandUsage + "\n--attention selects immutable failed and partially_applied outcomes; inspect one lineage with --refinement ID.", nil
	}
	page, err := s.Store.Query(ctx, addr.WorkspaceID, query)
	if err != nil {
		return "", err
	}
	return renderRefinementPage(page, query, attention, s.AuditAuthority), nil
}

func parseRefinementCommand(args string) (Query, bool, bool, error) {
	query := Query{Limit: DefaultQueryLimit}
	fields := strings.Fields(args)
	attention := false
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		key, value, hasValue := strings.Cut(field, "=")
		nextValue := func() (string, error) {
			if hasValue {
				if value == "" {
					return "", fmt.Errorf("%s requires a value", key)
				}
				return value, nil
			}
			if i+1 >= len(fields) {
				return "", fmt.Errorf("%s requires a value", key)
			}
			i++
			return fields[i], nil
		}
		switch key {
		case "--help", "-h":
			if hasValue {
				return Query{}, false, false, errors.New("--help does not accept a value")
			}
			return query, attention, true, nil
		case "--attention", "-a":
			if hasValue {
				return Query{}, false, false, errors.New("--attention does not accept a value")
			}
			attention = true
		case "--phase", "-p":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			for _, item := range splitRefinementFilter(raw) {
				query.Phases = append(query.Phases, Phase(item))
			}
		case "--outcome", "-o":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			for _, item := range splitRefinementFilter(raw) {
				query.Outcomes = append(query.Outcomes, Outcome(item))
			}
		case "--scope", "-s":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			for _, item := range splitRefinementFilter(raw) {
				query.Scopes = append(query.Scopes, Scope(item))
			}
		case "--resource", "-r":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			for _, item := range splitRefinementFilter(raw) {
				query.ResourceKinds = append(query.ResourceKinds, ResourceKind(item))
			}
		case "--refinement", "-i":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			query.RefinementID = raw
		case "--search", "-q":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			query.Text = raw
		case "--limit", "-n":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			limit, err := strconv.Atoi(raw)
			if err != nil {
				return Query{}, false, false, fmt.Errorf("limit must be between 1 and %d", MaxQueryLimit)
			}
			query.Limit = limit
		case "--cursor", "-c":
			raw, err := nextValue()
			if err != nil {
				return Query{}, false, false, err
			}
			query.PageToken = raw
		default:
			return Query{}, false, false, fmt.Errorf("unknown argument %q", field)
		}
	}
	if attention {
		if len(query.Outcomes) > 0 {
			return Query{}, false, false, errors.New("--attention cannot be combined with --outcome")
		}
		query.Outcomes = []Outcome{OutcomeFailed, OutcomePartiallyApplied}
	}
	normalized, err := NormalizeQuery(query)
	return normalized, attention, false, err
}

func splitRefinementFilter(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, strings.ReplaceAll(strings.ToLower(strings.TrimSpace(part)), "-", "_"))
	}
	return values
}

func renderRefinementPage(page Page, query Query, attention bool, authority string) string {
	if authority = strings.TrimSpace(authority); authority == "" {
		authority = "configured store"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Refinement audit — %d shown (source: %s authoritative)\n", len(page.Records), auditText(authority, 48))
	fmt.Fprintf(&b, "Scanned: %d", page.ScannedCount)
	if page.ScanTruncated {
		b.WriteString(" (bounded scan stopped before filling this page; continue with the cursor)")
	}
	b.WriteByte('\n')
	if attention {
		b.WriteString("Attention: immutable failed or partially_applied outcomes; inspect exact lineage before reconciling.\n")
	}
	if len(page.Records) == 0 {
		b.WriteString("No refinement records matched this page.\n")
	}
	for _, record := range page.Records {
		fmt.Fprintf(&b, "- %s phase=%s outcome=%s scope=%s refinement=%s created=%s\n",
			auditText(record.ID, 128), auditText(string(record.Phase), 24), auditText(string(record.Outcome), 32),
			auditText(string(record.Scope), 24), auditText(record.RefinementID, 128), auditText(record.CreatedAt, 48))
		fmt.Fprintf(&b, "  key=%s summary=%q edits=%d resources=%s",
			auditText(record.Key, 160), auditText(record.Summary, 200), len(record.Edits), auditResourceKinds(record.Edits))
		if record.ParentRecordID != "" {
			fmt.Fprintf(&b, " parent=%s", auditText(record.ParentRecordID, 128))
		}
		if record.RollbackOfRecordID != "" {
			fmt.Fprintf(&b, " rollback-of=%s", auditText(record.RollbackOfRecordID, 128))
		}
		if record.TaskRef != nil {
			fmt.Fprintf(&b, " task=%s:%s", auditText(record.TaskRef.System, 48), auditText(record.TaskRef.ID, 128))
		}
		b.WriteByte('\n')
		if record.Outcome == OutcomeFailed || record.Outcome == OutcomePartiallyApplied {
			b.WriteString("  attention=required follow-up=inspect exact record and authoritative resource heads before retry or rollback\n")
		}
	}
	if page.NextPageToken != "" {
		b.WriteString("Next page: /refinements")
		if attention {
			b.WriteString(" --attention")
		} else if len(query.Outcomes) > 0 {
			b.WriteString(" --outcome ")
			b.WriteString(joinStrings(query.Outcomes))
		}
		if len(query.Phases) > 0 {
			b.WriteString(" --phase ")
			b.WriteString(joinStrings(query.Phases))
		}
		if len(query.Scopes) > 0 {
			b.WriteString(" --scope ")
			b.WriteString(joinStrings(query.Scopes))
		}
		if len(query.ResourceKinds) > 0 {
			b.WriteString(" --resource ")
			b.WriteString(joinStrings(query.ResourceKinds))
		}
		if query.RefinementID != "" {
			b.WriteString(" --refinement ")
			b.WriteString(query.RefinementID)
		}
		if query.Text != "" {
			b.WriteString(" --search ")
			b.WriteString(query.Text)
		}
		fmt.Fprintf(&b, " --limit %d --cursor %s\n", query.Limit, page.NextPageToken)
	}
	return strings.TrimRight(b.String(), "\n")
}

func auditResourceKinds(edits []Edit) string {
	seen := map[ResourceKind]struct{}{}
	for _, edit := range edits {
		seen[edit.ResourceKind] = struct{}{}
	}
	values := make([]ResourceKind, 0, len(seen))
	for kind := range seen {
		values = append(values, kind)
	}
	normalized, _ := normalizeFilter(values, validResourceKind, "resource kind")
	if len(normalized) == 0 {
		return "none"
	}
	return joinStrings(normalized)
}

func joinStrings[T ~string](values []T) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = string(value)
	}
	return strings.Join(parts, ",")
}

func auditText(value string, maxRunes int) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	runes := []rune(value)
	if maxRunes > 0 && len(runes) > maxRunes {
		return string(runes[:maxRunes-1]) + "…"
	}
	return value
}
