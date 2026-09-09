// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package executionledger

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

const commandUsage = "Usage: /ledger [--type VALUE[,VALUE...]] [--branch ID] [--task ID] [--reference ID] [--limit 1..100] [--cursor OPAQUE_TOKEN]"

func (s *Service) RunExecutionLedgerCommand(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage, args string) (string, error) {
	if s == nil || s.Store == nil {
		return "", errors.New("execution ledger is not configured")
	}
	if s.AuditAuthorizer != nil {
		if err := s.AuditAuthorizer.AuthorizeExecutionLedger(ctx, addr, user); err != nil {
			return "", fmt.Errorf("authorize execution ledger: %w", err)
		}
	}
	query, help, err := parseCommand(args)
	if err != nil {
		return commandUsage + "\nError: " + err.Error(), nil
	}
	if help {
		return commandUsage + "\nEvents are newest-first. Parent IDs retain branch lineage; reference IDs resolve against their named authoritative store.", nil
	}
	page, err := s.Store.Query(ctx, sessionRef(addr), query)
	if err != nil {
		return "", err
	}
	return renderPage(page, query, s.Authority), nil
}

func parseCommand(args string) (Query, bool, error) {
	query := Query{Limit: DefaultQueryLimit}
	var err error
	fields := strings.Fields(args)
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		key, inline, hasInline := strings.Cut(field, "=")
		next := func() (string, error) {
			if hasInline {
				if inline == "" {
					return "", fmt.Errorf("%s requires a value", key)
				}
				return inline, nil
			}
			if i+1 >= len(fields) {
				return "", fmt.Errorf("%s requires a value", key)
			}
			i++
			return fields[i], nil
		}
		switch key {
		case "--help", "-h":
			if hasInline {
				return Query{}, false, errors.New("--help does not accept a value")
			}
			return query, true, nil
		case "--type", "-t":
			raw, err := next()
			if err != nil {
				return Query{}, false, err
			}
			for _, value := range strings.Split(raw, ",") {
				query.Types = append(query.Types, EventType(strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", "_")))
			}
		case "--branch", "-b":
			query.BranchID, err = next()
			if err != nil {
				return Query{}, false, err
			}
		case "--task":
			query.TaskID, err = next()
			if err != nil {
				return Query{}, false, err
			}
		case "--reference", "-r":
			query.ReferenceID, err = next()
			if err != nil {
				return Query{}, false, err
			}
		case "--limit", "-n":
			raw, err := next()
			if err != nil {
				return Query{}, false, err
			}
			query.Limit, err = strconv.Atoi(raw)
			if err != nil {
				return Query{}, false, fmt.Errorf("limit must be between 1 and %d", MaxQueryLimit)
			}
		case "--cursor", "-c":
			query.PageToken, err = next()
			if err != nil {
				return Query{}, false, err
			}
		default:
			return Query{}, false, fmt.Errorf("unknown argument %q", field)
		}
	}
	query, err = normalizeQuery(query)
	return query, false, err
}

func renderPage(page Page, query Query, authority string) string {
	if authority = strings.TrimSpace(authority); authority == "" {
		authority = "configured store"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Execution ledger — %d shown (source: %s authoritative)\n", len(page.Events), safeText(authority, 48))
	fmt.Fprintf(&b, "Scanned: %d\n", page.ScannedCount)
	if len(page.Events) == 0 {
		b.WriteString("No execution events matched this page.\n")
	}
	for _, event := range page.Events {
		fmt.Fprintf(&b, "- #%d %s id=%s branch=%s created=%s\n", event.Sequence, event.Type, safeText(event.ID, 128), safeText(event.BranchID, 128), safeText(event.CreatedAt, 48))
		fmt.Fprintf(&b, "  task=%s", empty(event.TaskID))
		if event.ParentID != "" {
			fmt.Fprintf(&b, " parent=%s", safeText(event.ParentID, 128))
		}
		if event.Model != "" {
			fmt.Fprintf(&b, " model=%s", safeText(event.Model, 128))
		}
		if event.Iteration != 0 {
			fmt.Fprintf(&b, " iteration=%d", event.Iteration)
		}
		if event.Outcome != "" {
			fmt.Fprintf(&b, " outcome=%s", safeText(event.Outcome, 32))
		}
		if event.Reference != nil {
			fmt.Fprintf(&b, " reference=%s:%s:%s", safeText(event.Reference.System, 48), safeText(event.Reference.Kind, 48), safeText(event.Reference.ID, 128))
		}
		b.WriteByte('\n')
		if event.Error != "" {
			fmt.Fprintf(&b, "  error=%q\n", safeText(event.Error, 240))
		}
	}
	if page.NextPageToken != "" {
		b.WriteString("Next page: /ledger")
		if len(query.Types) > 0 {
			values := make([]string, len(query.Types))
			for i, value := range query.Types {
				values[i] = string(value)
			}
			b.WriteString(" --type " + strings.Join(values, ","))
		}
		if query.BranchID != "" {
			b.WriteString(" --branch " + query.BranchID)
		}
		if query.TaskID != "" {
			b.WriteString(" --task " + query.TaskID)
		}
		if query.ReferenceID != "" {
			b.WriteString(" --reference " + query.ReferenceID)
		}
		fmt.Fprintf(&b, " --limit %d --cursor %s\n", query.Limit, page.NextPageToken)
	}
	return strings.TrimRight(b.String(), "\n")
}

func empty(value string) string {
	if value == "" {
		return "none"
	}
	return safeText(value, 128)
}

func safeText(value string, maxRunes int) string {
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
