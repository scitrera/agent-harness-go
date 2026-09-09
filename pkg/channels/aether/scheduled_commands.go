// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	pb "github.com/scitrera/aether/api/proto"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/threadindex"
)

const (
	defaultScheduledRunsCommandLimit int32 = 20
	maxScheduledRunsCursorBytes            = 8 << 10
)

const scheduledRunsCommandUsage = "Usage: /runs [--status STATUS[,STATUS...]] [--limit 1..100] [--cursor OPAQUE_TOKEN]"

// ScheduledOperationsAuthorizer is the enterprise policy seam for operations
// inspection. OSS is single-user and passes nil; multi-user hosts can authorize
// the complete authenticated/OBO message before any Aether or MemoryLayer read.
type ScheduledOperationsAuthorizer interface {
	AuthorizeScheduledOperations(
		ctx context.Context,
		addr protocol.MessageAddress,
		user protocol.ChatMessage,
		command string,
	) error
}

type scheduledTurnScheduleStateLister interface {
	ListScheduledTurnScheduleStates(context.Context) ([]ScheduledTurnScheduleState, error)
}

type scheduledRunPageQuerier interface {
	Query(context.Context, ScheduledRunQuery) (ScheduledRunPage, error)
}

// ScheduledOperationsCommands renders the worker-owned /schedules and /runs
// inspection surface. It owns no state: every invocation reads Aether and the
// configured journal/thread authorities through the existing projections.
type ScheduledOperationsCommands struct {
	schedules  scheduledTurnScheduleStateLister
	runs       scheduledRunPageQuerier
	authorizer ScheduledOperationsAuthorizer
}

// NewScheduledOperationsCommands constructs the operations surface for an
// Aether worker. The returned value structurally implements turn's optional
// ScheduledOperationsCommandProvider without coupling this channel package to
// the runner package.
func NewScheduledOperationsCommands(
	c *Channel,
	journal ScheduledRunJournal,
	threads threadindex.WorkspaceLookup,
	authorizer ScheduledOperationsAuthorizer,
	timeout time.Duration,
) (*ScheduledOperationsCommands, error) {
	if c == nil {
		return nil, errors.New("aether channel is required")
	}
	reader, err := c.ScheduledRunReader(journal, threads, timeout)
	if err != nil {
		return nil, err
	}
	return &ScheduledOperationsCommands{schedules: c, runs: reader, authorizer: authorizer}, nil
}

// RunScheduledOperationsCommand handles only schedules/runs. User mistakes are
// rendered with usage and do not fail the surrounding task; authority/backend
// failures remain errors so the task lifecycle records the failed inspection.
func (c *ScheduledOperationsCommands) RunScheduledOperationsCommand(
	ctx context.Context,
	addr protocol.MessageAddress,
	user protocol.ChatMessage,
	name string,
	args string,
) (string, error) {
	if c == nil || c.schedules == nil || c.runs == nil {
		return "", errors.New("scheduled operations are not configured")
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if c.authorizer != nil {
		if err := c.authorizer.AuthorizeScheduledOperations(ctx, addr, user, name); err != nil {
			return "", fmt.Errorf("authorize scheduled operations: %w", err)
		}
	}
	switch name {
	case "schedules":
		if arg := strings.TrimSpace(args); arg != "" {
			if arg == "--help" || arg == "-h" {
				return "Usage: /schedules", nil
			}
			return "Usage: /schedules\nError: this command does not accept arguments.", nil
		}
		states, err := c.schedules.ListScheduledTurnScheduleStates(ctx)
		if err != nil {
			return "", err
		}
		return renderScheduledTurnScheduleStates(states), nil
	case "runs":
		query, statusNames, help, err := parseScheduledRunsCommand(args)
		if err != nil {
			return scheduledRunsCommandUsage + "\nError: " + err.Error(), nil
		}
		if help {
			return scheduledRunsCommandUsage + "\nStatuses: queued, running, completed, failed, cancelled, waiting_input, waiting_authority, waiting_dependency, hibernated, rejected.", nil
		}
		page, err := c.runs.Query(ctx, query)
		if err != nil {
			return "", err
		}
		if page.NextPageToken != "" {
			if err := validateScheduledRunsCursor(page.NextPageToken); err != nil {
				return "", fmt.Errorf("aether returned an invalid next-page cursor: %w", err)
			}
		}
		return renderScheduledRunPage(page, query.Limit, statusNames), nil
	default:
		return "", fmt.Errorf("unsupported scheduled operations command %q", name)
	}
}

func parseScheduledRunsCommand(args string) (ScheduledRunQuery, []string, bool, error) {
	query := ScheduledRunQuery{Limit: defaultScheduledRunsCommandLimit}
	fields := strings.Fields(args)
	var statusNames []string
	seenStatuses := map[pb.TaskStatus]struct{}{}
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
				return ScheduledRunQuery{}, nil, false, errors.New("--help does not accept a value")
			}
			return query, statusNames, true, nil
		case "--limit", "-n":
			raw, err := nextValue()
			if err != nil {
				return ScheduledRunQuery{}, nil, false, err
			}
			limit, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || limit < 1 || limit > int64(maxScheduledRunPageSize) {
				return ScheduledRunQuery{}, nil, false, fmt.Errorf("limit must be between 1 and %d", maxScheduledRunPageSize)
			}
			query.Limit = int32(limit)
		case "--cursor", "-c":
			raw, err := nextValue()
			if err != nil {
				return ScheduledRunQuery{}, nil, false, err
			}
			if err := validateScheduledRunsCursor(raw); err != nil {
				return ScheduledRunQuery{}, nil, false, err
			}
			query.PageToken = raw
		case "--status", "-s":
			raw, err := nextValue()
			if err != nil {
				return ScheduledRunQuery{}, nil, false, err
			}
			for _, name := range strings.Split(raw, ",") {
				status, canonical, err := parseScheduledRunStatus(name)
				if err != nil {
					return ScheduledRunQuery{}, nil, false, err
				}
				if _, duplicate := seenStatuses[status]; duplicate {
					continue
				}
				seenStatuses[status] = struct{}{}
				query.Statuses = append(query.Statuses, status)
				statusNames = append(statusNames, canonical)
			}
		default:
			return ScheduledRunQuery{}, nil, false, fmt.Errorf("unknown argument %q", field)
		}
	}
	return query, statusNames, false, nil
}

func parseScheduledRunStatus(value string) (pb.TaskStatus, string, error) {
	canonical := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", "_")
	statuses := map[string]pb.TaskStatus{
		"queued":             pb.TaskStatus_TASK_STATUS_QUEUED,
		"running":            pb.TaskStatus_TASK_STATUS_RUNNING,
		"completed":          pb.TaskStatus_TASK_STATUS_COMPLETED,
		"failed":             pb.TaskStatus_TASK_STATUS_FAILED,
		"cancelled":          pb.TaskStatus_TASK_STATUS_CANCELLED,
		"waiting_input":      pb.TaskStatus_TASK_STATUS_WAITING_INPUT,
		"waiting_authority":  pb.TaskStatus_TASK_STATUS_WAITING_AUTHORITY,
		"waiting_dependency": pb.TaskStatus_TASK_STATUS_WAITING_DEPENDENCY,
		"hibernated":         pb.TaskStatus_TASK_STATUS_HIBERNATED,
		"rejected":           pb.TaskStatus_TASK_STATUS_REJECTED,
	}
	status, ok := statuses[canonical]
	if !ok {
		return pb.TaskStatus_TASK_STATUS_UNSPECIFIED, "", fmt.Errorf("unknown status %q", value)
	}
	return status, canonical, nil
}

func validateScheduledRunsCursor(cursor string) error {
	if cursor == "" {
		return errors.New("cursor must not be empty")
	}
	if len(cursor) > maxScheduledRunsCursorBytes {
		return fmt.Errorf("cursor exceeds %d bytes", maxScheduledRunsCursorBytes)
	}
	for _, r := range cursor {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("cursor contains whitespace or control characters")
		}
	}
	return nil
}

func renderScheduledTurnScheduleStates(states []ScheduledTurnScheduleState) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Scheduled turns — %d (Aether WorkflowEngine authoritative)\n", len(states))
	b.WriteString("Identity: pinned declaration workspace/view/worker.\n")
	if len(states) == 0 {
		b.WriteString("No scheduled turns are owned by this worker.")
		return b.String()
	}
	for _, state := range states {
		fmt.Fprintf(&b, "- %s [%s] %s %q miss=%s\n",
			operationsText(state.DeclarationID, 96), enabledLabel(state.Enabled),
			operationsText(state.ScheduleType, 24), operationsText(state.ScheduleExpression, 160),
			operationsText(state.MissPolicy, 32))
		fmt.Fprintf(&b, "  workspace=%s thread=%s view=%s worker=%s\n",
			operationsText(state.LogicalWorkspace, 96), operationsText(state.ThreadID, 96),
			viewLabel(state.ViewID, state.ViewRevision), operationsText(state.AssignedTo, 128))
		fmt.Fprintf(&b, "  next=%s last-fired=%s", operationsTime(state.NextFireAt), operationsTime(state.LastFiredAt))
		if state.LastOccurrence == nil {
			b.WriteString(" latest=none\n")
			continue
		}
		occurrence := state.LastOccurrence
		fmt.Fprintf(&b, " latest=%s", operationsText(occurrence.Disposition, 32))
		if occurrence.Reason != "" {
			fmt.Fprintf(&b, " reason=%s", operationsText(occurrence.Reason, 48))
		}
		fmt.Fprintf(&b, " scheduled=%s backlog=%s", occurrence.ScheduledFor.UTC().Format(time.RFC3339), backlogLabel(occurrence.BacklogCount, occurrence.BacklogTruncated))
		if occurrence.BacklogIndex > 0 {
			fmt.Fprintf(&b, " index=%d", occurrence.BacklogIndex)
		}
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

func renderScheduledRunPage(page ScheduledRunPage, limit int32, statusNames []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Scheduled runs — %d shown / %d matching\n", len(page.Runs), page.TotalCount)
	b.WriteString("Sources: task=Aether; execution=turn journal (Aether KV in OSS); thread=MemoryLayer when present.\n")
	if len(page.Runs) == 0 {
		b.WriteString("No scheduled runs matched this page.\n")
	}
	for _, run := range page.Runs {
		fmt.Fprintf(&b, "- %s task=%s state=%s declaration=%s\n",
			operationsText(run.TaskID, 128), taskStatusLabel(run.TaskStatus), operationsText(run.State, 48),
			operationsText(run.Occurrence.DeclarationID, 96))
		fmt.Fprintf(&b, "  scheduled=%s dispatched=%s disposition=%s backlog=%s",
			run.Occurrence.ScheduledFor.UTC().Format(time.RFC3339), run.Occurrence.DispatchedAt.UTC().Format(time.RFC3339),
			operationsText(run.Occurrence.Disposition, 32), backlogLabel(run.Occurrence.BacklogCount, run.Occurrence.BacklogTruncated))
		if run.Occurrence.BacklogIndex > 0 {
			fmt.Fprintf(&b, " index=%d", run.Occurrence.BacklogIndex)
		}
		fmt.Fprintf(&b, " delay=%dms\n", run.Occurrence.DispatchDelayMilliseconds)
		fmt.Fprintf(&b, "  workspace=%s thread=%s view=%s worker=%s",
			operationsText(run.LogicalWorkspace, 96), operationsText(run.ThreadID, 96),
			viewLabel(run.ViewID, run.ViewRevision), operationsText(run.AssignedTo, 128))
		if run.Thread != nil && strings.TrimSpace(run.Thread.Title) != "" {
			fmt.Fprintf(&b, " title=%q", operationsText(run.Thread.Title, 120))
		}
		b.WriteByte('\n')
		if run.TaskError != "" {
			fmt.Fprintf(&b, "  error=%q\n", operationsText(run.TaskError, 240))
		}
	}
	if page.NextPageToken != "" {
		b.WriteString("Next page: /runs")
		if len(statusNames) > 0 {
			b.WriteString(" --status ")
			b.WriteString(strings.Join(statusNames, ","))
		}
		fmt.Fprintf(&b, " --limit %d --cursor %s\n", limit, page.NextPageToken)
	}
	return strings.TrimRight(b.String(), "\n")
}

func enabledLabel(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}

func operationsTime(value *time.Time) string {
	if value == nil {
		return "none"
	}
	return value.UTC().Format(time.RFC3339)
}

func backlogLabel(count int, truncated bool) string {
	if truncated {
		return strconv.Itoa(count) + "+"
	}
	return strconv.Itoa(count)
}

func viewLabel(id, revision string) string {
	label := operationsText(id, 96)
	if revision = operationsText(revision, 48); revision != "" {
		label += "@" + revision
	}
	return label
}

func taskStatusLabel(status string) string {
	return strings.ToLower(strings.TrimPrefix(status, "TASK_STATUS_"))
}

func operationsText(value string, maxRunes int) string {
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
