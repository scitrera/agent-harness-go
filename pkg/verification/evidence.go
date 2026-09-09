// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package verification records bounded, content-free evidence from admitted
// tool results. It is an optional development-profile composition: merely
// constructing an Observer does not alter tool policy or turn completion.
package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const defaultMaxRecords = 256

type CheckScope string

const (
	CheckTargeted CheckScope = "targeted"
	CheckBroader  CheckScope = "broader"
)

type Status string

const (
	StatusNotRun         Status = "not_run"
	StatusChecksFailed   Status = "checks_failed"
	StatusTargetedPassed Status = "targeted_checks_passed"
	StatusBroaderPassed  Status = "broader_suite_passed"
)

// Recipe identifies a validation command without prescribing one globally.
// Every ArgumentsContain fragment must occur in the admitted JSON arguments.
type Recipe struct {
	Name             string
	ToolName         string
	ArgumentsContain []string
	Scope            CheckScope
}

type Config struct {
	MaxRecordsPerSession int
	Recipes              []Recipe
	Now                  func() time.Time
}

type Record struct {
	ID               string                  `json:"id"`
	Addr             protocol.MessageAddress `json:"addr"`
	CallID           string                  `json:"call_id"`
	ToolName         string                  `json:"tool_name"`
	ArgumentsHash    string                  `json:"arguments_hash"`
	WorkingDirectory string                  `json:"working_directory,omitempty"`
	ExitCode         int                     `json:"exit_code,omitempty"`
	IsError          bool                    `json:"is_error,omitempty"`
	CheckName        string                  `json:"check_name,omitempty"`
	CheckScope       CheckScope              `json:"check_scope,omitempty"`
	FileChanges      []tools.FileChange      `json:"file_changes,omitempty"`
	References       []tools.ResultReference `json:"references,omitempty"`
	FinishedAt       string                  `json:"finished_at"`
}

type Assessment struct {
	Status         Status   `json:"status"`
	MaterialWrites bool     `json:"material_writes"`
	ChangedPaths   []string `json:"changed_paths,omitempty"`
	EvidenceIDs    []string `json:"evidence_ids,omitempty"`
}

// Observer implements the existing hooks.ToolObserver and the optional richer
// hooks.ToolResultObserver. It stores no arguments or output payloads.
type Observer struct {
	mu         sync.Mutex
	maxRecords int
	recipes    []Recipe
	now        func() time.Time
	records    map[string][]Record
	nudged     map[string]bool
}

func NewObserver(config Config) (*Observer, error) {
	maxRecords := config.MaxRecordsPerSession
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	for _, recipe := range config.Recipes {
		if strings.TrimSpace(recipe.Name) == "" || strings.TrimSpace(recipe.ToolName) == "" {
			return nil, errors.New("verification: recipe name and tool name are required")
		}
		if recipe.Scope != CheckTargeted && recipe.Scope != CheckBroader {
			return nil, errors.New("verification: recipe scope must be targeted or broader")
		}
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Observer{
		maxRecords: maxRecords, recipes: append([]Recipe(nil), config.Recipes...), now: now,
		records: map[string][]Record{}, nudged: map[string]bool{},
	}, nil
}

func (*Observer) ToolStarted(context.Context, hooks.ToolCall)               {}
func (*Observer) ToolFinished(context.Context, hooks.ToolCall, bool, error) {}

func (o *Observer) ToolResult(ctx context.Context, call hooks.ToolCall, result tools.Result, runErr error) {
	recipe, matched := o.matchRecipe(call)
	if !matched && len(result.Metadata.FileChanges) == 0 {
		return
	}
	finishedAt := o.now().UTC().Format(time.RFC3339Nano)
	record := Record{
		Addr: call.Addr, CallID: call.CallID, ToolName: call.Name,
		ArgumentsHash: tools.ArgumentsHash(call.Args), ExitCode: result.Metadata.ExitCode,
		IsError: runErr != nil || result.IsError, FinishedAt: finishedAt,
		FileChanges: cloneFileChanges(result.Metadata.FileChanges),
		References:  cloneReferences(result.Metadata.References),
	}
	if cwd, ok := tools.WorkingDirectoryFrom(ctx); ok {
		record.WorkingDirectory = cwd
	}
	if matched {
		record.CheckName, record.CheckScope = recipe.Name, recipe.Scope
	}
	record.ID = evidenceID(record)

	key := sessionKey(call.Addr)
	o.mu.Lock()
	defer o.mu.Unlock()
	items := append(o.records[key], record)
	if overflow := len(items) - o.maxRecords; overflow > 0 {
		items = append([]Record(nil), items[overflow:]...)
	}
	o.records[key] = items
}

func (o *Observer) matchRecipe(call hooks.ToolCall) (Recipe, bool) {
	arguments := string(call.Args)
	var selected Recipe
	found := false
	for _, recipe := range o.recipes {
		if recipe.ToolName != call.Name {
			continue
		}
		matches := true
		for _, fragment := range recipe.ArgumentsContain {
			if !strings.Contains(arguments, fragment) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		if !found || recipe.Scope == CheckBroader {
			selected, found = recipe, true
		}
	}
	return selected, found
}

// Assess distinguishes targeted success, broader success, failed checks, and
// no validation run while reporting the paths actually changed.
func (o *Observer) Assess(addr protocol.MessageAddress) Assessment {
	o.mu.Lock()
	items := append([]Record(nil), o.records[sessionKey(addr)]...)
	o.mu.Unlock()

	assessment := Assessment{Status: StatusNotRun}
	paths := map[string]struct{}{}
	for _, record := range items {
		if len(record.FileChanges) > 0 {
			assessment.MaterialWrites = true
		}
		for _, change := range record.FileChanges {
			if path := strings.TrimSpace(change.Path); path != "" {
				paths[path] = struct{}{}
			}
		}
		if record.CheckScope == "" {
			continue
		}
		assessment.EvidenceIDs = append(assessment.EvidenceIDs, record.ID)
		if record.IsError {
			if assessment.Status == StatusNotRun {
				assessment.Status = StatusChecksFailed
			}
			continue
		}
		if record.CheckScope == CheckBroader {
			assessment.Status = StatusBroaderPassed
		} else if assessment.Status != StatusBroaderPassed {
			assessment.Status = StatusTargetedPassed
		}
	}
	for path := range paths {
		assessment.ChangedPaths = append(assessment.ChangedPaths, path)
	}
	sort.Strings(assessment.ChangedPaths)
	return assessment
}

// ConsumeFinishNudge returns at most one reminder per session when material
// writes lack successful configured validation evidence.
func (o *Observer) ConsumeFinishNudge(addr protocol.MessageAddress) (string, bool) {
	assessment := o.Assess(addr)
	if !assessment.MaterialWrites || assessment.Status == StatusTargetedPassed || assessment.Status == StatusBroaderPassed {
		return "", false
	}
	key := sessionKey(addr)
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.nudged[key] {
		return "", false
	}
	o.nudged[key] = true
	if assessment.Status == StatusChecksFailed {
		return "Material writes have validation evidence, but the configured check failed; fix or report that failure before completion.", true
	}
	return "Material writes have no matching validation evidence; run a configured targeted check or explicitly report that checks were not run.", true
}

func (o *Observer) Records(addr protocol.MessageAddress) []Record {
	o.mu.Lock()
	defer o.mu.Unlock()
	items := o.records[sessionKey(addr)]
	out := make([]Record, len(items))
	for index := range items {
		out[index] = items[index]
		out[index].FileChanges = cloneFileChanges(items[index].FileChanges)
		out[index].References = cloneReferences(items[index].References)
	}
	return out
}

func sessionKey(addr protocol.MessageAddress) string {
	return addr.WorkspaceID + "\x00" + addr.ThreadID
}

func evidenceID(record Record) string {
	value := strings.Join([]string{
		record.Addr.WorkspaceID, record.Addr.ThreadID, record.CallID, record.ToolName,
		record.ArgumentsHash, record.CheckName, string(record.CheckScope), record.FinishedAt,
	}, "\x00")
	sum := sha256.Sum256([]byte(value))
	return "evidence-" + hex.EncodeToString(sum[:])
}

func cloneFileChanges(values []tools.FileChange) []tools.FileChange {
	return append([]tools.FileChange(nil), values...)
}

func cloneReferences(values []tools.ResultReference) []tools.ResultReference {
	return append([]tools.ResultReference(nil), values...)
}

var _ hooks.ToolObserver = (*Observer)(nil)
var _ hooks.ToolResultObserver = (*Observer)(nil)
