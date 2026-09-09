// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package refinement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrApprovalRequired    = errors.New("refinement: explicit approval required")
	ErrApprovalRejected    = errors.New("refinement: approval rejected")
	ErrUnsupportedResource = errors.New("refinement: unsupported resource authority")
)

type RiskLevel string

const (
	RiskLow    RiskLevel = "low"
	RiskMedium RiskLevel = "medium"
	RiskHigh   RiskLevel = "high"
)

// Policy is intentionally conservative by default. Only a session-scoped,
// low-risk proposal can ever bypass explicit approval, and only when a host
// opts in.
type Policy struct {
	AllowSessionLowRiskWithoutApproval bool
}

type Assessment struct {
	Risk             RiskLevel `json:"risk"`
	RequiresApproval bool      `json:"requires_approval"`
	Reasons          []string  `json:"reasons"`
}

// Mutation is the authoritative result of exactly one edit. Editors must make
// operationID idempotent: replaying it with the same edit returns the same
// result, while replaying it with different input returns ErrConflict.
type Mutation struct {
	Before   *ResourceSnapshot `json:"before,omitempty"`
	After    *ResourceSnapshot `json:"after,omitempty"`
	Replayed bool              `json:"replayed,omitempty"`
}

type ResourceEditor interface {
	Apply(ctx context.Context, workspaceID, operationID string, edit Edit) (Mutation, error)
}

type ProposeRequest struct {
	OperationID        string
	Key                string
	Plan               Plan
	RollbackOfRecordID string
	TaskRef            *ExternalReference
	Metadata           map[string]any
}

type ProposalResult struct {
	Record     Record     `json:"record"`
	Assessment Assessment `json:"assessment"`
	Replayed   bool       `json:"replayed"`
}

type Approval struct {
	Granted   bool
	Reference *ExternalReference
}

type ApplyRequest struct {
	OperationID      string
	ProposalRecordID string
	Approval         Approval
}

type ApplyResult struct {
	Assessment  Assessment `json:"assessment"`
	Decision    Record     `json:"decision"`
	Application *Record    `json:"application,omitempty"`
}

// Service coordinates immutable audit transitions around resource editors. It
// does not own a distributed task lifecycle: TaskRef and approval references
// point at Aether (or another execution authority) when one is present.
type Service struct {
	Store           Store
	Editors         map[ResourceKind]ResourceEditor
	Policy          Policy
	AuditAuthority  string
	AuditAuthorizer AuditAuthorizer
}

func Assess(plan Plan, policy Policy) Assessment {
	risk := RiskLow
	reasons := make([]string, 0, 6)
	escalate := func(level RiskLevel, reason string) {
		if riskRank(level) > riskRank(risk) {
			risk = level
		}
		reasons = appendUnique(reasons, reason)
	}

	switch plan.Scope {
	case ScopeWorkspace:
		escalate(RiskMedium, "workspace-scoped changes affect future sessions")
	case ScopeUser, ScopeTenant, ScopeGlobal:
		escalate(RiskHigh, "broad-scope changes affect multiple workspaces or users")
	}
	if len(plan.Edits) > 1 {
		escalate(RiskMedium, "multi-resource changes can be partially applied")
	}
	for _, edit := range plan.Edits {
		switch edit.Action {
		case ActionDelete:
			escalate(RiskHigh, "deletion can remove authoritative behavior or knowledge")
		case ActionRestore:
			escalate(RiskHigh, "restoration reactivates previously deleted authoritative state")
		case ActionReplace:
			if edit.ExpectedETag == "" {
				escalate(RiskHigh, "replacement lacks an optimistic concurrency precondition")
			}
		}
		switch edit.ResourceKind {
		case ResourceAgentSpecification:
			escalate(RiskMedium, "agent specification changes alter delegated behavior")
		case ResourceMemory:
			escalate(RiskMedium, "memory changes persist beyond the current turn")
		case ResourceSkill:
			escalate(RiskHigh, "skill changes can alter executable capabilities")
		}
		if containsSensitiveControl(edit.Content) {
			escalate(RiskHigh, "content changes permissions, tools, code, endpoints, or credentials")
		}
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "single session-scoped prompt-note mutation")
	}
	requiresApproval := plan.Scope != ScopeSession || risk != RiskLow || !policy.AllowSessionLowRiskWithoutApproval
	return Assessment{Risk: risk, RequiresApproval: requiresApproval, Reasons: reasons}
}

func (s *Service) Propose(ctx context.Context, workspaceID string, request ProposeRequest) (ProposalResult, error) {
	if s == nil || s.Store == nil {
		return ProposalResult{}, fmt.Errorf("%w: audit store is required", ErrInvalid)
	}
	if err := validateApplicablePlan(request.Plan); err != nil {
		return ProposalResult{}, err
	}
	assessment := Assess(request.Plan, s.Policy)
	if request.RollbackOfRecordID != "" {
		assessment.Risk = RiskHigh
		assessment.RequiresApproval = true
		assessment.Reasons = appendUnique(assessment.Reasons, "rollback changes previously applied authoritative state")
	}
	metadata := cloneMap(request.Metadata)
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["assessment"] = assessment
	result, err := s.Store.Append(ctx, workspaceID, request.OperationID, AppendRequest{
		Key:                request.Key,
		Plan:               request.Plan,
		Phase:              PhaseProposal,
		Outcome:            OutcomeProposed,
		RollbackOfRecordID: request.RollbackOfRecordID,
		TaskRef:            request.TaskRef,
		SchemaVersion:      schemaVersionForPlan(request.Plan),
		Metadata:           metadata,
	})
	if err != nil {
		return ProposalResult{}, err
	}
	return ProposalResult{Record: result.Record, Assessment: assessment, Replayed: result.Replayed}, nil
}

func (s *Service) Apply(ctx context.Context, workspaceID string, request ApplyRequest) (ApplyResult, error) {
	if s == nil || s.Store == nil {
		return ApplyResult{}, fmt.Errorf("%w: audit store is required", ErrInvalid)
	}
	if strings.TrimSpace(request.OperationID) == "" || strings.TrimSpace(request.ProposalRecordID) == "" {
		return ApplyResult{}, fmt.Errorf("%w: operation id and proposal record id are required", ErrInvalid)
	}
	proposal, err := s.Store.Get(ctx, workspaceID, request.ProposalRecordID)
	if err != nil {
		return ApplyResult{}, err
	}
	if proposal.Phase != PhaseProposal || proposal.Outcome != OutcomeProposed {
		return ApplyResult{}, fmt.Errorf("%w: record %s is not an applicable proposal", ErrInvalid, proposal.ID)
	}
	assessment := Assess(proposal.Plan, s.Policy)
	if proposal.RollbackOfRecordID != "" {
		assessment.Risk = RiskHigh
		assessment.RequiresApproval = true
		assessment.Reasons = appendUnique(assessment.Reasons, "rollback changes previously applied authoritative state")
	}
	if assessment.RequiresApproval && request.Approval.Reference == nil {
		return ApplyResult{Assessment: assessment}, ErrApprovalRequired
	}
	if request.Approval.Reference != nil {
		if strings.TrimSpace(request.Approval.Reference.System) == "" || strings.TrimSpace(request.Approval.Reference.ID) == "" {
			return ApplyResult{Assessment: assessment}, fmt.Errorf("%w: approval reference requires system and id", ErrInvalid)
		}
	}

	decisionOutcome := OutcomeRejected
	if request.Approval.Granted {
		decisionOutcome = OutcomeApproved
	}
	decisionResult, err := s.Store.Append(ctx, workspaceID, derivedOperationID(request.OperationID, "decision"), AppendRequest{
		Key:                derivedKey(proposal.Key, request.OperationID, "decision"),
		Plan:               proposal.Plan,
		Phase:              PhaseDecision,
		Outcome:            decisionOutcome,
		ParentRecordID:     proposal.ID,
		RollbackOfRecordID: proposal.RollbackOfRecordID,
		TaskRef:            proposal.TaskRef,
		ApprovalRef:        request.Approval.Reference,
		SchemaVersion:      proposal.SchemaVersion,
		Metadata:           map[string]any{"assessment": assessment},
	})
	if err != nil {
		return ApplyResult{Assessment: assessment}, err
	}
	result := ApplyResult{Assessment: assessment, Decision: decisionResult.Record}
	if !request.Approval.Granted {
		return result, ErrApprovalRejected
	}

	edits := make([]Edit, len(proposal.Edits))
	applied := 0
	var applyErr error
	for i := range proposal.Edits {
		edits[i] = cloneEdit(proposal.Edits[i])
		editor := s.Editors[edits[i].ResourceKind]
		if editor == nil {
			applyErr = fmt.Errorf("%w: %s", ErrUnsupportedResource, edits[i].ResourceKind)
		} else {
			mutation, mutationErr := editor.Apply(ctx, workspaceID, derivedOperationID(proposal.ID, fmt.Sprintf("edit-%03d", i)), edits[i])
			if mutationErr != nil {
				applyErr = mutationErr
			} else {
				edits[i].Before = mutation.Before
				edits[i].After = mutation.After
				if mutation.Before != nil {
					edits[i].BeforeETag = mutation.Before.ETag
				}
				if mutation.After != nil {
					edits[i].AfterETag = mutation.After.ETag
				}
				edits[i].Applied = boolPointer(true)
				applied++
				continue
			}
		}
		edits[i].Applied = boolPointer(false)
		edits[i].Error = applyErr.Error()
		for j := i + 1; j < len(edits); j++ {
			edits[j] = cloneEdit(proposal.Edits[j])
			edits[j].Applied = boolPointer(false)
			edits[j].Error = "not attempted after prior edit failure"
		}
		break
	}

	phase := PhaseApplication
	outcome := OutcomeApplied
	if proposal.RollbackOfRecordID != "" {
		phase = PhaseRollback
		outcome = OutcomeRolledBack
	}
	if len(edits) == 0 {
		outcome = OutcomeNoOp
	} else if applied == 0 && applyErr != nil {
		outcome = OutcomeFailed
	} else if applied != len(edits) {
		outcome = OutcomePartiallyApplied
	}
	applicationPlan := proposal.Plan
	applicationPlan.Edits = edits
	applicationResult, appendErr := s.Store.Append(ctx, workspaceID, derivedOperationID(request.OperationID, "application"), AppendRequest{
		Key:                derivedKey(proposal.Key, request.OperationID, string(phase)),
		Plan:               applicationPlan,
		Phase:              phase,
		Outcome:            outcome,
		ParentRecordID:     decisionResult.Record.ID,
		RollbackOfRecordID: proposal.RollbackOfRecordID,
		TaskRef:            proposal.TaskRef,
		ApprovalRef:        request.Approval.Reference,
		SchemaVersion:      proposal.SchemaVersion,
		Metadata:           map[string]any{"assessment": assessment},
	})
	if appendErr != nil {
		return result, appendErr
	}
	result.Application = &applicationResult.Record
	if applyErr != nil {
		return result, applyErr
	}
	return result, nil
}

// BuildRollbackPlan deterministically reverses the applied subset in reverse
// order. Its ETag preconditions target the exact revisions created by the
// application, so later user changes cause a conflict rather than being lost.
func BuildRollbackPlan(application Record, refinementID string) (Plan, error) {
	if application.Phase != PhaseApplication && application.Phase != PhaseRollback {
		return Plan{}, fmt.Errorf("%w: record %s is not an application", ErrInvalid, application.ID)
	}
	if strings.TrimSpace(refinementID) == "" {
		return Plan{}, fmt.Errorf("%w: refinement id is required", ErrInvalid)
	}
	edits := make([]Edit, 0, len(application.Edits))
	for i := len(application.Edits) - 1; i >= 0; i-- {
		original := application.Edits[i]
		if original.Applied == nil || !*original.Applied {
			continue
		}
		if original.After == nil {
			return Plan{}, fmt.Errorf("%w: edit %d lacks its authoritative after snapshot", ErrInvalid, i)
		}
		reverse := Edit{
			ResourceKind: original.ResourceKind,
			ResourceKey:  original.ResourceKey,
			ResourceID:   original.ResourceID,
			Reason:       "restore state captured before application " + application.ID,
		}
		switch original.Action {
		case ActionCreate:
			reverse.Action = ActionDelete
			reverse.ResourceID = firstNonEmpty(snapshotID(original.After), original.ResourceID)
			reverse.ExpectedETag = snapshotETag(original.After)
		case ActionReplace:
			if original.Before == nil {
				return Plan{}, fmt.Errorf("%w: replacement edit %d lacks its before snapshot", ErrInvalid, i)
			}
			reverse.Action = ActionReplace
			reverse.ResourceID = firstNonEmpty(snapshotID(original.After), original.ResourceID)
			reverse.ExpectedETag = snapshotETag(original.After)
			reverse.Content = cloneMap(original.Before.Content)
		case ActionDelete:
			if original.Before == nil || original.Before.Deleted || !original.After.Deleted || original.After.ETag == "" {
				return Plan{}, fmt.Errorf("%w: deletion edit %d lacks a valid active-before/tombstone-after pair", ErrInvalid, i)
			}
			reverse.Action = ActionRestore
			reverse.ResourceID = firstNonEmpty(snapshotID(original.After), snapshotID(original.Before), original.ResourceID)
			reverse.ExpectedETag = snapshotETag(original.After)
		case ActionRestore:
			if original.Before == nil || !original.Before.Deleted || original.After.Deleted || original.After.ETag == "" {
				return Plan{}, fmt.Errorf("%w: restore edit %d lacks a valid tombstone-before/active-after pair", ErrInvalid, i)
			}
			reverse.Action = ActionDelete
			reverse.ResourceID = firstNonEmpty(snapshotID(original.After), original.ResourceID)
			reverse.ExpectedETag = snapshotETag(original.After)
		default:
			return Plan{}, fmt.Errorf("%w: unknown action %q", ErrInvalid, original.Action)
		}
		edits = append(edits, reverse)
	}
	return Plan{
		RefinementID:    refinementID,
		Trigger:         "rollback of application " + application.ID,
		Scope:           application.Scope,
		Summary:         "Rollback: " + application.Summary,
		Rationale:       "Restore the authoritative snapshots captured before the selected application.",
		ExpectedOutcome: "The applied resources match their pre-application snapshots.",
		Evidence:        []Evidence{{Kind: EvidenceResourceRevision, Reference: application.ID, Description: "immutable refinement application record"}},
		Edits:           edits,
	}, nil
}

func validateApplicablePlan(plan Plan) error {
	if len(plan.Edits) == 0 {
		return fmt.Errorf("%w: an applicable proposal requires at least one edit", ErrInvalid)
	}
	if len(plan.Edits) > 32 {
		return fmt.Errorf("%w: at most 32 edits may be included in one proposal", ErrInvalid)
	}
	seen := map[string]struct{}{}
	for i, edit := range plan.Edits {
		identity := string(edit.ResourceKind) + "/" + edit.ResourceKey
		if _, exists := seen[identity]; exists {
			return fmt.Errorf("%w: duplicate resource edit %q", ErrInvalid, identity)
		}
		seen[identity] = struct{}{}
		if edit.Applied != nil || edit.Error != "" || edit.Before != nil || edit.After != nil || edit.BeforeETag != "" || edit.AfterETag != "" {
			return fmt.Errorf("%w: proposal edit %d contains application output", ErrInvalid, i)
		}
		switch edit.Action {
		case ActionCreate:
			if edit.ResourceID != "" || edit.ExpectedETag != "" || len(edit.Content) == 0 {
				return fmt.Errorf("%w: create edit %d requires content and no resource id or expected ETag", ErrInvalid, i)
			}
		case ActionReplace:
			if edit.ResourceID == "" || edit.ExpectedETag == "" || len(edit.Content) == 0 {
				return fmt.Errorf("%w: replace edit %d requires resource id, content, and expected ETag", ErrInvalid, i)
			}
		case ActionDelete:
			if edit.ResourceID == "" || edit.ExpectedETag == "" || len(edit.Content) != 0 {
				return fmt.Errorf("%w: delete edit %d requires resource id and expected ETag and no content", ErrInvalid, i)
			}
		case ActionRestore:
			if edit.ResourceID == "" || edit.ExpectedETag == "" || len(edit.Content) != 0 {
				return fmt.Errorf("%w: restore edit %d requires resource id and tombstone ETag and no content", ErrInvalid, i)
			}
		}
		encoded, err := json.Marshal(edit.Content)
		if err != nil {
			return fmt.Errorf("%w: edit %d content is not JSON: %v", ErrInvalid, i, err)
		}
		if len(encoded) > 256<<10 {
			return fmt.Errorf("%w: edit %d content exceeds 262144 bytes", ErrInvalid, i)
		}
	}
	return nil
}

func containsSensitiveControl(content map[string]any) bool {
	if content == nil {
		return false
	}
	sensitive := map[string]struct{}{
		"allowed_tools": {}, "denied_tools": {}, "permission_mode": {}, "permissions": {},
		"trust": {}, "command": {}, "script": {}, "code": {}, "endpoint": {},
		"credential": {}, "credentials": {}, "secret": {}, "token": {},
	}
	var walk func(any) bool
	walk = func(value any) bool {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				if _, found := sensitive[strings.ToLower(key)]; found {
					return true
				}
				if walk(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if walk(child) {
					return true
				}
			}
		}
		return false
	}
	return walk(content)
}

func schemaVersionForPlan(plan Plan) int {
	for _, edit := range plan.Edits {
		if edit.Action == ActionRestore {
			return 2
		}
	}
	return 1
}

func riskRank(level RiskLevel) int {
	switch level {
	case RiskHigh:
		return 3
	case RiskMedium:
		return 2
	default:
		return 1
	}
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func derivedOperationID(base, suffix string) string {
	candidate := base + "/" + suffix
	if len(candidate) <= 200 {
		return candidate
	}
	sum := sha256.Sum256([]byte(candidate))
	return base[:min(len(base), 150)] + "/sha256-" + hex.EncodeToString(sum[:16])
}

func derivedKey(base, operationID, suffix string) string {
	sum := sha256.Sum256([]byte(operationID + "/" + suffix))
	tail := suffix + "-" + hex.EncodeToString(sum[:8])
	candidate := strings.TrimRight(base, "/") + "/" + tail
	if len(candidate) <= 200 && validKey.MatchString(candidate) {
		return candidate
	}
	prefix := strings.TrimRight(base[:min(len(base), 160)], "/")
	return prefix + "/" + tail
}

func boolPointer(value bool) *bool { return &value }

func snapshotID(snapshot *ResourceSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return snapshot.ResourceID
}

func snapshotETag(snapshot *ResourceSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return snapshot.ETag
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
