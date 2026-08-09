// Package refinement defines backend-neutral continual-refinement plans and
// append-only audit records. It deliberately does not apply edits: hosts keep
// proposal execution, approval, and recovery under their existing authorities.
package refinement

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalid  = errors.New("refinement: invalid record")
	ErrConflict = errors.New("refinement: conflict")
	ErrNotFound = errors.New("refinement: record not found")
)

var validKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$`)

type Phase string
type Scope string
type Outcome string
type ResourceKind string
type Action string
type EvidenceKind string

const (
	PhaseProposal    Phase = "proposal"
	PhaseDecision    Phase = "decision"
	PhaseApplication Phase = "application"
	PhaseRollback    Phase = "rollback"

	ScopeSession   Scope = "session"
	ScopeWorkspace Scope = "workspace"
	ScopeUser      Scope = "user"
	ScopeTenant    Scope = "tenant"
	ScopeGlobal    Scope = "global"

	OutcomeProposed         Outcome = "proposed"
	OutcomeApproved         Outcome = "approved"
	OutcomeRejected         Outcome = "rejected"
	OutcomeApplied          Outcome = "applied"
	OutcomePartiallyApplied Outcome = "partially_applied"
	OutcomeFailed           Outcome = "failed"
	OutcomeRolledBack       Outcome = "rolled_back"
	OutcomeNoOp             Outcome = "no_op"

	ResourcePromptNote         ResourceKind = "prompt_note"
	ResourceMemory             ResourceKind = "memory"
	ResourceSkill              ResourceKind = "skill"
	ResourceAgentSpecification ResourceKind = "agent_specification"

	ActionCreate  Action = "create"
	ActionReplace Action = "replace"
	ActionDelete  Action = "delete"

	EvidenceMessage          EvidenceKind = "message"
	EvidenceToolResult       EvidenceKind = "tool_result"
	EvidenceResourceRevision EvidenceKind = "resource_revision"
	EvidenceEvaluation       EvidenceKind = "evaluation"
	EvidenceUserInstruction  EvidenceKind = "user_instruction"
	EvidenceOther            EvidenceKind = "other"
)

type Evidence struct {
	Kind        EvidenceKind `json:"kind"`
	Reference   string       `json:"reference"`
	Description string       `json:"description"`
	ContentHash string       `json:"content_hash,omitempty"`
}

// ResourceSnapshot captures the authority response before or after a mutation.
// Content is the authority-neutral resource document used to plan a safe
// rollback; secrets and other non-resource execution state must not be placed
// here.
type ResourceSnapshot struct {
	ResourceID    string         `json:"resource_id,omitempty"`
	ResourceKey   string         `json:"resource_key"`
	ETag          string         `json:"etag,omitempty"`
	SchemaVersion int            `json:"schema_version,omitempty"`
	Content       map[string]any `json:"content,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Deleted       bool           `json:"deleted,omitempty"`
}

type Edit struct {
	Action       Action       `json:"action"`
	ResourceKind ResourceKind `json:"resource_kind"`
	ResourceKey  string       `json:"resource_key"`
	ResourceID   string       `json:"resource_id,omitempty"`
	ExpectedETag string       `json:"expected_etag,omitempty"`
	BeforeETag   string       `json:"before_etag,omitempty"`
	AfterETag    string       `json:"after_etag,omitempty"`
	Reason       string       `json:"reason"`
	// Content is the desired authority-neutral resource document for create and
	// replace actions. Delete actions leave it empty.
	Content map[string]any    `json:"content,omitempty"`
	Before  *ResourceSnapshot `json:"before,omitempty"`
	After   *ResourceSnapshot `json:"after,omitempty"`
	Applied *bool             `json:"applied,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// ExternalReference correlates an audit record to an execution-plane object
// such as an Aether task or approval without copying its live state.
type ExternalReference struct {
	System    string `json:"system"`
	ID        string `json:"id"`
	AttemptID string `json:"attempt_id,omitempty"`
}

// Plan is the evidence-backed semantic proposal shared by proposal and outcome
// records. Before/after ETags live on edits so applications are auditable.
type Plan struct {
	RefinementID    string     `json:"refinement_id"`
	Trigger         string     `json:"trigger"`
	Scope           Scope      `json:"scope"`
	Summary         string     `json:"summary"`
	Rationale       string     `json:"rationale"`
	ExpectedOutcome string     `json:"expected_outcome"`
	Evidence        []Evidence `json:"evidence"`
	Edits           []Edit     `json:"edits"`
}

// AppendRequest creates one immutable audit record. Later decisions,
// applications, corrections, and rollbacks are separate linked records.
type AppendRequest struct {
	Key string `json:"key"`
	Plan
	Phase              Phase              `json:"phase"`
	Outcome            Outcome            `json:"outcome"`
	ParentRecordID     string             `json:"parent_record_id,omitempty"`
	RollbackOfRecordID string             `json:"rollback_of_record_id,omitempty"`
	TaskRef            *ExternalReference `json:"task_ref,omitempty"`
	ApprovalRef        *ExternalReference `json:"approval_ref,omitempty"`
	SchemaVersion      int                `json:"schema_version,omitempty"`
	Metadata           map[string]any     `json:"metadata,omitempty"`
}

// Record is the immutable, authority-neutral audit projection.
type Record struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	AppendRequest
	Revision  int    `json:"revision"`
	ETag      string `json:"etag"`
	CreatedAt string `json:"created_at"`
}

type AppendResult struct {
	Record   Record
	Replayed bool
}

// Store is an append-only, workspace-aware refinement audit. It never applies
// edits or represents an execution task's live lifecycle.
type Store interface {
	Append(ctx context.Context, workspaceID, operationID string, request AppendRequest) (AppendResult, error)
	Get(ctx context.Context, workspaceID, recordID string) (Record, error)
	// List returns immutable records newest-first.
	List(ctx context.Context, workspaceID string) ([]Record, error)
}

func (r AppendRequest) Validate() error {
	if !validKey.MatchString(r.Key) {
		return fmt.Errorf("%w: invalid key %q", ErrInvalid, r.Key)
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = 1
	}
	if r.SchemaVersion != 1 {
		return fmt.Errorf("%w: unsupported schema version %d", ErrInvalid, r.SchemaVersion)
	}
	for name, value := range map[string]string{
		"refinement_id":    r.Plan.RefinementID,
		"trigger":          r.Plan.Trigger,
		"summary":          r.Plan.Summary,
		"rationale":        r.Plan.Rationale,
		"expected_outcome": r.Plan.ExpectedOutcome,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalid, name)
		}
	}
	if !validScope(r.Plan.Scope) {
		return fmt.Errorf("%w: invalid scope %q", ErrInvalid, r.Plan.Scope)
	}
	if len(r.Plan.Evidence) == 0 {
		return fmt.Errorf("%w: evidence is required", ErrInvalid)
	}
	for i, evidence := range r.Plan.Evidence {
		if !validEvidenceKind(evidence.Kind) || strings.TrimSpace(evidence.Reference) == "" || strings.TrimSpace(evidence.Description) == "" {
			return fmt.Errorf("%w: invalid evidence at index %d", ErrInvalid, i)
		}
	}
	if r.Outcome != OutcomeNoOp && len(r.Plan.Edits) == 0 {
		return fmt.Errorf("%w: non-no-op records require an edit", ErrInvalid)
	}
	for i, edit := range r.Plan.Edits {
		if !validAction(edit.Action) || !validResourceKind(edit.ResourceKind) || !validKey.MatchString(edit.ResourceKey) || strings.TrimSpace(edit.Reason) == "" {
			return fmt.Errorf("%w: invalid edit at index %d (action=%q resource_kind=%q resource_key=%q has_reason=%t)", ErrInvalid, i, edit.Action, edit.ResourceKind, edit.ResourceKey, strings.TrimSpace(edit.Reason) != "")
		}
	}
	if !validPhaseOutcome(r.Phase, r.Outcome) {
		return fmt.Errorf("%w: outcome %q is invalid for phase %q", ErrInvalid, r.Outcome, r.Phase)
	}
	if r.Phase == PhaseRollback && strings.TrimSpace(r.RollbackOfRecordID) == "" {
		return fmt.Errorf("%w: rollback record requires rollback_of_record_id", ErrInvalid)
	}
	for name, ref := range map[string]*ExternalReference{"task_ref": r.TaskRef, "approval_ref": r.ApprovalRef} {
		if ref != nil && (strings.TrimSpace(ref.System) == "" || strings.TrimSpace(ref.ID) == "") {
			return fmt.Errorf("%w: %s requires system and id", ErrInvalid, name)
		}
	}
	return nil
}

func validScope(scope Scope) bool {
	switch scope {
	case ScopeSession, ScopeWorkspace, ScopeUser, ScopeTenant, ScopeGlobal:
		return true
	default:
		return false
	}
}

func validEvidenceKind(kind EvidenceKind) bool {
	switch kind {
	case EvidenceMessage, EvidenceToolResult, EvidenceResourceRevision, EvidenceEvaluation, EvidenceUserInstruction, EvidenceOther:
		return true
	default:
		return false
	}
}

func validAction(action Action) bool {
	return action == ActionCreate || action == ActionReplace || action == ActionDelete
}

func validResourceKind(kind ResourceKind) bool {
	switch kind {
	case ResourcePromptNote, ResourceMemory, ResourceSkill, ResourceAgentSpecification:
		return true
	default:
		return false
	}
}

func validPhaseOutcome(phase Phase, outcome Outcome) bool {
	switch phase {
	case PhaseProposal:
		return outcome == OutcomeProposed || outcome == OutcomeNoOp
	case PhaseDecision:
		return outcome == OutcomeApproved || outcome == OutcomeRejected
	case PhaseApplication:
		return outcome == OutcomeApplied || outcome == OutcomePartiallyApplied || outcome == OutcomeFailed || outcome == OutcomeNoOp
	case PhaseRollback:
		return outcome == OutcomeRolledBack || outcome == OutcomePartiallyApplied || outcome == OutcomeFailed || outcome == OutcomeNoOp
	default:
		return false
	}
}
