// Package turnjournal provides durable, workspace-scoped checkpoints for an
// in-progress parent model/tool loop. It is deliberately separate from chat
// history: history records what was said, while this journal records which
// execution mutation is safe to perform next after a process restart.
package turnjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	Schema                     = "agent-harness.turn.execution"
	SchemaRevision             = 2
	maxExternalDescriptorBytes = 256 << 10
)

var (
	ErrNotFound          = errors.New("turnjournal: execution not found")
	ErrAlreadyExists     = errors.New("turnjournal: execution already exists")
	ErrConflict          = errors.New("turnjournal: revision conflict")
	ErrInvalidRecord     = errors.New("turnjournal: invalid execution record")
	ErrInvalidTransition = errors.New("turnjournal: invalid execution transition")
	ErrCorruptStore      = errors.New("turnjournal: corrupt execution store")
)

// Phase identifies the last durably confirmed boundary in a parent turn. Only
// WaitingExternalChild and ChildResolved may resume model/tool execution.
// Completing/Failing/Interrupting are an outbox boundary: the turn-side outcome
// is durable, but the authoritative task host has not yet acknowledged the
// corresponding terminal state. Recovery may finish that idempotent handshake
// without replaying the model or tools.
type Phase string

const (
	PhasePrepared             Phase = "prepared"
	PhaseProviderPending      Phase = "provider_pending"
	PhaseToolPending          Phase = "tool_pending"
	PhaseWaitingExternalChild Phase = "waiting_external_child"
	PhaseChildResolved        Phase = "child_resolved"
	PhaseCompleting           Phase = "completing"
	PhaseFailing              Phase = "failing"
	PhaseInterrupting         Phase = "interrupting"
	PhaseCompleted            Phase = "completed"
	PhaseFailed               Phase = "failed"
	PhaseInterrupted          Phase = "interrupted"
)

// ToolOutcome distinguishes a requested mutation from an externally admitted
// child and a confirmed result. Requested/uncertain mutations are never replayed
// automatically.
type ToolOutcome string

const (
	ToolOutcomeRequested ToolOutcome = "requested"
	ToolOutcomeAdmitted  ToolOutcome = "admitted"
	ToolOutcomeConfirmed ToolOutcome = "confirmed"
	ToolOutcomeUncertain ToolOutcome = "uncertain"
)

// HistoryMessageRef names an immutable message already persisted in the
// workspace/session transcript. Digest is the lowercase SHA-256 of its canonical
// encoded form as defined by the caller; the journal validates shape, while the
// recovery layer resolves and verifies content.
type HistoryMessageRef struct {
	WorkspaceID string `json:"workspace_id"`
	SessionID   string `json:"session_id"`
	MessageID   string `json:"message_id"`
	Digest      string `json:"digest"`
}

// ExternalChildRef binds one pending spawn invocation to the authoritative task
// and immutable child execution descriptor/result location.
type ExternalChildRef struct {
	TaskID           string          `json:"task_id"`
	ExecutionID      string          `json:"execution_id"`
	ChildSessionID   string          `json:"child_session_id"`
	Descriptor       json.RawMessage `json:"descriptor"`
	DescriptorDigest string          `json:"descriptor_digest"`
}

// NewExternalChildRef binds an admitted task to the exact opaque execution
// descriptor needed to resolve its durable result after the original assignment
// delivery has already been acknowledged. The journal never interprets the
// descriptor and does not carry credentials in it.
func NewExternalChildRef(taskID, executionID, childSessionID string, descriptor []byte) (ExternalChildRef, error) {
	normalized, err := compactJSON(descriptor)
	if err != nil {
		return ExternalChildRef{}, fmt.Errorf("%w: invalid external execution descriptor", ErrInvalidRecord)
	}
	ref := ExternalChildRef{
		TaskID: taskID, ExecutionID: executionID, ChildSessionID: childSessionID,
		Descriptor:       normalized,
		DescriptorDigest: digestBytes(normalized),
	}
	if err := validateExternalChildRef(ref); err != nil {
		return ExternalChildRef{}, err
	}
	return ref, nil
}

// ToolCheckpoint is the durable parent-side boundary around one tool call.
// Assistant identifies the persisted assistant message containing the call.
// Result is set only after the corresponding tool-result message is durable.
type ToolCheckpoint struct {
	InvocationID string             `json:"invocation_id"`
	Name         string             `json:"name"`
	ArgsDigest   string             `json:"args_digest"`
	Assistant    HistoryMessageRef  `json:"assistant"`
	Outcome      ToolOutcome        `json:"outcome"`
	External     *ExternalChildRef  `json:"external,omitempty"`
	Result       *HistoryMessageRef `json:"result,omitempty"`
}

// Record is the complete, versioned checkpoint for one parent task. Revision is
// store-owned and monotonically increments on every successful transition.
// OwnerIdentity is the stable runtime identity permitted to reconcile it.
type Record struct {
	SchemaRevision uint32            `json:"schema_revision"`
	Schema         string            `json:"schema"`
	Revision       uint64            `json:"revision"`
	WorkspaceID    string            `json:"workspace_id"`
	SessionID      string            `json:"session_id"`
	TaskID         string            `json:"task_id"`
	OwnerIdentity  string            `json:"owner_identity"`
	Phase          Phase             `json:"phase"`
	Input          HistoryMessageRef `json:"input"`
	Iteration      uint32            `json:"iteration"`
	LastMessageID  string            `json:"last_message_id,omitempty"`
	Tool           *ToolCheckpoint   `json:"tool,omitempty"`
	FailureReason  string            `json:"failure_reason,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	CompletedAt    *time.Time        `json:"completed_at,omitempty"`
}

// Store is the backend-neutral optimistic journal contract. expectedRevision
// must be the Revision returned by Get/Create; exactly one concurrent writer can
// advance it. ListActive is the startup-recovery scan for a stable owner.
type Store interface {
	Create(ctx context.Context, record Record) (Record, error)
	Get(ctx context.Context, workspaceID, taskID string) (Record, error)
	Update(ctx context.Context, record Record, expectedRevision uint64) (Record, error)
	ListActive(ctx context.Context, ownerIdentity string) ([]Record, error)
}

func (r Record) Validate() error {
	if r.Schema != Schema || r.SchemaRevision != SchemaRevision || r.Revision == 0 {
		return fmt.Errorf("%w: unsupported schema or zero revision", ErrInvalidRecord)
	}
	for name, value := range map[string]string{
		"workspace_id":   r.WorkspaceID,
		"session_id":     r.SessionID,
		"task_id":        r.TaskID,
		"owner_identity": r.OwnerIdentity,
	} {
		if err := validateIdentifier(name, value); err != nil {
			return err
		}
	}
	if !validPhase(r.Phase) {
		return fmt.Errorf("%w: unknown phase %q", ErrInvalidRecord, r.Phase)
	}
	if err := validateHistoryRef("input", r.Input, r.WorkspaceID, r.SessionID); err != nil {
		return err
	}
	if r.LastMessageID != "" {
		if err := validateIdentifier("last_message_id", r.LastMessageID); err != nil {
			return err
		}
	}
	if r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() || r.UpdatedAt.Before(r.CreatedAt) {
		return fmt.Errorf("%w: invalid timestamps", ErrInvalidRecord)
	}
	if r.CreatedAt.Location() != time.UTC || r.UpdatedAt.Location() != time.UTC {
		return fmt.Errorf("%w: timestamps must be UTC", ErrInvalidRecord)
	}
	if err := validateTerminal(r); err != nil {
		return err
	}
	if err := validateToolForPhase(r); err != nil {
		return err
	}
	return nil
}

func validateTerminal(r Record) error {
	terminal := r.Terminal()
	if terminal {
		if r.CompletedAt == nil || r.CompletedAt.IsZero() || r.CompletedAt.Location() != time.UTC || r.CompletedAt.Before(r.CreatedAt) || r.UpdatedAt.Before(*r.CompletedAt) {
			return fmt.Errorf("%w: terminal phase requires a valid UTC completion timestamp", ErrInvalidRecord)
		}
		if (r.Phase == PhaseFailed || r.Phase == PhaseInterrupted) && strings.TrimSpace(r.FailureReason) == "" {
			return fmt.Errorf("%w: failed/interrupted phase requires a reason", ErrInvalidRecord)
		}
		return nil
	}
	if r.CompletedAt != nil {
		return fmt.Errorf("%w: active phase cannot carry terminal fields", ErrInvalidRecord)
	}
	if r.Phase == PhaseFailing || r.Phase == PhaseInterrupting {
		if strings.TrimSpace(r.FailureReason) == "" {
			return fmt.Errorf("%w: pending failure/interruption requires a reason", ErrInvalidRecord)
		}
	} else if r.FailureReason != "" {
		return fmt.Errorf("%w: active phase cannot carry a failure reason", ErrInvalidRecord)
	}
	return nil
}

func validateToolForPhase(r Record) error {
	requiresTool := r.Phase == PhaseToolPending || r.Phase == PhaseWaitingExternalChild || r.Phase == PhaseChildResolved
	if requiresTool && r.Tool == nil {
		return fmt.Errorf("%w: phase %q requires a tool checkpoint", ErrInvalidRecord, r.Phase)
	}
	if r.Tool == nil {
		return nil
	}
	t := r.Tool
	if err := validateIdentifier("tool invocation_id", t.InvocationID); err != nil {
		return err
	}
	if err := validateIdentifier("tool name", t.Name); err != nil {
		return err
	}
	if !validDigest(t.ArgsDigest) {
		return fmt.Errorf("%w: invalid tool argument digest", ErrInvalidRecord)
	}
	if err := validateHistoryRef("tool assistant", t.Assistant, r.WorkspaceID, r.SessionID); err != nil {
		return err
	}
	switch t.Outcome {
	case ToolOutcomeRequested:
		if t.External != nil || t.Result != nil || (r.Phase != PhaseToolPending && r.Phase != PhaseFailed && r.Phase != PhaseFailing) {
			return fmt.Errorf("%w: requested tool has incompatible phase or result", ErrInvalidRecord)
		}
	case ToolOutcomeUncertain:
		if t.Result != nil || (r.Phase != PhaseInterrupted && r.Phase != PhaseInterrupting) {
			return fmt.Errorf("%w: uncertain tool must terminate as interrupted", ErrInvalidRecord)
		}
	case ToolOutcomeAdmitted:
		if t.External == nil || t.Result != nil || (r.Phase != PhaseWaitingExternalChild && r.Phase != PhaseFailed && r.Phase != PhaseInterrupted && r.Phase != PhaseFailing && r.Phase != PhaseInterrupting) {
			return fmt.Errorf("%w: admitted tool must retain one external child in a compatible phase", ErrInvalidRecord)
		}
	case ToolOutcomeConfirmed:
		if t.Result == nil {
			return fmt.Errorf("%w: confirmed tool requires a result reference", ErrInvalidRecord)
		}
		if err := validateHistoryRef("tool result", *t.Result, r.WorkspaceID, r.SessionID); err != nil {
			return err
		}
		if r.Phase != PhaseChildResolved && !r.Terminal() && !r.PendingTerminal() && r.Phase != PhaseProviderPending {
			return fmt.Errorf("%w: confirmed tool has incompatible phase %q", ErrInvalidRecord, r.Phase)
		}
	default:
		return fmt.Errorf("%w: unknown tool outcome %q", ErrInvalidRecord, t.Outcome)
	}
	if t.External != nil {
		if err := validateExternalChildRef(*t.External); err != nil {
			return err
		}
	}
	return nil
}

func validateExternalChildRef(ref ExternalChildRef) error {
	for name, value := range map[string]string{
		"external task_id":          ref.TaskID,
		"external execution_id":     ref.ExecutionID,
		"external child_session_id": ref.ChildSessionID,
	} {
		if err := validateIdentifier(name, value); err != nil {
			return err
		}
	}
	if len(ref.Descriptor) == 0 || len(ref.Descriptor) > maxExternalDescriptorBytes || !json.Valid(ref.Descriptor) {
		return fmt.Errorf("%w: invalid external execution descriptor", ErrInvalidRecord)
	}
	normalized, err := compactJSON(ref.Descriptor)
	if err != nil {
		return fmt.Errorf("%w: invalid external execution descriptor", ErrInvalidRecord)
	}
	if !validDigest(ref.DescriptorDigest) || ref.DescriptorDigest != digestBytes(normalized) {
		return fmt.Errorf("%w: external execution descriptor digest mismatch", ErrInvalidRecord)
	}
	return nil
}

func validateHistoryRef(name string, ref HistoryMessageRef, workspaceID, sessionID string) error {
	if ref.WorkspaceID != workspaceID || ref.SessionID != sessionID {
		return fmt.Errorf("%w: %s crosses workspace or session", ErrInvalidRecord, name)
	}
	if err := validateIdentifier(name+" message_id", ref.MessageID); err != nil {
		return err
	}
	if !validDigest(ref.Digest) {
		return fmt.Errorf("%w: invalid %s digest", ErrInvalidRecord, name)
	}
	return nil
}

func (r Record) Terminal() bool {
	switch r.Phase {
	case PhaseCompleted, PhaseFailed, PhaseInterrupted:
		return true
	default:
		return false
	}
}

// PendingTerminal reports whether the local outcome is durable but still
// awaits acknowledgment from the authoritative task host.
func (r Record) PendingTerminal() bool {
	switch r.Phase {
	case PhaseCompleting, PhaseFailing, PhaseInterrupting:
		return true
	default:
		return false
	}
}

// AcknowledgedPhase returns the immutable local terminal phase that follows a
// successful authoritative task transition.
func (r Record) AcknowledgedPhase() (Phase, bool) {
	switch r.Phase {
	case PhaseCompleting:
		return PhaseCompleted, true
	case PhaseFailing:
		return PhaseFailed, true
	case PhaseInterrupting:
		return PhaseInterrupted, true
	default:
		return "", false
	}
}

func validPhase(phase Phase) bool {
	switch phase {
	case PhasePrepared, PhaseProviderPending, PhaseToolPending, PhaseWaitingExternalChild, PhaseChildResolved,
		PhaseCompleting, PhaseFailing, PhaseInterrupting, PhaseCompleted, PhaseFailed, PhaseInterrupted:
		return true
	default:
		return false
	}
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validDigest(value string) bool { return digestPattern.MatchString(value) }

func validateIdentifier(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || trimmed == "" || len(value) > 1024 || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: invalid %s", ErrInvalidRecord, name)
	}
	return nil
}

func validateTransition(previous, next Record) error {
	if previous.Terminal() {
		return fmt.Errorf("%w: terminal record is immutable", ErrInvalidTransition)
	}
	if previous.Schema != next.Schema || previous.SchemaRevision != next.SchemaRevision || previous.WorkspaceID != next.WorkspaceID || previous.SessionID != next.SessionID || previous.TaskID != next.TaskID || previous.OwnerIdentity != next.OwnerIdentity || previous.Input != next.Input || !previous.CreatedAt.Equal(next.CreatedAt) {
		return fmt.Errorf("%w: immutable identity changed", ErrInvalidTransition)
	}
	if next.Iteration < previous.Iteration {
		return fmt.Errorf("%w: iteration regressed", ErrInvalidTransition)
	}
	if !allowedPhaseTransition(previous.Phase, next.Phase) {
		return fmt.Errorf("%w: %q to %q", ErrInvalidTransition, previous.Phase, next.Phase)
	}
	if previous.Tool != nil && next.Tool != nil {
		replacingConfirmed := previous.Phase == PhaseProviderPending && previous.Tool.Outcome == ToolOutcomeConfirmed && next.Phase == PhaseToolPending && next.Tool.Outcome == ToolOutcomeRequested
		identityChanged := previous.Tool.InvocationID != next.Tool.InvocationID || previous.Tool.Name != next.Tool.Name || previous.Tool.ArgsDigest != next.Tool.ArgsDigest || previous.Tool.Assistant != next.Tool.Assistant
		if identityChanged && !replacingConfirmed {
			return fmt.Errorf("%w: tool invocation identity changed", ErrInvalidTransition)
		}
		if !replacingConfirmed && previous.Tool.External != nil && (next.Tool.External == nil || !sameExternalChild(*previous.Tool.External, *next.Tool.External)) {
			return fmt.Errorf("%w: external child identity changed", ErrInvalidTransition)
		}
		if !replacingConfirmed && previous.Tool.Result != nil && (next.Tool.Result == nil || *previous.Tool.Result != *next.Tool.Result) {
			return fmt.Errorf("%w: confirmed tool result changed", ErrInvalidTransition)
		}
	}
	if previous.Tool != nil && next.Tool == nil && (previous.Tool.Outcome != ToolOutcomeConfirmed || (previous.Phase != PhaseChildResolved && previous.Phase != PhaseProviderPending)) {
		return fmt.Errorf("%w: unconfirmed tool checkpoint was removed", ErrInvalidTransition)
	}
	return nil
}

func sameExternalChild(left, right ExternalChildRef) bool {
	return left.TaskID == right.TaskID && left.ExecutionID == right.ExecutionID &&
		left.ChildSessionID == right.ChildSessionID && left.DescriptorDigest == right.DescriptorDigest &&
		equalJSON(left.Descriptor, right.Descriptor)
}

func compactJSON(value []byte) (json.RawMessage, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), compact.Bytes()...), nil
}

func equalJSON(left, right []byte) bool {
	leftCompact, leftErr := compactJSON(left)
	rightCompact, rightErr := compactJSON(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCompact, rightCompact)
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func allowedPhaseTransition(from, to Phase) bool {
	if from == to {
		return true
	}
	switch from {
	case PhaseCompleting:
		return to == PhaseCompleted || to == PhaseInterrupted
	case PhaseFailing:
		return to == PhaseFailed || to == PhaseInterrupted
	case PhaseInterrupting:
		return to == PhaseInterrupted
	}
	if to == PhaseFailed || to == PhaseInterrupted || to == PhaseCompleting || to == PhaseFailing || to == PhaseInterrupting {
		return true
	}
	switch from {
	case PhasePrepared:
		return to == PhaseProviderPending
	case PhaseProviderPending:
		return to == PhaseToolPending || to == PhaseCompleted
	case PhaseToolPending:
		return to == PhaseWaitingExternalChild || to == PhaseProviderPending
	case PhaseWaitingExternalChild:
		return to == PhaseChildResolved
	case PhaseChildResolved:
		return to == PhaseProviderPending || to == PhaseCompleted
	default:
		return false
	}
}
