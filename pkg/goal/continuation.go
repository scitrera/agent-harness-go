package goal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

const ContinuationEnvelopeSchema = "agent-harness.goal-continuation.v1"

// ContinuationAdmission is the immutable request handed to a durable delivery
// backend. Authority is intentionally separate from Envelope: adapters may
// persist it in a typed, task-scoped authority field, but it must never be
// serialized into the portable payload or the private decision ledger.
type ContinuationAdmission struct {
	WorkspaceID     string
	SessionID       string
	GoalID          string
	ParentTaskID    string
	ParentMessageID string
	Attempt         uint32
	Inbound         channel.Inbound
	Authority       tools.MemoryAuthority
}

// ContinuationEnvelope is the versioned, credential-free payload a durable
// backend stores and later reconstructs at assignment time.
type ContinuationEnvelope struct {
	Schema          string          `json:"schema"`
	WorkspaceID     string          `json:"workspace_id"`
	SessionID       string          `json:"session_id"`
	GoalID          string          `json:"goal_id"`
	ParentTaskID    string          `json:"parent_task_id,omitempty"`
	ParentMessageID string          `json:"parent_message_id"`
	Attempt         uint32          `json:"attempt"`
	Inbound         channel.Inbound `json:"inbound"`
}

// Clone returns an independently mutable copy suitable for ledger reads.
func (e ContinuationEnvelope) Clone() ContinuationEnvelope {
	out := e
	out.Inbound.Addr.Telemetry = cloneContinuationRawMap(e.Inbound.Addr.Telemetry)
	out.Inbound.Addr.Extra = cloneContinuationRawMap(e.Inbound.Addr.Extra)
	out.Inbound.Message = e.Inbound.Message.Clone()
	out.Inbound.Meta = append(json.RawMessage(nil), e.Inbound.Meta...)
	return out
}

func cloneContinuationRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	if values == nil {
		return nil
	}
	out := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		out[key] = append(json.RawMessage(nil), value...)
	}
	return out
}

// ContinuationReceipt identifies one authoritative backend delivery.
type ContinuationReceipt struct {
	Backend string
	TaskID  string
}

type ContinuationState string

const (
	ContinuationAdmitted  ContinuationState = "admitted"
	ContinuationRunning   ContinuationState = "running"
	ContinuationCompleted ContinuationState = "completed"
	ContinuationFailed    ContinuationState = "failed"
	ContinuationCancelled ContinuationState = "cancelled"
)

// ContinuationBackend makes a host-created follow-up durable. Admit must be
// idempotent for the admission identity. Inspect is authoritative and does not
// mutate task state.
type ContinuationBackend interface {
	Admit(ctx context.Context, admission ContinuationAdmission) (ContinuationReceipt, error)
	Inspect(ctx context.Context, receipt ContinuationReceipt) (ContinuationState, error)
}

// IsContinuationMessage identifies the trusted internal envelope stamped by
// Runtime. Transport adapters still validate their durable task payload and
// metadata before admitting it to ingress.
func IsContinuationMessage(message protocol.ChatMessage) bool {
	raw := message.Meta[GoalContinuationMetaKey]
	if len(raw) == 0 {
		return false
	}
	var metadata struct {
		GoalID    string `json:"goal_id"`
		Attempt   uint32 `json:"attempt"`
		Automated bool   `json:"automated"`
	}
	return json.Unmarshal(raw, &metadata) == nil && metadata.Automated &&
		strings.TrimSpace(metadata.GoalID) != "" && metadata.Attempt > 0
}

func NewContinuationEnvelope(admission ContinuationAdmission) (ContinuationEnvelope, error) {
	if err := admission.Validate(); err != nil {
		return ContinuationEnvelope{}, err
	}
	return ContinuationEnvelope{
		Schema:      ContinuationEnvelopeSchema,
		WorkspaceID: admission.WorkspaceID, SessionID: admission.SessionID,
		GoalID: admission.GoalID, ParentTaskID: admission.ParentTaskID,
		ParentMessageID: admission.ParentMessageID, Attempt: admission.Attempt,
		Inbound: admission.Inbound,
	}, nil
}

func (a ContinuationAdmission) Validate() error {
	return validateContinuationIdentity(
		a.WorkspaceID, a.SessionID, a.GoalID, a.ParentMessageID, a.Attempt, a.Inbound,
	)
}

func (e ContinuationEnvelope) Validate() error {
	if e.Schema != ContinuationEnvelopeSchema {
		return fmt.Errorf("goal: unsupported continuation envelope schema %q", e.Schema)
	}
	return validateContinuationIdentity(
		e.WorkspaceID, e.SessionID, e.GoalID, e.ParentMessageID, e.Attempt, e.Inbound,
	)
}

func (r ContinuationReceipt) Validate() error {
	if strings.TrimSpace(r.Backend) == "" || strings.TrimSpace(r.TaskID) == "" {
		return errors.New("goal: continuation receipt requires backend and task id")
	}
	return nil
}

func MarshalContinuationEnvelope(admission ContinuationAdmission) ([]byte, error) {
	envelope, err := NewContinuationEnvelope(admission)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("goal: encode continuation envelope: %w", err)
	}
	return payload, nil
}

func ParseContinuationEnvelope(payload []byte) (ContinuationEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var envelope ContinuationEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return ContinuationEnvelope{}, fmt.Errorf("goal: decode continuation envelope: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ContinuationEnvelope{}, errors.New("goal: decode continuation envelope: trailing value")
	}
	if err := envelope.Validate(); err != nil {
		return ContinuationEnvelope{}, err
	}
	return envelope, nil
}

func validateContinuationIdentity(workspaceID, sessionID, goalID, parentMessageID string, attempt uint32, inbound channel.Inbound) error {
	switch {
	case strings.TrimSpace(workspaceID) == "":
		return errors.New("goal: continuation workspace is required")
	case strings.TrimSpace(sessionID) == "":
		return errors.New("goal: continuation session is required")
	case strings.TrimSpace(goalID) == "":
		return errors.New("goal: continuation goal is required")
	case strings.TrimSpace(parentMessageID) == "":
		return errors.New("goal: continuation parent message is required")
	case attempt == 0:
		return errors.New("goal: continuation attempt must be positive")
	case strings.TrimSpace(inbound.Message.ID) == "":
		return errors.New("goal: continuation message id is required")
	case inbound.Message.Role != protocol.RoleUser:
		return errors.New("goal: continuation message must have user role")
	case inbound.Addr.WorkspaceID != workspaceID || inbound.Message.Addr.WorkspaceID != workspaceID:
		return errors.New("goal: continuation message workspace does not match admission")
	case inbound.Addr.ThreadID != sessionID || inbound.Message.Addr.ThreadID != sessionID:
		return errors.New("goal: continuation message session does not match admission")
	case strings.TrimSpace(inbound.Addr.RequestID) == "" || inbound.Message.Addr.RequestID != inbound.Addr.RequestID:
		return errors.New("goal: continuation request id is required and must match the message")
	case inbound.Message.Addr.TaskID != inbound.Addr.TaskID:
		return errors.New("goal: continuation task id must match the message")
	}
	if len(inbound.Message.Content) == 0 {
		return errors.New("goal: continuation message content is required")
	}
	if raw := inbound.Message.Meta["scitrera"]; len(raw) > 0 {
		var metadata struct {
			AuthorityHandoff string `json:"authority_handoff"`
		}
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return errors.New("goal: continuation scitrera metadata is invalid")
		}
		if metadata.AuthorityHandoff != "" {
			return errors.New("goal: durable continuation payload must not carry an authority handoff")
		}
	}
	return nil
}
