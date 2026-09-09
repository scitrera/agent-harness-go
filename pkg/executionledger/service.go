// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package executionledger

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// AuditAuthorizer is the enterprise policy seam in front of operator reads.
// The OSS composition leaves it nil under its single-user trust model.
type AuditAuthorizer interface {
	AuthorizeExecutionLedger(context.Context, protocol.MessageAddress, protocol.ChatMessage) error
}

type Service struct {
	Store           Store
	Authority       string
	AuditAuthorizer AuditAuthorizer
}

func sessionRef(addr protocol.MessageAddress) Ref {
	return Ref{WorkspaceID: addr.WorkspaceID, SessionID: addr.ThreadID}
}

func branchID(ctx context.Context, addr protocol.MessageAddress) string {
	if branch, ok := hooks.ExecutionBranchIDFrom(ctx); ok {
		return branch
	}
	if addr.TaskID != "" {
		return addr.TaskID
	}
	return "session-control"
}

func (s *Service) ObserveTurn(ctx context.Context, observed hooks.TurnEvent) {
	if s == nil || s.Store == nil || hooks.ExecutionLedgerDisabled(ctx) || observed.Addr.WorkspaceID == "" || observed.Addr.ThreadID == "" {
		return
	}
	kind, ok := eventTypeFor(observed)
	if !ok {
		return
	}
	if kind == EventTurnFinished {
		ctx = context.WithoutCancel(ctx)
	}
	request := AppendRequest{
		Type: kind, BranchID: branchID(ctx, observed.Addr), TaskID: observed.Addr.TaskID,
		MessageID: observed.MessageID, Iteration: observed.Iteration, Model: observed.Model,
	}
	if observed.Err != nil {
		request.Outcome = "failed"
		request.Error = observed.Err.Error()
	} else if kind == EventModelCallFinished || kind == EventTurnFinished {
		request.Outcome = "succeeded"
	}
	if observed.Reference != nil {
		request.Reference = &Reference{System: observed.Reference.System, Kind: observed.Reference.Kind, ID: observed.Reference.ID}
	}
	operationID := observed.OperationID
	if operationID == "" {
		operationID = observerOperationID(request)
	} else {
		// Provider call IDs and host journal IDs are only locally unique. Scope
		// them to this session branch before they enter the store-wide operation
		// namespace so two turns may legitimately reuse e.g. "call_1".
		operationID = "observe-" + hash([]byte(request.BranchID+"\x00"+string(kind)+"\x00"+operationID))
	}
	if _, err := s.Store.Append(ctx, sessionRef(observed.Addr), operationID, request); err != nil {
		slog.WarnContext(ctx, "execution ledger observation failed", slog.String("event", string(kind)), slog.Any("err", err))
	}
}

func eventTypeFor(observed hooks.TurnEvent) (EventType, bool) {
	switch observed.Phase {
	case hooks.PhaseTurnStarted:
		return EventTurnStarted, true
	case hooks.PhaseUserPromptSubmit:
		return EventUserPromptSubmitted, true
	case hooks.PhaseModelCallStarted:
		return EventModelCallStarted, true
	case hooks.PhaseModelCallFinished:
		return EventModelCallFinished, true
	case hooks.PhaseCompacted:
		return EventContextCompacted, true
	case hooks.PhaseRecoveryStarted:
		return EventRecoveryMarker, true
	case hooks.PhaseResourceReferenced:
		if observed.Reference == nil {
			return "", false
		}
		switch observed.Reference.System {
		case "goal-store":
			return EventGoalReferenced, true
		case "refinement-store":
			return EventRefinementReferenced, true
		case "skill-catalog":
			return EventSkillReferenced, true
		default:
			return "", false
		}
	case hooks.PhaseTurnFinished:
		return EventTurnFinished, true
	default:
		return "", false
	}
}

func observerOperationID(request AppendRequest) string {
	reference := ""
	if request.Reference != nil {
		reference = request.Reference.System + ":" + request.Reference.Kind + ":" + request.Reference.ID
	}
	value := strings.Join([]string{request.BranchID, string(request.Type), strconv.Itoa(request.Iteration), request.Model, reference}, "\x00")
	return "observe-" + hash([]byte(value))
}

// PinModel durably records a thread-local model selection. Persistence happens
// before the runner updates its in-memory cache, so a failed write cannot make
// the current process claim a pin that will disappear on restart.
func (s *Service) PinModel(ctx context.Context, addr protocol.MessageAddress, model string) error {
	if s == nil || s.Store == nil || hooks.ExecutionLedgerDisabled(ctx) {
		return nil
	}
	request := AppendRequest{Type: EventModelPinned, BranchID: branchID(ctx, addr), TaskID: addr.TaskID, Model: model}
	operationID := "pin-" + hash([]byte(request.BranchID+"\x00"+model))
	_, err := s.Store.Append(ctx, sessionRef(addr), operationID, request)
	return err
}

func (s *Service) PinnedModel(ctx context.Context, addr protocol.MessageAddress) (string, error) {
	if s == nil || s.Store == nil {
		return "", nil
	}
	return s.Store.PinnedModel(ctx, sessionRef(addr))
}

var _ hooks.TurnObserver = (*Service)(nil)

func (s *Service) String() string {
	if s == nil {
		return "execution ledger (disabled)"
	}
	authority := strings.TrimSpace(s.Authority)
	if authority == "" {
		authority = "configured store"
	}
	return fmt.Sprintf("execution ledger (%s)", authority)
}
