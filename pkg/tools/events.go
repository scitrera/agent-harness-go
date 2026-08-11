package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os/exec"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

type ToolEventStatus string

const (
	ToolEventQueued   ToolEventStatus = "queued"
	ToolEventStarted  ToolEventStatus = "started"
	ToolEventFinished ToolEventStatus = "finished"
	ToolEventAborted  ToolEventStatus = "aborted"
)

type ToolEventSink interface {
	EmitToolEvent(ctx context.Context, event ToolEvent) error
}

type ToolEvent struct {
	Status           ToolEventStatus         `json:"status"`
	CallID           string                  `json:"call_id"`
	ToolName         string                  `json:"tool_name"`
	Addr             protocol.MessageAddress `json:"addr"`
	ArgumentsHash    string                  `json:"arguments_hash"`
	ArgumentsBytes   int                     `json:"arguments_bytes"`
	DurationMS       int64                   `json:"duration_ms,omitempty"`
	ApprovalDecision string                  `json:"approval_decision,omitempty"`
	PolicyDecision   string                  `json:"policy_decision,omitempty"`
	Reason           string                  `json:"reason,omitempty"`
	IsError          bool                    `json:"is_error,omitempty"`
	ErrorCode        string                  `json:"error_code,omitempty"`
	ErrorMessage     string                  `json:"error,omitempty"`
	Result           ResultMetadata          `json:"result,omitempty"`
}

type ResultMetadata struct {
	PayloadBytes    int          `json:"payload_bytes,omitempty"`
	ExitCode        int          `json:"exit_code,omitempty"`
	OutputBytes     int          `json:"output_bytes,omitempty"`
	OutputTruncated bool         `json:"output_truncated,omitempty"`
	PID             int          `json:"pid,omitempty"`
	FileChanges     []FileChange `json:"file_changes,omitempty"`
	// References are stable pointers returned by a tool to an authoritative
	// record. They let execution observers retain provenance without copying the
	// record payload into an operational ledger.
	References []ResultReference `json:"references,omitempty"`
}

type ResultReference struct {
	System string `json:"system"`
	Kind   string `json:"kind"`
	ID     string `json:"id"`
}

type FileChange struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
}

func NewToolEvent(status ToolEventStatus, req Request) ToolEvent {
	return ToolEvent{
		Status:         status,
		CallID:         req.CallID,
		ToolName:       req.Name,
		Addr:           req.Addr,
		ArgumentsHash:  ArgumentsHash(req.Arguments),
		ArgumentsBytes: len(req.Arguments),
	}
}

func ArgumentsHash(args []byte) string {
	sum := sha256.Sum256(args)
	return hex.EncodeToString(sum[:])
}

func DurationMillis(elapsed time.Duration) int64 {
	if elapsed <= 0 {
		return 0
	}
	ms := elapsed.Milliseconds()
	if ms == 0 {
		return 1
	}
	return ms
}

func ApplySafeError(event *ToolEvent, err error) {
	if err == nil {
		return
	}
	event.ErrorCode, event.ErrorMessage = SafeToolError(err)
}

func SafeToolError(err error) (string, string) {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled", "tool invocation canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout", "tool invocation timed out"
	case errors.Is(err, ErrInvalidArgument):
		return "invalid_argument", "invalid tool arguments"
	case errors.Is(err, ErrUnknownTool):
		return "unknown_tool", "unknown tool"
	case errors.Is(err, ErrInvalidTool):
		return "invalid_tool", "invalid tool"
	case errors.Is(err, ErrToolRequiresApproval):
		return "approval_required", "tool approval required"
	case errors.Is(err, localtools.ErrCommandFailed):
		var executableError *exec.Error
		if errors.As(err, &executableError) {
			return "command_failed", "command executable not found"
		}
		return "command_failed", "command exited unsuccessfully"
	case errors.Is(err, localtools.ErrPathOutsideRoot):
		return "path_outside_workspace", "path outside workspace"
	case errors.Is(err, localtools.ErrOldTextNotFound):
		return "old_text_not_found", "old text not found"
	case errors.Is(err, localtools.ErrInvalidFile):
		return "invalid_file", "invalid file"
	case errors.Is(err, localtools.ErrInvalidRoot):
		return "invalid_workspace", "invalid workspace"
	default:
		return "tool_error", "tool failed"
	}
}
