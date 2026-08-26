package tools

import (
	"encoding/json"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
)

func decodeArgs(req Request, target interface{}) error {
	if len(req.Arguments) == 0 {
		return fmt.Errorf("%w: empty arguments", ErrInvalidArgument)
	}
	if err := json.Unmarshal(req.Arguments, target); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	return nil
}

func marshalResult(req Request, payload interface{}) (Result, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Result{}, fmt.Errorf("marshal result: %w", err)
	}
	return NewJSONResult(req.CallID, req.Name, data)
}

func okResult(req Request) (Result, error) {
	return marshalResult(req, struct {
		OK bool `json:"ok"`
	}{OK: true})
}

// commandResult renders a finished command for the model. archiveRef, when
// non-empty, is the workspace-relative path holding the output that did not fit.
//
// The truncation marker is appended to `output` itself rather than left to the
// sibling fields: Result.Metadata never reaches the model (ContentPart
// serializes Payload alone), so a cut used to be entirely invisible — the model
// read a half-finished build log as if it were the whole thing. The marker goes
// on AFTER the cap so it survives the very truncation it is reporting.
func commandResult(req Request, result localtools.CommandResult, archiveRef string) (Result, error) {
	output := result.Output
	if result.OutputTruncated {
		output += truncationMarker(len(result.Output), result.OutputBytes, archiveRef)
	}
	out, err := marshalResult(req, struct {
		ExitCode      int    `json:"exit_code"`
		Output        string `json:"output"`
		Truncated     bool   `json:"output_truncated,omitempty"`
		OutputBytes   int    `json:"output_bytes,omitempty"`
		OutputArchive string `json:"output_archive,omitempty"`
	}{
		ExitCode:      result.ExitCode,
		Output:        output,
		Truncated:     result.OutputTruncated,
		OutputBytes:   result.OutputBytes,
		OutputArchive: archiveRef,
	})
	if err != nil {
		return Result{}, err
	}
	out.Metadata.ExitCode = result.ExitCode
	out.Metadata.OutputBytes = result.OutputBytes
	out.Metadata.OutputTruncated = result.OutputTruncated
	out.Metadata.PID = result.PID
	return out, nil
}

// truncationMarker states what was cut and, when an archive exists, how to get
// the rest. It names read_file explicitly because the alternative — a bare path —
// leaves the model guessing which tool applies.
func truncationMarker(shown, total int, archiveRef string) string {
	if archiveRef == "" {
		return fmt.Sprintf("\n[... output truncated: %d of %d bytes shown ...]", shown, total)
	}
	return fmt.Sprintf("\n[... output truncated: %d of %d bytes shown; head and tail retained at %s — read it with read_file ...]",
		shown, total, archiveRef)
}
