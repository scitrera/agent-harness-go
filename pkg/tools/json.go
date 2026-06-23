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

func commandResult(req Request, result localtools.CommandResult) (Result, error) {
	return marshalResult(req, struct {
		ExitCode int    `json:"exit_code"`
		Output   string `json:"output"`
	}{ExitCode: result.ExitCode, Output: result.Output})
}
