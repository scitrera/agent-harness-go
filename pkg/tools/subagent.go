package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

const subagentToolName = "spawn_subagent"

// RegisterSubagent registers the spawn_subagent tool, which delegates a
// self-contained sub-task to a fresh, bounded sub-agent. maxDepth caps nesting
// (a call at or beyond the limit returns an error result rather than recursing).
// The runner may be a late-bound *subagent.Ref so the tool can be surfaced to
// the model before the concrete runner is constructed.
func RegisterSubagent(reg *Registry, runner subagent.Runner, maxDepth int) error {
	if runner == nil {
		return fmt.Errorf("%w: subagent runner required", ErrInvalidTool)
	}
	err := reg.Register(subagentToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		depth := subagent.Depth(ctx)
		if maxDepth > 0 && depth >= maxDepth {
			return errorResult(req, fmt.Sprintf("sub-agent depth limit (%d) reached; handle this task directly", maxDepth))
		}
		var args struct {
			Task  string `json:"task"`
			Model string `json:"model"`
		}
		if err := decodeArgs(req, &args); err != nil {
			return Result{}, err
		}
		if args.Task == "" {
			return errorResult(req, "task is required")
		}
		res, err := runner.RunSubagent(subagent.WithDepth(ctx, depth+1), subagent.Request{
			Task:        args.Task,
			Depth:       depth + 1,
			Parent:      req.Addr,
			GrantID:     req.Authority.GrantID,
			SubjectType: req.Authority.SubjectType,
			SubjectID:   req.Authority.SubjectID,
			Model:       args.Model,
		})
		if err != nil {
			return errorResult(req, "sub-agent failed: "+err.Error())
		}
		out, err := json.Marshal(map[string]string{"result": res.Text})
		if err != nil {
			return Result{}, err
		}
		return NewJSONResult(req.CallID, req.Name, out)
	}))
	if err != nil {
		return err
	}
	reg.Describe(Descriptor{
		Name:        subagentToolName,
		Description: "Delegate a self-contained sub-task to a fresh sub-agent (bounded; no shared conversation history). Returns the sub-agent's final answer. Use for focused research/analysis you want isolated from the main thread, or to consult a specific/specialist model via the optional 'model' argument.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"task":{"type":"string","description":"A self-contained instruction for the sub-agent"},"model":{"type":"string","description":"Optional: a specific model name to run the sub-agent on (e.g. a vision or stronger model). Omit to use the default."}},"required":["task"]}`),
	})
	return nil
}

// errorResult builds an is_error tool result the model can read and adapt to.
func errorResult(req Request, message string) (Result, error) {
	payload, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		return Result{}, err
	}
	return Result{CallID: req.CallID, Name: req.Name, Payload: payload, IsError: true}, nil
}
