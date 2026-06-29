package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

const subagentToolName = "spawn_subagent"

type SubagentConfig struct {
	Runner   subagent.Runner
	MaxDepth int
	Catalog  subagent.Catalog
}

type agentSelection struct {
	Agent string
	Type  string
}

// RegisterSubagent registers the spawn_subagent tool, which delegates a
// self-contained sub-task to a fresh, bounded sub-agent. maxDepth caps nesting
// (a call at or beyond the limit returns an error result rather than recursing).
// The runner may be a late-bound *subagent.Ref so the tool can be surfaced to
// the model before the concrete runner is constructed.
func RegisterSubagent(reg *Registry, runner subagent.Runner, maxDepth int) error {
	return RegisterSubagentWithConfig(reg, SubagentConfig{Runner: runner, MaxDepth: maxDepth})
}

func RegisterSubagentWithConfig(reg *Registry, cfg SubagentConfig) error {
	if cfg.Runner == nil {
		return fmt.Errorf("%w: subagent runner required", ErrInvalidTool)
	}
	err := reg.Register(subagentToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		depth := subagent.Depth(ctx)
		if cfg.MaxDepth > 0 && depth >= cfg.MaxDepth {
			return errorResult(req, fmt.Sprintf("sub-agent depth limit (%d) reached; handle this task directly", cfg.MaxDepth))
		}
		var args struct {
			Task  string   `json:"task"`
			Model string   `json:"model"`
			Agent string   `json:"agent"`
			Type  string   `json:"type"`
			Tools []string `json:"tools"`
		}
		if err := decodeArgs(req, &args); err != nil {
			return Result{}, err
		}
		if args.Task == "" {
			return errorResult(req, "task is required")
		}
		def, selected, err := selectAgentDefinition(ctx, cfg.Catalog, agentSelection{Agent: args.Agent, Type: args.Type})
		if err != nil {
			return errorResult(req, err.Error())
		}
		if selected {
			for _, tool := range args.Tools {
				if err := def.AllowsTool(tool); err != nil {
					return errorResult(req, err.Error())
				}
			}
			if args.Model == "" {
				args.Model = def.Model
			}
		}
		subReq := subagent.Request{
			Task:        args.Task,
			Depth:       depth + 1,
			Parent:      req.Addr,
			GrantID:     req.Authority.GrantID,
			SubjectType: req.Authority.SubjectType,
			SubjectID:   req.Authority.SubjectID,
			Model:       args.Model,
		}
		if selected {
			applyDefinition(&subReq, def)
		}
		res, err := cfg.Runner.RunSubagent(subagent.WithDepth(ctx, depth+1), subReq)
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
		Parameters:  json.RawMessage(`{"type":"object","properties":{"task":{"type":"string","description":"A self-contained instruction for the sub-agent"},"agent":{"type":"string","description":"Optional: filesystem agent type/name from the local catalog."},"type":{"type":"string","description":"Alias for agent."},"model":{"type":"string","description":"Optional: a specific model name to run the sub-agent on (e.g. a vision or stronger model). Omit to use the default or selected agent model."},"tools":{"type":"array","items":{"type":"string"},"description":"Optional requested tool names checked against the selected agent definition."}},"required":["task"]}`),
	})
	return nil
}

func selectAgentDefinition(ctx context.Context, catalog subagent.Catalog, selection agentSelection) (subagent.Definition, bool, error) {
	if catalog == nil {
		return subagent.Definition{}, false, nil
	}
	selected := selection.Agent
	if selected == "" {
		selected = selection.Type
	}
	if selected == "" {
		return subagent.Definition{}, false, nil
	}
	def, err := catalog.Get(ctx, subagent.AgentType(selected))
	if err != nil {
		return subagent.Definition{}, false, err
	}
	return def, true, nil
}

func applyDefinition(req *subagent.Request, def subagent.Definition) {
	req.AgentName = def.Name
	req.AgentType = def.Type
	req.Instructions = def.Prompt
	req.MaxTurns = def.MaxTurns
	req.AllowedTools = append([]string(nil), def.AllowedTools...)
	req.DeniedTools = append([]string(nil), def.DeniedTools...)
	req.Skills = append([]string(nil), def.Skills...)
	req.MCPServers = append([]string(nil), def.MCPServers...)
	req.PermissionMode = def.PermissionMode
	req.ExecPolicyHint = def.ExecPolicyHint
	req.Background = def.Background
}

// errorResult builds an is_error tool result the model can read and adapt to.
func errorResult(req Request, message string) (Result, error) {
	payload, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		return Result{}, err
	}
	return Result{CallID: req.CallID, Name: req.Name, Payload: payload, IsError: true}, nil
}
