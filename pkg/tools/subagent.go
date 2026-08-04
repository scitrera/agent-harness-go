package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

// SubagentToolName is the registry name of the spawn_subagent tool. Exported so a
// distribution can scope it out per turn (e.g. WithExcludedTools on an ephemeral
// one-shot, which must not spawn durable child threads).
const SubagentToolName = "spawn_subagent"

type SubagentConfig struct {
	Runner   subagent.Runner
	MaxDepth int
	Catalog  subagent.Catalog
	// AllowBackground advertises + enables the `background` spawn argument. Set it
	// only when the Runner supports detached execution (implements
	// subagent.BackgroundRunner with a Notifier wired); off → the tool is
	// synchronous-only and the schema omits `background`.
	AllowBackground bool
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
	err := reg.Register(SubagentToolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		depth := subagent.Depth(ctx)
		if cfg.MaxDepth > 0 && depth >= cfg.MaxDepth {
			return errorResult(req, fmt.Sprintf("sub-agent depth limit (%d) reached; handle this task directly", cfg.MaxDepth))
		}
		var args struct {
			Task       string   `json:"task"`
			Model      string   `json:"model"`
			Agent      string   `json:"agent"`
			Type       string   `json:"type"`
			Tools      []string `json:"tools"`
			Thread     string   `json:"thread"`
			Background bool     `json:"background"`
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
			// A non-empty thread continues an existing child sub-agent thread
			// (its handle is the thread_id returned from a prior spawn) instead
			// of minting a new one.
			ResumeThreadID: args.Thread,
			// ParentMessageID is the spawning assistant message's id, carried on
			// req.MessageID by the turn loop, recorded on the child thread's task
			// message as MessageRef.ParentMessageID (the cross-thread back-ref).
			ParentMessageID: req.MessageID,
		}
		if selected {
			applyDefinition(&subReq, def)
		}
		name := subagentPartName(selected, def, args.Agent, args.Type)
		// Background spawn: return a handle immediately and deliver the result later
		// as a pushed follow-up turn. Falls through to the synchronous path when the
		// runner has no background seam (so behavior degrades cleanly).
		if cfg.AllowBackground && (args.Background || subReq.Background) {
			if result, handled, err := startBackgroundSpawn(ctx, cfg, req, subReq, name, depth); handled {
				return result, err
			}
		}
		res, err := cfg.Runner.RunSubagent(subagent.WithDepth(ctx, depth+1), subReq)
		if err != nil {
			return errorResult(req, "sub-agent failed: "+err.Error())
		}
		// Record the sub-agent on the per-turn world-state sink (both new spawns and
		// resumes) so its thread_id handle + name + summary age in WorldState and
		// surface to the orchestrator, which can then route a follow-up back to it.
		if sink, ok := WorldStateSinkFrom(ctx); ok {
			sink.RecordSubagent(res.ThreadID, name, "completed", res.Summary)
		}
		out, err := json.Marshal(map[string]string{
			"result":    res.Text,
			"thread_id": res.ThreadID,
			"summary":   res.Summary,
		})
		if err != nil {
			return Result{}, err
		}
		result, err := NewJSONResult(req.CallID, req.Name, out)
		if err != nil {
			return Result{}, err
		}
		// Record a structured subagent reference part (thread linkage + summary)
		// alongside the tool result so the parent conversation carries the
		// reference for recall/UI (spec §3.9/§11.3). The turn loop co-locates it
		// on the tool-result message.
		sp, perr := protocol.NewSubagentPart(protocol.SubagentPart{
			ID:       req.CallID,
			Name:     name,
			ThreadID: res.ThreadID,
			Status:   protocol.SubagentCompleted,
			Summary:  res.Summary,
		})
		if perr == nil {
			result.Parts = []protocol.ContentPart{sp}
		}
		return result, nil
	}))
	if err != nil {
		return err
	}
	props := `"task":{"type":"string","description":"A self-contained instruction for the sub-agent. The child inherits the active working directory; describe filesystem paths relative to it unless the user explicitly requested another allowed absolute directory."},"agent":{"type":"string","description":"Optional: filesystem agent type/name from the local catalog."},"type":{"type":"string","description":"Alias for agent."},"model":{"type":"string","description":"Optional: a specific model name to run the sub-agent on (e.g. a vision or stronger model). Omit to use the default or selected agent model."},"tools":{"type":"array","items":{"type":"string"},"description":"Optional requested tool names checked against the selected agent definition."},"thread":{"type":"string","description":"Optional: a thread_id returned by a previous spawn_subagent call. Provide it to CONTINUE that same sub-agent thread (route a follow-up to it) instead of creating a new sub-agent."}`
	if cfg.AllowBackground {
		props += `,"background":{"type":"boolean","description":"Optional: run the sub-agent in the BACKGROUND. Returns immediately with its thread_id and status 'running' instead of the result; the result is delivered later as a follow-up message on this thread, so you can keep working or spawn several in parallel and react to each as it finishes. Omit (default) to run synchronously and get the result inline."}`
	}
	reg.Describe(Descriptor{
		Name:        SubagentToolName,
		Description: "Delegate a self-contained sub-task to a sub-agent that runs on its own durable, persisted thread (bounded; isolated from the main conversation). Returns the sub-agent's final answer plus a `thread_id` handle and a short `summary`. Use for focused research/analysis you want isolated from the main thread, or to consult a specific/specialist model via the optional 'model' argument. Pass the optional `thread` (a prior `thread_id`) to CONTINUE a previous sub-agent instead of spawning a new one; the returned `thread_id` is that handle. PREFER continuing an existing sub-agent when a follow-up builds on work it already did: it keeps its own context (e.g. a document or image it already inspected, or a model it was pinned to) that you do not otherwise hold — reuse its handle rather than re-delegating the task from scratch.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{` + props + `},"required":["task"]}`),
	})
	return nil
}

// startBackgroundSpawn attempts to launch subReq as a DETACHED background
// sub-agent. handled is false when the runner has no background seam (not a
// BackgroundRunner, or it reports ErrBackgroundUnsupported) — the caller then
// falls back to a synchronous run. On success it records the running handle on the
// turn world-state (status "running", so the ledger surfaces it to later parent
// turns) and returns a {thread_id,status:"running"} tool result plus a
// SubagentPart(running) for the parent conversation/UI; the child's outcome arrives
// later as a pushed follow-up turn.
func startBackgroundSpawn(ctx context.Context, cfg SubagentConfig, req Request, subReq subagent.Request, name string, depth int) (Result, bool, error) {
	bg, ok := cfg.Runner.(subagent.BackgroundRunner)
	if !ok {
		return Result{}, false, nil
	}
	threadID, err := bg.StartBackground(subagent.WithDepth(ctx, depth+1), subReq)
	if errors.Is(err, subagent.ErrBackgroundUnsupported) {
		return Result{}, false, nil
	}
	if err != nil {
		res, rerr := errorResult(req, "sub-agent failed: "+err.Error())
		return res, true, rerr
	}
	if sink, ok := WorldStateSinkFrom(ctx); ok {
		sink.RecordSubagent(threadID, name, "running", "")
	}
	out, err := json.Marshal(map[string]string{
		"thread_id": threadID,
		"status":    "running",
		"note":      "sub-agent started in the background; its result will arrive as a follow-up message on this thread",
	})
	if err != nil {
		return Result{}, true, err
	}
	result, err := NewJSONResult(req.CallID, req.Name, out)
	if err != nil {
		return Result{}, true, err
	}
	if sp, perr := protocol.NewSubagentPart(protocol.SubagentPart{
		ID:       req.CallID,
		Name:     name,
		ThreadID: threadID,
		Status:   protocol.SubagentRunning,
	}); perr == nil {
		result.Parts = []protocol.ContentPart{sp}
	}
	return result, true, nil
}

// subagentPartName resolves a display name for the subagent reference part:
// the selected catalog definition's name/type when one was used, else the
// requested agent/type argument, else a generic "subagent".
func subagentPartName(selected bool, def subagent.Definition, agentArg, typeArg string) string {
	if selected {
		if def.Name != "" {
			return string(def.Name)
		}
		if def.Type != "" {
			return string(def.Type)
		}
	}
	if agentArg != "" {
		return agentArg
	}
	if typeArg != "" {
		return typeArg
	}
	return "subagent"
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
