package turn

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// subagentSeq is a process-wide monotonic counter that makes each subagent
// invocation's threadID unique (so sibling subagents get distinct kernels).
var subagentSeq atomic.Uint64

// nextSubagentSeq returns the next process-wide subagent sequence number.
func nextSubagentSeq() uint64 { return subagentSeq.Add(1) }

// RunSubagent runs a bounded, ephemeral sub-agent turn and returns its final
// text. It reuses the runner's tools, bootstrap loader, context manager, and
// model, but on a throwaway in-memory session: no durable history, no memory
// recall/commit, and no stream egress (the sub-agent is internal). The parent's
// OBO grant is carried through so tools act on behalf of the same principal.
//
// This is the in-process backend for the spawn_subagent tool; the
// subagent.Runner interface lets an Aether-task backend replace it later.
func (r *Runner) RunSubagent(ctx context.Context, req subagent.Request) (_ subagent.Result, err error) {
	ctx, span := telemetry.StartSubagent(ctx, req.Depth)
	defer telemetry.Finish(span, &err)

	addr := req.Parent
	thread := addr.ThreadID
	if thread == "" {
		thread = "subagent"
	}
	// Make the subagent threadID UNIQUE PER INVOCATION (a process-wide counter), so
	// sibling subagents of the same parent don't collide on one threadID. Downstream
	// (sahara codeexec) keys the python kernel by threadID, so a unique threadID gives
	// each subagent invocation its own kernel; execd's idle-reap cleans them up.
	addr.ThreadID = fmt.Sprintf("%s::sub::%d", thread, nextSubagentSeq())

	auth := tools.MemoryAuthority{GrantID: req.GrantID, SubjectType: req.SubjectType, SubjectID: req.SubjectID}
	// Carry the subagent's OBO on ctx so per-turn tool discovery + dynamic tool
	// invocations act under the user's grant (mirrors Run).
	ctx = tools.WithMemoryAuthority(ctx, auth)
	session, err := harness.NewSession(ctx, addr, harness.NewMemoryStore(), r.registry, auth)
	if err != nil {
		return subagent.Result{}, fmt.Errorf("subagent session: %w", err)
	}
	taskPart, err := protocol.NewTextPart(req.Task)
	if err != nil {
		return subagent.Result{}, err
	}
	userMsg := protocol.ChatMessage{Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{taskPart}}
	if err := session.Append(ctx, userMsg); err != nil {
		return subagent.Result{}, fmt.Errorf("subagent append task: %w", err)
	}
	bootstrap, err := r.loader.LoadBootstrap(ctx)
	if err != nil {
		return subagent.Result{}, fmt.Errorf("subagent bootstrap: %w", err)
	}
	bootstrap = subagentBootstrap(bootstrap, req)
	// nil publisher => the streamer is a no-op, so the sub-agent does not emit
	// stream events to the user-facing channel.
	streamer := newTurnStreamer(nil, addr, streamMessageID(addr), r.now, r.streamFlush)
	// Discover tools relevant to the subagent's task, then apply catalog policy.
	tt := filterSubagentTools(r.assembleTurnTools(ctx, addr, userMsg), req)
	// req.Model (from spawn_subagent's model arg) is an explicit override; with it
	// empty the sub-agent uses normal capability-matched selection for its task.
	subModel := r.resolveTurnModel(ctx, addr, userMsg, req.Model)
	if req.MaxTurns > 0 {
		ctx = withToolIterationLimit(ctx, req.MaxTurns)
	}
	approvers := subagentApprovers(req)
	assistant, err := r.runProviderLoop(ctx, session, addr, userMsg, bootstrap, streamer, nil, subModel, approvers, tt)
	if err != nil {
		return subagent.Result{}, err
	}
	return subagent.Result{Text: textOf(assistant)}, nil
}

func subagentBootstrap(files []bootstrap.File, req subagent.Request) []bootstrap.File {
	instructions := strings.TrimSpace(req.Instructions)
	if instructions == "" {
		return files
	}
	out := make([]bootstrap.File, 0, len(files)+1)
	out = append(out, files...)
	name := "SUBAGENT.md"
	if req.AgentType != "" {
		name = fmt.Sprintf("SUBAGENT.%s.md", req.AgentType)
	}
	out = append(out, bootstrap.File{Name: name, Content: instructions})
	return out
}

func subagentApprovers(req subagent.Request) []hooks.ToolApprover {
	if len(req.AllowedTools) == 0 && len(req.DeniedTools) == 0 {
		return nil
	}
	return []hooks.ToolApprover{subagentToolPolicy{req: req}}
}

type subagentToolPolicy struct {
	req subagent.Request
}

func (p subagentToolPolicy) ApproveTool(_ context.Context, call hooks.ToolCall) hooks.Decision {
	if err := p.req.AllowsTool(call.Name); err != nil {
		return hooks.Deny(err.Error())
	}
	return hooks.Allow()
}

func filterSubagentTools(tt turnTools, req subagent.Request) turnTools {
	if len(req.AllowedTools) == 0 && len(req.DeniedTools) == 0 {
		return tt
	}
	specs := make([]provider.ToolSpec, 0, len(tt.specs))
	dynamicNames := make(map[string]struct{}, len(tt.dynamicNames))
	for _, spec := range tt.specs {
		if err := req.AllowsTool(spec.Name); err != nil {
			continue
		}
		specs = append(specs, spec)
		if _, ok := tt.dynamicNames[spec.Name]; ok {
			dynamicNames[spec.Name] = struct{}{}
		}
	}
	filtered := turnTools{specs: specs}
	if len(dynamicNames) > 0 {
		filtered.dynamicNames = dynamicNames
	}
	return filtered
}
