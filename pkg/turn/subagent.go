package turn

import (
	"context"
	"encoding/json"
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

// RunSubagent runs a bounded sub-agent turn on its OWN durable, persisted child
// thread and returns its final text plus the re-addressable thread handle. It
// reuses the runner's tools, bootstrap loader, context manager, and model. A new
// child thread is minted as "<parentThread>::sub::<seq>"; passing
// Request.ResumeThreadID instead CONTINUES that existing child thread (resume
// continuity comes for free from the injected store's LoadHistory cold-load).
// The child thread's task message back-references the spawning message via
// MessageRef{parent_thread_id, parent_message_id}, so the sub-agent is durable,
// recallable (a thread query), and re-addressable. The parent's OBO grant is
// carried through so tools act on behalf of the same principal; no stream egress
// (the sub-agent is internal) and no auto-recall (resume covers continuity).
//
// This is the in-process backend for the spawn_subagent tool; the
// subagent.Runner interface lets an Aether-task backend replace it later.
func (r *Runner) RunSubagent(ctx context.Context, req subagent.Request) (_ subagent.Result, err error) {
	ctx, span := telemetry.StartSubagent(ctx, req.Depth)
	defer telemetry.Finish(span, &err)

	parentThread := req.Parent.ThreadID
	// Resolve the child thread id: resume an existing child thread verbatim, or
	// mint a NEW one unique per invocation (a process-wide counter) so sibling
	// subagents of the same parent don't collide. Downstream (sahara codeexec)
	// keys the python kernel by threadID, so a unique threadID gives each new
	// subagent its own kernel; a resumed thread reuses its handle.
	resume := req.ResumeThreadID != ""
	childThreadID := req.ResumeThreadID
	if !resume {
		base := parentThread
		if base == "" {
			base = "subagent"
		}
		childThreadID = fmt.Sprintf("%s::sub::%d", base, nextSubagentSeq())
	}
	addr := req.Parent
	addr.ThreadID = childThreadID

	auth := tools.MemoryAuthority{GrantID: req.GrantID, SubjectType: req.SubjectType, SubjectID: req.SubjectID}
	// Carry the subagent's OBO on ctx so per-turn tool discovery + dynamic tool
	// invocations act under the user's grant (mirrors Run).
	ctx = tools.WithMemoryAuthority(ctx, auth)
	// Use the DURABLE store (not a throwaway in-memory one): on resume, the
	// injected store cold-loads the child thread's prior history via LoadHistory,
	// giving resume-continuity without an explicit recall.
	session, err := harness.NewSession(ctx, addr, r.store, r.registry, auth)
	if err != nil {
		return subagent.Result{}, fmt.Errorf("subagent session: %w", err)
	}
	taskPart, err := protocol.NewTextPart(req.Task)
	if err != nil {
		return subagent.Result{}, err
	}
	userMsg := protocol.ChatMessage{Role: protocol.RoleUser, Addr: addr, Content: []protocol.ContentPart{taskPart}}
	if !resume {
		// New child thread: stamp the cross-thread back-ref to the spawning message
		// and the spawn provenance meta. On resume the thread already exists, so we
		// skip both (the follow-up is just another turn on the same child thread).
		userMsg.Ref = &protocol.MessageRef{ParentThreadID: parentThread, ParentMessageID: req.ParentMessageID}
		userMsg.Meta = stampSubagentSpawnMeta(userMsg.Meta, req.Parent.AgentID)
	}
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
	// Persist the child thread to memory exactly like Run (assistant-only vs
	// user+assistant per memAutoCommitAsstOnly) so the sub-agent is durable and
	// recallable in sahara (MemoryLayer). No auto-recall: resume continuity comes
	// from the store's LoadHistory cold-load.
	r.commitToMemory(ctx, auth, addr, userMsg, assistant)
	text := textOf(assistant)
	return subagent.Result{Text: text, ThreadID: childThreadID, Summary: summarizeSubagent(text)}, nil
}

// stampSubagentSpawnMeta records spawn provenance on the child thread's task
// message under meta["scitrera"].spawn = {"by": <parent agent id>, "kind":
// "delegation"} (mirrors how other turn code stamps meta as json.RawMessage).
func stampSubagentSpawnMeta(meta map[string]json.RawMessage, parentAgentID string) map[string]json.RawMessage {
	if meta == nil {
		meta = map[string]json.RawMessage{}
	}
	raw, err := json.Marshal(map[string]any{
		"spawn": map[string]any{"by": parentAgentID, "kind": "delegation"},
	})
	if err != nil {
		return meta
	}
	meta["scitrera"] = raw
	return meta
}

// summarizeSubagent returns a compact one-line digest of the sub-agent answer
// (first line, trimmed, capped at ~200 chars) for the parent to keep as a
// reference alongside the thread handle.
func summarizeSubagent(text string) string {
	s := strings.TrimSpace(text)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	if len(s) > 200 {
		s = strings.TrimSpace(s[:200])
	}
	return s
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
