package turn

import (
	"context"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/telemetry"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

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
	addr.ThreadID = thread + "::sub"

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
	// nil publisher => the streamer is a no-op, so the sub-agent does not emit
	// stream events to the user-facing channel.
	streamer := newTurnStreamer(nil, addr, streamMessageID(addr), r.now, r.streamFlush)
	// Discover tools relevant to the subagent's task (best-effort, same as Run).
	tt := r.assembleTurnTools(ctx, addr, userMsg)
	// req.Model (from spawn_subagent's model arg) is an explicit override; with it
	// empty the sub-agent uses normal capability-matched selection for its task.
	subModel := r.resolveTurnModel(ctx, addr, userMsg, req.Model)
	assistant, err := r.runProviderLoop(ctx, session, addr, userMsg, bootstrap, streamer, nil, subModel, nil, tt)
	if err != nil {
		return subagent.Result{}, err
	}
	return subagent.Result{Text: textOf(assistant)}, nil
}
