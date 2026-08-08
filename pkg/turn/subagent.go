package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/bootstrap"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/compaction"
	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/ids"
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
// child thread is minted by an optional ThreadRegistrar and its canonical id is
// adopted; without one it falls back to "<parentThread>::sub::<seq>". Passing
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
	childThreadID, resume := r.resolveSubagentThread(ctx, req)
	execution, err := r.admitSubagent(ctx, req, childThreadID, false)
	if err != nil {
		return subagent.Result{}, err
	}
	if err := r.startSubagent(ctx, execution); err != nil {
		return subagent.Result{}, err
	}
	var publisher channel.Publisher
	if r.streamSubagents {
		publisher = r.publisher
	}
	result, runErr := r.runSubagentOn(ctx, req, childThreadID, resume, publisher, &execution)
	return result, r.finishSubagent(ctx, execution, runErr)
}

type subagentExecution struct {
	req           subagent.Request
	childThreadID string
	taskID        string
	admittedAt    time.Time
	model         string
	childUsage    map[string]json.RawMessage
}

// admitSubagent creates the durable execution task before projecting admission.
// A backend failure is fail-closed: no model or tool work starts without the
// execution authority accepting the child. Local mode has no task backend and
// retains the historical observer-only behavior.
func (r *Runner) admitSubagent(ctx context.Context, req subagent.Request, childThreadID string, background bool) (subagentExecution, error) {
	execution := subagentExecution{
		req:           req,
		childThreadID: childThreadID,
		admittedAt:    r.subagentLifecycleNow(),
		model:         req.Model,
	}
	if r.subagentTasks != nil {
		workspaceID := r.subagentWorkspace(req)
		taskID, err := r.subagentTasks.Admit(ctx, subagent.TaskAdmission{
			WorkspaceID:     workspaceID,
			ParentSessionID: req.Parent.ThreadID,
			ChildSessionID:  childThreadID,
			ParentTaskID:    req.Parent.TaskID,
			ParentMessageID: req.ParentMessageID,
			InvocationID:    req.InvocationID,
			Name:            subagentDisplayName(req),
			Kind:            string(req.AgentType),
			Model:           req.Model,
			Depth:           req.Depth,
			Background:      background,
			GrantID:         req.GrantID,
			SubjectType:     req.SubjectType,
			SubjectID:       req.SubjectID,
		})
		if err != nil {
			return subagentExecution{}, fmt.Errorf("subagent task admission: %w", err)
		}
		if strings.TrimSpace(taskID) == "" {
			return subagentExecution{}, errors.New("subagent task admission: backend returned an empty task id")
		}
		execution.taskID = taskID
	}
	r.observeSubagent(ctx, req, childThreadID, execution.taskID, spec.SessionSubagentAdmitted, execution.admittedAt, execution.admittedAt, "", nil)
	return execution, nil
}

// startSubagent claims the durable task before projecting running. If claim
// fails, it attempts one terminal cancellation and never runs the child.
func (r *Runner) startSubagent(ctx context.Context, execution subagentExecution) error {
	if r.subagentTasks != nil {
		if err := r.subagentTasks.Start(ctx, execution.taskID); err != nil {
			claimErr := fmt.Errorf("subagent task start: %w", err)
			terminalAt := r.subagentLifecycleNow()
			finishErr := r.subagentTasks.Finish(
				context.WithoutCancel(ctx), execution.taskID, subagent.TaskOutcomeCancelled, claimErr.Error(),
			)
			status := spec.SessionSubagentCancelled
			if finishErr != nil {
				status = spec.SessionSubagentInterrupted
				claimErr = errors.Join(claimErr, fmt.Errorf("%w: cancel unstarted task %q: %v", subagent.ErrTaskOutcomeUncertain, execution.taskID, finishErr))
			}
			r.observeSubagent(ctx, execution.req, execution.childThreadID, execution.taskID, status, execution.admittedAt, terminalAt, execution.model, execution.childUsage)
			return claimErr
		}
	}
	r.observeSubagent(ctx, execution.req, execution.childThreadID, execution.taskID, spec.SessionSubagentRunning, execution.admittedAt, r.subagentLifecycleNow(), execution.model, nil)
	return nil
}

// finishSubagent transitions the execution authority before publishing the
// terminal registry projection. A terminal operation that cannot be confirmed
// becomes interrupted/uncertain and is never automatically replayed.
func (r *Runner) finishSubagent(ctx context.Context, execution subagentExecution, runErr error) error {
	status := spec.SessionSubagentCompleted
	outcome := subagent.TaskOutcomeCompleted
	if runErr != nil {
		status = spec.SessionSubagentFailed
		outcome = subagent.TaskOutcomeFailed
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			status = spec.SessionSubagentCancelled
			outcome = subagent.TaskOutcomeCancelled
		}
	}
	terminalAt := r.subagentLifecycleNow()
	if r.subagentTasks != nil {
		reason := ""
		if runErr != nil {
			reason = runErr.Error()
		}
		if err := r.subagentTasks.Finish(context.WithoutCancel(ctx), execution.taskID, outcome, reason); err != nil {
			status = spec.SessionSubagentInterrupted
			authorityErr := fmt.Errorf("%w: finish task %q: %v", subagent.ErrTaskOutcomeUncertain, execution.taskID, err)
			r.observeSubagent(ctx, execution.req, execution.childThreadID, execution.taskID, status, execution.admittedAt, terminalAt, execution.model, execution.childUsage)
			if runErr != nil {
				return errors.Join(runErr, authorityErr)
			}
			return authorityErr
		}
	}
	r.observeSubagent(ctx, execution.req, execution.childThreadID, execution.taskID, status, execution.admittedAt, terminalAt, execution.model, execution.childUsage)
	return runErr
}

// resolveSubagentThread resolves the canonical child id before the child is
// admitted or starts work. Resume always keeps the supplied id verbatim. For a
// new child, a ThreadRegistrar gets first chance to mint a backend-owned id with
// the parent relationship already attached. The returned id is adopted by every
// downstream surface (session, lifecycle, persistence, kernel key, and handle).
// If the optional registrar is absent or unavailable, a process-unique local id
// preserves the standalone/offline behavior.
func (r *Runner) resolveSubagentThread(ctx context.Context, req subagent.Request) (childThreadID string, resume bool) {
	if req.ResumeThreadID != "" {
		return req.ResumeThreadID, true
	}
	if reg, ok := r.memory.(ThreadRegistrar); ok {
		auth := tools.MemoryAuthority{GrantID: req.GrantID, SubjectType: req.SubjectType, SubjectID: req.SubjectID}
		id, err := reg.EnsureThread(ctx, auth, ThreadSpec{
			WorkspaceID:    req.Parent.WorkspaceID,
			ParentThreadID: req.Parent.ThreadID,
			Ownership:      req.Parent.Ownership,
			Origin:         "subagent",
		})
		if err == nil && strings.TrimSpace(id) != "" {
			slog.InfoContext(ctx, "subagent: minted child thread via registrar",
				slog.String("thread", id), slog.String("parent", req.Parent.ThreadID))
			return id, false
		}
		if err != nil {
			slog.WarnContext(ctx, "subagent: registrar thread mint failed; using local id",
				slog.String("parent", req.Parent.ThreadID), slog.Any("err", err))
		} else {
			slog.WarnContext(ctx, "subagent: registrar returned an empty thread id; using local id",
				slog.String("parent", req.Parent.ThreadID))
		}
	}
	base := req.Parent.ThreadID
	if base == "" {
		base = "subagent"
	}
	return fmt.Sprintf("%s::sub::%d", base, nextSubagentSeq()), false
}

// runSubagentOn runs a bounded sub-agent turn on the resolved child thread and
// returns its final text plus the re-addressable thread handle. publisher is the
// egress the child streams to: nil for the synchronous path (silent, internal to
// the tool call); the background path passes the runner's publisher (keyed to the
// child thread id) when StreamBackgroundSubagents is on. All other behavior mirrors
// the original synchronous body: durable store (resume cold-loads via LoadHistory),
// OBO carried through, cross-thread back-ref + spawn meta on new child threads,
// and the durable commit of task+assistant. The optional thread registrar has
// already resolved the canonical id before this function is called.
func (r *Runner) runSubagentOn(ctx context.Context, req subagent.Request, childThreadID string, resume bool, publisher channel.Publisher, execution *subagentExecution) (result subagent.Result, err error) {
	ctx = withWorkingDirectoryPrompt(ctx)
	parentThread := req.Parent.ThreadID
	addr := req.Parent
	addr.ThreadID = childThreadID
	if execution.taskID != "" {
		addr.TaskID = execution.taskID
	}

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
	// publisher is nil for a synchronous sub-agent (silent, internal to the tool
	// call) and the runner's publisher for a streamed background sub-agent (keyed to
	// the child thread id, so a client can subscribe to that thread to render it).
	streamer := newTurnStreamer(publisher, addr, streamMessageID(addr), r.now, r.streamFlush)
	if err := streamer.start(ctx); err != nil {
		return subagent.Result{}, fmt.Errorf("subagent publish start: %w", err)
	}
	// Discover tools relevant to the subagent's task, then apply catalog policy.
	tt := filterSubagentTools(r.assembleTurnTools(ctx, addr, userMsg), req)
	// req.Model (from spawn_subagent's model arg) is an explicit override; with it
	// empty the sub-agent uses normal capability-matched selection for its task.
	subModel := r.resolveTurnModel(ctx, addr, userMsg, req.Model)
	execution.model = subModel
	if req.MaxTurns > 0 {
		ctx = withToolIterationLimit(ctx, req.MaxTurns)
	}
	approvers := subagentApprovers(req)
	assistant, err := r.runProviderLoop(ctx, session, addr, userMsg, bootstrap, streamer, nil, subModel, approvers, tt)
	if err != nil {
		if publisher != nil {
			_ = publisher.PublishEvent(ctx, channel.Event{Type: channel.EventError, Addr: addr})
		}
		return subagent.Result{}, err
	}
	assistant, err = streamer.finalize(ctx, assistant)
	if err != nil {
		return subagent.Result{}, fmt.Errorf("subagent publish final: %w", err)
	}
	// Persist the child thread to memory so the sub-agent is durable and recallable
	// in sahara (MemoryLayer). No auto-recall: resume continuity comes from the
	// store's LoadHistory cold-load. Commit the task message AND the assistant
	// (never assistant-only here): the assistant-only mode exists because a host
	// pre-commits the parent thread's user turn, but NOTHING pre-commits this
	// internally-minted child task message — and it is the ONLY message carrying the
	// cross-thread back-ref (MessageRef{parent_thread_id, parent_message_id}) + spawn
	// meta. Dropping it would leave the child→parent linkage nowhere in memory except
	// the "::sub::" thread-name convention. On resume the task message has no ref
	// (skipped above), so committing it is still correct (just the follow-up turn).
	r.commitMessages(ctx, auth, addr, []protocol.ChatMessage{userMsg, assistant})
	execution.childUsage = sessionUsageProjection(assistant)
	text := textOf(assistant)
	return subagent.Result{Text: text, ThreadID: childThreadID, Summary: summarizeSubagent(text)}, nil
}

// StartBackground runs a sub-agent DETACHED: it resolves (or resumes) the child
// thread, launches the run on a cancellation-independent context (so it survives
// the spawning parent turn ending, mirroring the async memory-commit path), and
// returns the child thread handle immediately. On completion it pushes a notice
// back to the parent thread (via the Notifier) that wakes a fresh parent turn, so
// the parent can react to the actual outcome. Concurrent detached children are
// capped by bgSem. Returns ErrBackgroundUnsupported when no Notifier is wired.
func (r *Runner) StartBackground(ctx context.Context, req subagent.Request) (string, error) {
	if r.notifier == nil {
		return "", subagent.ErrBackgroundUnsupported
	}
	childThreadID, resume := r.resolveSubagentThread(ctx, req)
	execution, err := r.admitSubagent(ctx, req, childThreadID, true)
	if err != nil {
		return "", err
	}
	// Snapshot the parent OBO so the woken completion turn can act as the same
	// principal. It is handed off out-of-band via a single-use token (never the
	// credential on the message); a zero authority (local dev, no OBO) yields an
	// empty token and the notice carries no authority.
	parentAuth := tools.MemoryAuthority{GrantID: req.GrantID, SubjectType: req.SubjectType, SubjectID: req.SubjectID}
	// Detach from the parent turn's context: the child — and its completion push —
	// must outlive the turn that spawned it (WithoutCancel mirrors the async commit).
	bgCtx := context.WithoutCancel(ctx)
	go func() {
		// Cap concurrent detached children; excess spawns queue on the semaphore.
		r.bgSem <- struct{}{}
		defer func() { <-r.bgSem }()
		var err error
		sctx, span := telemetry.StartSubagent(bgCtx, req.Depth)
		defer func() { telemetry.Finish(span, &err) }()
		if err = r.startSubagent(sctx, execution); err != nil {
			r.notifySubagentComplete(sctx, req, childThreadID, parentAuth, subagent.Result{}, err)
			return
		}
		var publisher channel.Publisher
		if r.streamBackgroundSubagents {
			publisher = r.publisher
		}
		var res subagent.Result
		res, err = r.runSubagentOn(sctx, req, childThreadID, resume, publisher, &execution)
		err = r.finishSubagent(sctx, execution, err)
		r.notifySubagentComplete(sctx, req, childThreadID, parentAuth, res, err)
	}()
	return childThreadID, nil
}

func (r *Runner) subagentLifecycleNow() time.Time {
	if r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
}

func (r *Runner) observeSubagent(
	ctx context.Context,
	req subagent.Request,
	childThreadID string,
	taskID string,
	status spec.SessionSubagentStatus,
	createdAt, updatedAt time.Time,
	model string,
	childUsage map[string]json.RawMessage,
) {
	if r.subagentObserver == nil {
		return
	}
	workspaceID := r.subagentWorkspace(req)
	record := spec.SessionSubagentRecord{
		ID:              childThreadID,
		ParentSessionID: req.Parent.ThreadID,
		ChildSessionID:  childThreadID,
		TaskID:          taskID,
		Name:            subagentDisplayName(req),
		Kind:            string(req.AgentType),
		Model:           model,
		Depth:           uint32(max(req.Depth, 0)),
		Status:          status,
		CreatedAt:       createdAt.Format(time.RFC3339Nano),
		UpdatedAt:       updatedAt.Format(time.RFC3339Nano),
		ChildUsage:      childUsage,
	}
	if status == spec.SessionSubagentCompleted || status == spec.SessionSubagentFailed || status == spec.SessionSubagentCancelled || status == spec.SessionSubagentInterrupted {
		record.CompletedAt = record.UpdatedAt
	}
	if err := r.subagentObserver.ObserveSubagent(context.WithoutCancel(ctx), subagent.LifecycleEvent{
		WorkspaceID: workspaceID,
		Record:      record,
	}); err != nil {
		slog.WarnContext(ctx, "subagent: record lifecycle failed",
			slog.String("thread", childThreadID), slog.String("status", string(status)), slog.Any("err", err))
	}
}

func (r *Runner) subagentWorkspace(req subagent.Request) string {
	if req.Parent.WorkspaceID != "" {
		return req.Parent.WorkspaceID
	}
	return r.subagentDefaultWorkspace
}

func sessionUsageProjection(message protocol.ChatMessage) map[string]json.RawMessage {
	raw := message.Meta[compaction.MetaUsage]
	if len(raw) == 0 {
		return nil
	}
	var usage map[string]json.RawMessage
	if err := json.Unmarshal(raw, &usage); err != nil {
		return nil
	}
	return usage
}

// notifySubagentComplete pushes a completion notice for a background sub-agent
// back to its PARENT thread, waking a fresh parent turn. The notice is a user-role
// message carrying a short status line plus a terminal SubagentPart (for the UI);
// the full result stays durable on the child thread (reachable via thread=<child>).
// The parent's OBO is handed off via an opaque single-use token stamped in the
// message meta — resolved by the distribution's Authority func against the handoff
// store, never trusted as a credential — so the woken turn acts as the same
// principal without a gateway round-trip. A fresh TaskID/RequestID keeps the notice
// a distinct turn so it never clobbers the parent's in-flight turn (the runtime
// keys per-turn cancellation by TaskID). Best-effort: an enqueue failure is logged;
// the child thread is durably committed regardless.
func (r *Runner) notifySubagentComplete(ctx context.Context, req subagent.Request, childThreadID string, parentAuth tools.MemoryAuthority, res subagent.Result, runErr error) {
	if r.notifier == nil {
		return
	}
	status := protocol.SubagentCompleted
	summary := res.Summary
	if runErr != nil {
		status = protocol.SubagentFailed
		summary = summarizeSubagent("sub-agent failed: " + runErr.Error())
	}
	name := subagentDisplayName(req)
	addr := req.Parent
	addr.ThreadID = req.Parent.ThreadID
	addr.TaskID = noticeID("task-")
	addr.RequestID = noticeID("req-")

	text := fmt.Sprintf("[background sub-agent %s] %s (thread %s)\n\n%s", status, name, childThreadID, summary)
	textPart, err := protocol.NewTextPart(text)
	if err != nil {
		slog.WarnContext(ctx, "subagent: build completion notice failed", slog.Any("err", err))
		return
	}
	content := []protocol.ContentPart{textPart}
	if sp, perr := protocol.NewSubagentPart(protocol.SubagentPart{
		ID:       childThreadID,
		Name:     name,
		ThreadID: childThreadID,
		Status:   status,
		Summary:  summary,
	}); perr == nil {
		content = append(content, sp)
	}
	msg := protocol.ChatMessage{Role: protocol.RoleUser, Addr: addr, Content: content}
	msg.Meta = stampAuthorityHandoff(msg.Meta, r.authHandoff.Put(parentAuth))

	if err := r.notifier.Enqueue(ctx, channel.Inbound{Addr: addr, Message: msg}); err != nil {
		slog.WarnContext(ctx, "subagent: enqueue completion notice failed",
			slog.String("thread", childThreadID),
			slog.String("parent", req.Parent.ThreadID), slog.Any("err", err))
	}
}

// subagentDisplayName resolves a display name for the completion notice: the
// catalog agent name/type when the spawn used one, else a generic "subagent".
func subagentDisplayName(req subagent.Request) string {
	if req.AgentName != "" {
		return string(req.AgentName)
	}
	if req.AgentType != "" {
		return string(req.AgentType)
	}
	return "subagent"
}

// noticeID mints a fresh random id for a completion notice's TaskID/RequestID,
// falling back to the bare prefix if the RNG fails (never expected).
func noticeID(prefix string) string {
	id, err := ids.New(prefix)
	if err != nil {
		return prefix
	}
	return id
}

// stampAuthorityHandoff records the OBO handoff token on the notice message under
// meta["scitrera"].authority_handoff. An empty token (no OBO to hand off) is a
// no-op. The token is a lookup key into the harness-private handoff store, NOT a
// credential: the distribution's Authority func resolves it there, and an unknown
// token simply yields the default authority.
func stampAuthorityHandoff(meta map[string]json.RawMessage, token string) map[string]json.RawMessage {
	if token == "" {
		return meta
	}
	if meta == nil {
		meta = map[string]json.RawMessage{}
	}
	raw, err := json.Marshal(map[string]any{"authority_handoff": token})
	if err != nil {
		return meta
	}
	meta["scitrera"] = raw
	return meta
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
	route := make(map[string]ToolProvider, len(tt.providerByTool))
	for _, spec := range tt.specs {
		if err := req.AllowsTool(spec.Name); err != nil {
			continue
		}
		specs = append(specs, spec)
		if p, ok := tt.providerByTool[spec.Name]; ok {
			route[spec.Name] = p
		}
	}
	filtered := turnTools{specs: specs}
	if len(route) > 0 {
		filtered.providerByTool = route
	}
	return filtered
}
