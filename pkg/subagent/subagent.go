// Package subagent defines the contract for delegating a bounded sub-task to a
// nested agent. The in-process implementation (internal/turn) runs an ephemeral
// turn; the interface is intentionally backend-agnostic so a future Aether-task
// implementation (spawning a separate sandbox/agent) can satisfy the same
// contract without changing the spawn tool.
package subagent

import (
	"context"
	"errors"
	"sync"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// ErrNoRunner is returned when a sub-agent is requested but no runner is wired.
var ErrNoRunner = errors.New("subagent: no runner configured")

// ErrBackgroundUnsupported is returned when a background spawn is requested but
// the installed runner does not implement BackgroundRunner (e.g. a backend
// without a detached-execution seam). The spawn tool treats it as a signal to
// fall back to a synchronous run rather than failing the call.
var ErrBackgroundUnsupported = errors.New("subagent: background execution not supported")

// Request is a sub-task delegation. Authority fields carry the parent turn's OBO
// grant (as primitives to avoid importing the tools package and creating a
// cycle) so the sub-agent acts on behalf of the same principal.
type Request struct {
	Task        string
	Depth       int
	Parent      protocol.MessageAddress
	GrantID     string
	SubjectType string
	SubjectID   string
	// Model optionally pins the sub-agent to a specific model (validated against
	// the registry by the runner; empty → the runner's normal per-turn selection).
	Model string
	// ResumeThreadID, when set, CONTINUES an existing child sub-agent thread
	// (used verbatim as the child thread id) instead of minting a new
	// "<parentThread>::sub::<seq>" thread. It is the handle returned as
	// Result.ThreadID from a prior spawn; empty → create a new child thread.
	ResumeThreadID string
	// ParentMessageID is the id of the spawning parent message, recorded on the
	// child thread's task message as MessageRef.ParentMessageID (the cross-thread
	// back-ref). Empty is acceptable (the back-ref still carries the parent
	// thread id).
	ParentMessageID string
	// Agent fields are populated when a file-backed catalog definition is used.
	// The OSS in-process runner applies Instructions, MaxTurns, AllowedTools,
	// DeniedTools, and Model. Skills, MCPServers, PermissionMode,
	// ExecPolicyHint, and Background are retained for catalog-compatible
	// backends until OSS has concrete runtime seams for them.
	AgentName      AgentName
	AgentType      AgentType
	Instructions   string
	MaxTurns       int
	AllowedTools   []string
	DeniedTools    []string
	Skills         []string
	MCPServers     []string
	PermissionMode PermissionMode
	ExecPolicyHint string
	Background     bool
}

// Result is the sub-agent's final answer.
type Result struct {
	Text string
	// ThreadID is the child sub-agent thread's id — the re-addressable handle.
	// Pass it back as Request.ResumeThreadID to continue the same sub-agent.
	ThreadID string
	// Summary is a compact one-line digest of the answer (first line / first
	// ~200 chars), suitable for the parent to keep as a reference.
	Summary string
}

// Runner executes a sub-agent request.
type Runner interface {
	RunSubagent(ctx context.Context, req Request) (Result, error)
}

// BackgroundRunner is a Runner that can additionally start a sub-agent DETACHED:
// StartBackground mints (or resumes) the child thread, launches the run
// asynchronously, and returns the child thread id — the re-addressable handle —
// immediately, without waiting for the child to finish. The backend delivers the
// child's completion out-of-band (the in-process backend pushes a notice back to
// the parent thread that wakes a fresh parent turn). A backend that cannot detach
// need not implement this; the spawn tool falls back to a synchronous RunSubagent.
type BackgroundRunner interface {
	Runner
	StartBackground(ctx context.Context, req Request) (threadID string, err error)
}

// LifecycleEvent is an authoritative child-session registry update. The record
// uses the shared session protocol shape so local files, MemoryLayer adapters,
// Aether checkpoints, and remote clients project the same lifecycle fields.
type LifecycleEvent struct {
	WorkspaceID string
	Record      spec.SessionSubagentRecord
}

// LifecycleObserver records subagent admission/running/terminal transitions.
// Runner execution treats observation as best-effort; recovery surfaces fail
// closed when a configured registry cannot be read or validated.
type LifecycleObserver interface {
	ObserveSubagent(ctx context.Context, event LifecycleEvent) error
}

type depthKey struct{}

// WithDepth returns a context carrying the sub-agent nesting depth.
func WithDepth(ctx context.Context, depth int) context.Context {
	return context.WithValue(ctx, depthKey{}, depth)
}

// Depth reports the current sub-agent nesting depth (0 at the top level).
func Depth(ctx context.Context) int {
	if v, ok := ctx.Value(depthKey{}).(int); ok {
		return v
	}
	return 0
}

// Ref is a late-bound Runner: the spawn tool can be registered (and thus
// surfaced to the model in the tool list) before the concrete runner exists,
// then wired via Set. RunSubagent returns ErrNoRunner until Set is called.
type Ref struct {
	mu     sync.RWMutex
	runner Runner
}

// Set installs the concrete runner.
func (r *Ref) Set(runner Runner) {
	r.mu.Lock()
	r.runner = runner
	r.mu.Unlock()
}

// RunSubagent delegates to the installed runner.
func (r *Ref) RunSubagent(ctx context.Context, req Request) (Result, error) {
	r.mu.RLock()
	runner := r.runner
	r.mu.RUnlock()
	if runner == nil {
		return Result{}, ErrNoRunner
	}
	return runner.RunSubagent(ctx, req)
}

// StartBackground forwards to the installed runner when it is a BackgroundRunner.
// It returns ErrNoRunner before Set is called and ErrBackgroundUnsupported when
// the installed runner lacks the detached-execution seam — so *Ref satisfies
// BackgroundRunner (a spawn tool holding the ref can attempt a background spawn)
// while still degrading cleanly on a backend that doesn't support it.
func (r *Ref) StartBackground(ctx context.Context, req Request) (string, error) {
	r.mu.RLock()
	runner := r.runner
	r.mu.RUnlock()
	if runner == nil {
		return "", ErrNoRunner
	}
	bg, ok := runner.(BackgroundRunner)
	if !ok {
		return "", ErrBackgroundUnsupported
	}
	return bg.StartBackground(ctx, req)
}
