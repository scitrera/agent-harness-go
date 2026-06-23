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

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// ErrNoRunner is returned when a sub-agent is requested but no runner is wired.
var ErrNoRunner = errors.New("subagent: no runner configured")

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
}

// Result is the sub-agent's final answer.
type Result struct {
	Text string
}

// Runner executes a sub-agent request.
type Runner interface {
	RunSubagent(ctx context.Context, req Request) (Result, error)
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
