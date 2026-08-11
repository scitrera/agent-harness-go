package tools

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type Registry struct {
	handlers    map[string]Handler
	descriptors map[string]Descriptor
	excluded    map[string]struct{}
	policy      Policy
	audit       AuditSink
	events      ToolEventSink
}

// PreparedInvocation is a registry call whose policy, audit, and execution-view
// checks have already succeeded. Preparing a batch in assistant source order
// lets the turn runner settle every approval before any explicitly safe call is
// started. A prepared invocation is immutable and may be invoked once.
type PreparedInvocation struct {
	registry    *Registry
	request     Request
	handler     Handler
	decision    Decision
	hasDecision bool
	used        *atomic.Bool
}

type preparationError struct {
	err         error
	decision    Decision
	hasDecision bool
}

func (e *preparationError) Error() string { return e.err.Error() }
func (e *preparationError) Unwrap() error { return e.err }

func NewRegistry() *Registry {
	return &Registry{handlers: map[string]Handler{}, descriptors: map[string]Descriptor{}}
}

func NewAuditedRegistry(policy Policy, audit AuditSink) *Registry {
	return &Registry{handlers: map[string]Handler{}, descriptors: map[string]Descriptor{}, policy: policy, audit: audit}
}

func (r *Registry) SetPolicy(policy Policy, audit AuditSink) {
	r.policy = policy
	r.audit = audit
}

func (r *Registry) SetEventSink(events ToolEventSink) {
	r.events = events
}

// SetExcluded marks tool names that must NOT enter the registry: subsequent
// Register/Override/Describe calls for an excluded name become silent no-ops, so
// the tool is invisible to the model (absent from Descriptors) and uninvocable
// (Invoke returns ErrUnknownTool). Call before registering tools; additive across
// calls. This is the general "drop a built-in tool" seam (e.g. a distribution
// that provides web_search as a skill instead of the built-in Exa tool).
func (r *Registry) SetExcluded(names []string) {
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			if r.excluded == nil {
				r.excluded = map[string]struct{}{}
			}
			r.excluded[n] = struct{}{}
		}
	}
}

func (r *Registry) isExcluded(name string) bool {
	_, ok := r.excluded[name]
	return ok
}

func (r *Registry) Register(name string, handler Handler) error {
	if name == "" || handler == nil {
		return fmt.Errorf("%w: name and handler required", ErrInvalidTool)
	}
	if r.isExcluded(name) {
		return nil // excluded by config; silently skip so registration callers don't fail
	}
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("%w: %s", ErrToolExists, name)
	}
	r.handlers[name] = handler
	return nil
}

// Override registers handler under name, replacing any existing handler for that
// name (unlike Register, which errors on a duplicate). Used to swap an
// already-registered tool implementation — e.g. redirecting the local "python"
// tool to a sandboxed execd backend. Descriptors are left untouched; re-describe
// separately if the schema changes.
func (r *Registry) Override(name string, handler Handler) error {
	if name == "" || handler == nil {
		return fmt.Errorf("%w: name and handler required", ErrInvalidTool)
	}
	if r.isExcluded(name) {
		return nil
	}
	r.handlers[name] = handler
	return nil
}

func (r *Registry) Invoke(ctx context.Context, req Request) (Result, error) {
	prepared, err := r.Prepare(ctx, req)
	if err != nil {
		started := time.Now()
		r.emitToolEvent(ctx, NewToolEvent(ToolEventStarted, req))
		event := finishedToolEvent(req, started, Result{}, err)
		var prepErr *preparationError
		if errors.As(err, &prepErr) && prepErr.hasDecision {
			event.PolicyDecision = string(prepErr.decision.Code)
			event.Reason = prepErr.decision.Reason
		}
		r.emitToolEvent(ctx, event)
		return Result{}, err
	}
	return prepared.Invoke(ctx)
}

// Prepare resolves a tool and performs every admission check without executing
// its body. Authorization is deliberately separated from Invoke so a caller can
// preflight an entire explicitly parallel-safe batch deterministically.
func (r *Registry) Prepare(ctx context.Context, req Request) (PreparedInvocation, error) {
	handler, exists := r.handlers[req.Name]
	if !exists {
		return PreparedInvocation{}, fmt.Errorf("%w: %s", ErrUnknownTool, req.Name)
	}
	decision, hasDecision, err := r.authorize(ctx, req)
	if err != nil {
		return PreparedInvocation{}, &preparationError{err: err, decision: decision, hasDecision: hasDecision}
	}
	// An exact view's write ceiling is a hard execution-authority boundary. It
	// runs after ordinary policy/audit but cannot be bypassed by a one-shot tool
	// approval: approval may admit a tool, never broaden the selected view.
	if err = authorizeExecutionScope(ctx, req); err != nil {
		return PreparedInvocation{}, err
	}
	return PreparedInvocation{registry: r, request: req, handler: handler, decision: decision, hasDecision: hasDecision, used: &atomic.Bool{}}, nil
}

// Invoke executes the already-admitted call body. It does not repeat policy or
// approval checks; the execution-view ceiling was captured by Prepare.
func (p PreparedInvocation) Invoke(ctx context.Context) (Result, error) {
	if p.registry == nil || p.handler == nil || p.used == nil {
		return Result{}, fmt.Errorf("%w: invocation was not prepared", ErrInvalidTool)
	}
	if !p.used.CompareAndSwap(false, true) {
		return Result{}, fmt.Errorf("%w: prepared invocation was already used", ErrInvalidTool)
	}
	r := p.registry
	req := p.request
	started := time.Now()
	r.emitToolEvent(ctx, NewToolEvent(ToolEventStarted, req))
	var result Result
	var err error
	if delegate := ToolDelegateFrom(ctx); delegate != nil && delegate.HandlesTool(req.Name) {
		result, err = delegate.InvokeTool(ctx, req)
	} else {
		result, err = p.handler.Invoke(ctx, req)
	}
	if err != nil {
		wrapped := fmt.Errorf("invoke %s: %w", req.Name, err)
		r.emitToolEvent(ctx, finishedToolEvent(req, started, result, wrapped))
		return result, wrapped
	}
	event := finishedToolEvent(req, started, result, nil)
	if p.hasDecision {
		event.PolicyDecision = string(p.decision.Code)
		event.Reason = p.decision.Reason
	}
	r.emitToolEvent(ctx, event)
	return result, nil
}

func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) authorize(ctx context.Context, req Request) (Decision, bool, error) {
	if r.policy == nil {
		return Decision{}, false, nil
	}
	var decision Decision
	if req.Approved {
		// The user just authorized this call via the approval flow; bypass the
		// policy gate for this single invocation (still audited).
		decision = Decision{Code: DecisionAllow, Reason: "approved by user", AuditCode: "tool.user_approved"}
	} else {
		decision = r.policy.Decide(req)
	}
	if r.audit != nil {
		if err := r.audit.Record(ctx, NewAuditRecord(req, decision)); err != nil {
			return decision, true, fmt.Errorf("record tool audit: %w", err)
		}
	}
	if !decision.Allowed() {
		return decision, true, policyError(decision, req.Name)
	}
	return decision, true, nil
}

func (r *Registry) emitToolEvent(ctx context.Context, event ToolEvent) {
	if r.events == nil {
		return
	}
	_ = r.events.EmitToolEvent(ctx, event)
}

func finishedToolEvent(req Request, started time.Time, result Result, err error) ToolEvent {
	event := NewToolEvent(ToolEventFinished, req)
	event.DurationMS = DurationMillis(time.Since(started))
	event.Result = result.Metadata
	event.Result.PayloadBytes = len(result.Payload)
	event.IsError = result.IsError || err != nil
	ApplySafeError(&event, err)
	return event
}
