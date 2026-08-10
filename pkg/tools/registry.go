package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	handler, exists := r.handlers[req.Name]
	if !exists {
		return Result{}, fmt.Errorf("%w: %s", ErrUnknownTool, req.Name)
	}
	started := time.Now()
	r.emitToolEvent(ctx, NewToolEvent(ToolEventStarted, req))
	decision, hasDecision, err := r.authorize(ctx, req)
	if err != nil {
		event := finishedToolEvent(req, started, Result{}, err)
		if hasDecision {
			event.PolicyDecision = string(decision.Code)
			event.Reason = decision.Reason
		}
		r.emitToolEvent(ctx, event)
		return Result{}, err
	}
	var result Result
	if delegate := ToolDelegateFrom(ctx); delegate != nil && delegate.HandlesTool(req.Name) {
		result, err = delegate.InvokeTool(ctx, req)
	} else {
		result, err = handler.Invoke(ctx, req)
	}
	if err != nil {
		wrapped := fmt.Errorf("invoke %s: %w", req.Name, err)
		r.emitToolEvent(ctx, finishedToolEvent(req, started, result, wrapped))
		return result, wrapped
	}
	event := finishedToolEvent(req, started, result, nil)
	if hasDecision {
		event.PolicyDecision = string(decision.Code)
		event.Reason = decision.Reason
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
