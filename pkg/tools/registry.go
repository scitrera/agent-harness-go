package tools

import (
	"context"
	"fmt"
	"sort"
)

type Registry struct {
	handlers    map[string]Handler
	descriptors map[string]Descriptor
	policy      Policy
	audit       AuditSink
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

func (r *Registry) Register(name string, handler Handler) error {
	if name == "" || handler == nil {
		return fmt.Errorf("%w: name and handler required", ErrInvalidTool)
	}
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("%w: %s", ErrToolExists, name)
	}
	r.handlers[name] = handler
	return nil
}

func (r *Registry) Invoke(ctx context.Context, req Request) (Result, error) {
	handler, exists := r.handlers[req.Name]
	if !exists {
		return Result{}, fmt.Errorf("%w: %s", ErrUnknownTool, req.Name)
	}
	if err := r.authorize(ctx, req); err != nil {
		return Result{}, err
	}
	result, err := handler.Invoke(ctx, req)
	if err != nil {
		return Result{}, fmt.Errorf("invoke %s: %w", req.Name, err)
	}
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

func (r *Registry) authorize(ctx context.Context, req Request) error {
	if r.policy == nil {
		return nil
	}
	decision := r.policy.Decide(req)
	if r.audit != nil {
		if err := r.audit.Record(ctx, NewAuditRecord(req, decision)); err != nil {
			return fmt.Errorf("record tool audit: %w", err)
		}
	}
	if !decision.Allowed() {
		return policyError(decision, req.Name)
	}
	return nil
}
