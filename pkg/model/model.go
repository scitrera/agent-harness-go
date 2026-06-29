// Package model defines a neutral model registry + per-turn model-selection seam.
//
// It is policy-free: oss ships a Registry (the available models + a default) and
// a Selector interface whose default impl (CapabilityDefault) only enforces
// capability matching. A distribution (e.g. sahara) plugs in a Selector that
// applies cost/complexity routing — that policy lives there, NOT in oss core.
package model

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// Capabilities describes what a model supports. Extensible; v1 gates on Vision
// (the modality that actually varies across frontier models) and Tools (the
// harness cannot function without tool-calling).
type Capabilities struct {
	Vision bool `json:"vision" yaml:"vision"`
	Tools  bool `json:"tools" yaml:"tools"`
	Audio  bool `json:"audio" yaml:"audio"`
}

// Satisfies reports whether c covers every capability required by req.
func (c Capabilities) Satisfies(req Capabilities) bool {
	return (!req.Vision || c.Vision) && (!req.Tools || c.Tools) && (!req.Audio || c.Audio)
}

// Model is a registry entry: a logical name (what the provider/sidecar routes to
// a real upstream) plus capabilities and an open Tier label for distribution-side
// routing policies (e.g. a sahara cost router's "light"/"primary").
type Model struct {
	Name         string       `json:"name" yaml:"name"`
	Capabilities Capabilities `json:"capabilities" yaml:"capabilities"`
	Tier         string       `json:"tier,omitempty" yaml:"tier,omitempty"`
}

// Registry is the set of available models + the default. Built from config
// (sahara) or left nil (oss falls back to the runner's single configured model).
type Registry struct {
	byName  map[string]Model
	order   []string // declaration order, for stable List()
	defName string
}

// NewRegistry builds a registry from models + a default name. Duplicate names
// keep the first occurrence. defaultName need not be present (Default() reports
// absence); callers decide how to handle a missing default.
func NewRegistry(models []Model, defaultName string) *Registry {
	r := &Registry{byName: make(map[string]Model, len(models)), defName: defaultName}
	for _, m := range models {
		if m.Name == "" {
			continue
		}
		if _, dup := r.byName[m.Name]; dup {
			continue
		}
		r.byName[m.Name] = m
		r.order = append(r.order, m.Name)
	}
	return r
}

// Get returns the model with the given name.
func (r *Registry) Get(name string) (Model, bool) {
	if r == nil {
		return Model{}, false
	}
	m, ok := r.byName[name]
	return m, ok
}

// Default returns the registry's default model.
func (r *Registry) Default() (Model, bool) {
	if r == nil {
		return Model{}, false
	}
	return r.Get(r.defName)
}

// DefaultName returns the configured default model name (may be empty).
func (r *Registry) DefaultName() string {
	if r == nil {
		return ""
	}
	return r.defName
}

// List returns all models in declaration order.
func (r *Registry) List() []Model {
	if r == nil {
		return nil
	}
	out := make([]Model, 0, len(r.order))
	for _, name := range r.order {
		out = append(out, r.byName[name])
	}
	return out
}

// Capable returns the models (declaration order) that satisfy req.
func (r *Registry) Capable(req Capabilities) []Model {
	if r == nil {
		return nil
	}
	out := make([]Model, 0, len(r.order))
	for _, name := range r.order {
		if m := r.byName[name]; m.Capabilities.Satisfies(req) {
			out = append(out, m)
		}
	}
	return out
}

// SelectInput is the per-turn context handed to a Selector. Required is the
// capability set this turn needs (e.g. Vision when the message carries images);
// Default is the runner's configured default model name.
type SelectInput struct {
	Addr     protocol.MessageAddress
	User     protocol.ChatMessage
	Required Capabilities
	Registry *Registry
	Default  string
	// Attempts are the models that already failed this turn, in order (empty on
	// the initial pick). The runner re-consults the Selector after a *retryable*
	// provider failure that same-model recovery (backoff, context trim) could not
	// resolve, so a Selector can avoid a model that just failed and escalate
	// (e.g. to a larger-context or higher tier). Returning "" declines further
	// fallback and surfaces the error. oss's CapabilityDefault always declines on
	// retry — cross-model fallback is opt-in via a distribution Selector.
	Attempts []Attempt
}

// Attempt records a model that failed within the current turn. Reason is the
// provider failure classification as a string (kept a string so this package
// stays free of a provider dependency); empty when unknown.
type Attempt struct {
	Model  string
	Reason string
}

// Selector picks the model name for a turn. oss calls it for the auto path only
// (an explicit /model or spawn(model=) override bypasses it). The default impl is
// CapabilityDefault; a distribution plugs in a cost/complexity router.
type Selector interface {
	SelectModel(ctx context.Context, in SelectInput) (string, error)
}

// CapabilityDefault is oss's neutral Selector: it keeps the configured default
// when that model satisfies the turn's required capabilities, otherwise it falls
// back to the first capable model in the registry, otherwise to the default
// (best-effort — selection never fails the turn). No cost/complexity policy.
type CapabilityDefault struct{}

func (CapabilityDefault) SelectModel(_ context.Context, in SelectInput) (string, error) {
	// Pure initial-pick: oss never switches models on failure. Once a model has
	// failed this turn (the runner re-consults the Selector after a retryable
	// failure), decline so the error surfaces. Cross-model fallback is opt-in via
	// a distribution Selector (e.g. the sahara cost router).
	if len(in.Attempts) > 0 {
		return "", nil
	}
	def := in.Default
	if m, ok := in.Registry.Get(def); ok && m.Capabilities.Satisfies(in.Required) {
		return def, nil
	}
	if capable := in.Registry.Capable(in.Required); len(capable) > 0 {
		return capable[0].Name, nil
	}
	return def, nil
}
