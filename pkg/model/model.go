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
// routing policies (e.g. a sahara cost router's "light"/"primary"). Provider is
// the optional NAME of a ProviderConfig (in the registry's provider set) this
// model routes to; empty means "use the runner's single configured provider".
type Model struct {
	Name         string       `json:"name" yaml:"name"`
	Capabilities Capabilities `json:"capabilities" yaml:"capabilities"`
	Tier         string       `json:"tier,omitempty" yaml:"tier,omitempty"`
	Provider     string       `json:"provider,omitempty" yaml:"provider,omitempty"`
	// Context is the model's total context window in tokens (0 = unknown). Used to
	// budget compaction to the model actually being called — critical when a turn
	// escalates from a large-context orchestrator to a smaller-context vision model,
	// so history is trimmed to fit the target instead of overflowing it.
	Context int `json:"context,omitempty" yaml:"context,omitempty"`
}

// ProviderConfig is a named upstream a model can be routed to: a base URL, an
// inline API key (APIKey) or the env var holding it (APIKeyEnv), and a wire
// Format ("openai"/"native"). It is pure data — construction of an actual
// provider client from it lives in the turn package (which imports provider),
// keeping this package free of a provider dependency. Missing fields are filled
// from the runner's env default by the resolver, not here.
type ProviderConfig struct {
	Name      string `json:"name" yaml:"name"`
	BaseURL   string `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	APIKey    string `json:"api_key,omitempty" yaml:"api_key,omitempty"`
	APIKeyEnv string `json:"api_key_env,omitempty" yaml:"api_key_env,omitempty"`
	Format    string `json:"format,omitempty" yaml:"format,omitempty"`
}

// Registry is the set of available models + the default. Built from config
// (sahara) or left nil (oss falls back to the runner's single configured model).
type Registry struct {
	byName    map[string]Model
	order     []string // declaration order, for stable List()
	defName   string
	providers map[string]ProviderConfig
}

// NewRegistry builds a registry from models + a default name. Duplicate names
// keep the first occurrence. defaultName need not be present (Default() reports
// absence); callers decide how to handle a missing default.
func NewRegistry(models []Model, defaultName string) *Registry {
	return NewRegistryWithProviders(models, defaultName, nil)
}

// NewRegistryWithProviders is NewRegistry plus a set of named provider configs a
// model can reference (Model.Provider). Duplicate provider names keep the first
// occurrence. Providers with an empty Name are skipped.
func NewRegistryWithProviders(models []Model, defaultName string, providers []ProviderConfig) *Registry {
	r := &Registry{
		byName:    make(map[string]Model, len(models)),
		defName:   defaultName,
		providers: make(map[string]ProviderConfig, len(providers)),
	}
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
	for _, p := range providers {
		if p.Name == "" {
			continue
		}
		if _, dup := r.providers[p.Name]; dup {
			continue
		}
		r.providers[p.Name] = p
	}
	return r
}

// ProviderFor returns the ProviderConfig the named model references. It reports
// (_, false) when the registry is nil, the model is unknown, the model declares
// no provider, or the referenced provider name is not configured — in every one
// of those cases the caller should fall back to the default provider.
func (r *Registry) ProviderFor(modelName string) (ProviderConfig, bool) {
	if r == nil {
		return ProviderConfig{}, false
	}
	m, ok := r.byName[modelName]
	if !ok || m.Provider == "" {
		return ProviderConfig{}, false
	}
	pc, ok := r.providers[m.Provider]
	if !ok {
		return ProviderConfig{}, false
	}
	return pc, true
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
