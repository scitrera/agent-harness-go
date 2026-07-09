package tools

import (
	"encoding/json"
	"sort"
)

// TrustLevel is a tool's authorization hint carried on its Descriptor: how the
// authorization pipeline should treat it before the runtime policy weighs in.
// The zero value (TrustDefault) preserves today's behavior — the local
// requires-approval trigger stays the runtime policy, not this hint.
type TrustLevel int

const (
	// TrustDefault leaves authorization to the runtime policy (local tools) or
	// the provider's own gating (provider tools). Zero value; today's behavior.
	TrustDefault TrustLevel = iota
	// TrustRequiresApproval marks a tool that must be approved before it runs.
	TrustRequiresApproval
	// TrustPreAuthorized marks a tool authorized ahead of time (no prompt); it
	// still passes through the safety authorizer like every other tool.
	TrustPreAuthorized
)

// Descriptor is the model-facing metadata for a tool: its name, a one-line
// description, and a JSON-schema object for its arguments. It is what the
// provider tool-API needs to expose the tool to the model.
type Descriptor struct {
	Name        string
	Description string
	Parameters  json.RawMessage // JSON-schema object; empty => {"type":"object"}
	// Trust is the authorization hint for the tool. Zero value (TrustDefault)
	// leaves authorization to the runtime policy; a provider may stamp a stronger
	// hint (a later stage — no local descriptor sets it today).
	Trust TrustLevel
}

// Describe attaches model-facing metadata for an already-registered tool.
// Unknown names are ignored so callers can describe best-effort.
func (r *Registry) Describe(d Descriptor) {
	if d.Name == "" || r.isExcluded(d.Name) {
		return
	}
	if r.descriptors == nil {
		r.descriptors = map[string]Descriptor{}
	}
	r.descriptors[d.Name] = d
}

// Descriptors returns descriptors for all registered tools, sorted by name.
// Tools registered without an explicit descriptor get a name-only entry so the
// model still learns they exist.
func (r *Registry) Descriptors() []Descriptor {
	out := make([]Descriptor, 0, len(r.handlers))
	for name := range r.handlers {
		if d, ok := r.descriptors[name]; ok {
			out = append(out, d)
		} else {
			out = append(out, Descriptor{Name: name})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
