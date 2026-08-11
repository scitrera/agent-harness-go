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
	// TrustRequiresFreshApproval requires a new once-only human decision for
	// every invocation. Session and durable grants cannot bypass it. Use it for
	// mutations whose exact arguments, not merely the tool name, need review.
	TrustRequiresFreshApproval
)

// ConcurrencyClass declares whether invocations of a tool may overlap other
// calls from the same assistant message. This is execution metadata only: it is
// intentionally not sent to the model or inferred from a tool name.
//
// The zero value is conservative. A call is eligible for concurrent execution
// only when every call in the assistant's batch is explicitly ParallelSafe.
type ConcurrencyClass int

const (
	// ConcurrencyUnspecified keeps the tool sequential. This is the descriptor
	// zero value so existing and dynamically discovered tools fail closed.
	ConcurrencyUnspecified ConcurrencyClass = iota
	// ConcurrencySequential explicitly requires source-ordered execution.
	ConcurrencySequential
	// ConcurrencyParallelSafe promises that overlapping invocations do not
	// mutate shared state, do not use turn-mutating emitters, and are safe under
	// the turn's shared cancellation. Effects must be returned in Result.
	ConcurrencyParallelSafe
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
	// Concurrency is the tool's neutral runtime concurrency contract. It is not
	// part of the model-facing function schema. Unspecified is sequential.
	Concurrency ConcurrencyClass
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
