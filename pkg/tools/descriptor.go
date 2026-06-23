package tools

import (
	"encoding/json"
	"sort"
)

// Descriptor is the model-facing metadata for a tool: its name, a one-line
// description, and a JSON-schema object for its arguments. It is what the
// provider tool-API needs to expose the tool to the model.
type Descriptor struct {
	Name        string
	Description string
	Parameters  json.RawMessage // JSON-schema object; empty => {"type":"object"}
}

// Describe attaches model-facing metadata for an already-registered tool.
// Unknown names are ignored so callers can describe best-effort.
func (r *Registry) Describe(d Descriptor) {
	if d.Name == "" {
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
