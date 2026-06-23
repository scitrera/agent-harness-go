package commands

import "sort"

// Registry holds discovered workspace commands, keyed for hyphen/underscore- and
// case-insensitive lookup. A nil *Registry is usable (empty). First occurrence
// of a key wins, so callers should pass commands in precedence order.
type Registry struct {
	byKey map[string]Command
	order []string
}

// New builds a Registry from the given commands (deduplicated by canonical key;
// first occurrence wins).
func New(cmds []Command) *Registry {
	r := &Registry{byKey: make(map[string]Command, len(cmds))}
	for _, c := range cmds {
		key := CanonicalKey(c.Name)
		if key == "" {
			continue
		}
		if _, exists := r.byKey[key]; exists {
			continue
		}
		r.byKey[key] = c
		r.order = append(r.order, key)
	}
	return r
}

// Lookup resolves a command by name (hyphen/underscore- and case-insensitive).
func (r *Registry) Lookup(name string) (Command, bool) {
	if r == nil {
		return Command{}, false
	}
	c, ok := r.byKey[CanonicalKey(name)]
	return c, ok
}

// List returns all commands sorted by name.
func (r *Registry) List() []Command {
	if r == nil {
		return nil
	}
	out := make([]Command, 0, len(r.order))
	for _, k := range r.order {
		out = append(out, r.byKey[k])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Len reports the number of registered commands.
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	return len(r.byKey)
}
