package turn

import (
	"sort"

	"github.com/scitrera/agent-harness-go/pkg/commands"
)

// AvailableCommands returns runner-owned slash commands for command palettes.
// The returned slice is detached from the registry and safe for callers to
// sort or filter.
func (r *Runner) AvailableCommands() []commands.Command {
	out := make([]commands.Command, 0, len(commands.ReservedNames)+r.commands.Len())
	for name, description := range commands.ReservedNames {
		hint := ""
		if name == "model" || name == "models" {
			hint = "[list|switch <name>]"
		}
		out = append(out, commands.Command{
			Name:         name,
			Description:  description,
			ArgumentHint: hint,
		})
	}
	out = append(out, r.commands.List()...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
