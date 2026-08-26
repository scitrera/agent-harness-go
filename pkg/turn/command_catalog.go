package turn

import (
	"sort"

	"github.com/scitrera/agent-harness-go/pkg/commands"
)

// AvailableCommands returns runner-owned slash commands for command palettes.
// The returned slice is detached from the registry and safe for callers to
// sort or filter.
func (r *Runner) AvailableCommands() []commands.Command {
	definitions := commands.Definitions(commands.SurfaceRunner)
	out := make([]commands.Command, 0, len(definitions)+r.commands.Len())
	for _, definition := range definitions {
		out = append(out, commands.Command{
			Name:         definition.Name,
			Description:  definition.Description,
			ArgumentHint: definition.ArgumentHint,
		})
	}
	out = append(out, r.commands.List()...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
