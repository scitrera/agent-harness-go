// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package commands

// Surface identifies a command presentation or dispatch surface.
type Surface string

const (
	SurfaceRunner Surface = "runner"
	SurfaceTUI    Surface = "tui"
)

// Owner identifies the component that handles a command after metadata-driven
// discovery. Handlers remain in their owning packages.
type Owner string

const (
	OwnerRunner Owner = "runner"
	OwnerTUI    Owner = "tui"
)

// CommandDef is the declarative metadata shared by reserved-name checks, help,
// completion, and command catalogs. Aliases are expanded by Definitions.
type CommandDef struct {
	Name         string
	Aliases      []string
	Description  string
	ArgumentHint string
	Owner        Owner
	Surfaces     []Surface
	AliasFor     string
}

var builtinDefinitions = []CommandDef{
	{Name: "help", Aliases: []string{"commands"}, Description: "List available commands.", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "clear", Description: "Clear this thread's history.", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "model", Aliases: []string{"models"}, Description: "List available models, or switch models.", ArgumentHint: "[list|switch <name>]", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "reasoning", Description: "Show or set reasoning effort for this model and thread.", ArgumentHint: "[none|minimal|low|medium|high|xhigh|max|default]", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "schedules", Description: "Inspect authoritative scheduled-turn definitions.", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "runs", Description: "Inspect authoritative scheduled-turn runs.", ArgumentHint: "[--status S] [--limit N] [--cursor C]", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "refinements", Description: "Browse the authoritative continual-refinement audit.", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "ledger", Description: "Browse branch-aware execution events.", Owner: OwnerRunner, Surfaces: []Surface{SurfaceRunner, SurfaceTUI}},
	{Name: "thread", Aliases: []string{"threads"}, Description: "Manage threads.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "status", Description: "Show status.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "shell-response", Description: "Configure whether idle !commands trigger an agent response.", ArgumentHint: "[status|on|off|default|user on|off|default]", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "pwd", Description: "Show working directory.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "cd", Description: "Change working directory.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "cancel", Description: "Cancel the active task.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "tools", Description: "Inspect tool activity.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "attach", Aliases: []string{"attachments"}, Description: "Attach or inspect queued files.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "tasks", Aliases: []string{"task"}, Description: "Manage task state.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "team", Description: "Manage the team graph.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "agents", Aliases: []string{"agent"}, Description: "Inspect agents.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "approvals", Description: "Review approvals.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "approve", Description: "Approve a pending request.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "deny", Description: "Deny a pending request.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "permissions", Aliases: []string{"permission"}, Description: "Manage permissions.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "requirements", Description: "Show resolved requirements.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "mcp", Description: "Show MCP state.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "hooks", Description: "Show hook state.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "worldstate", Description: "Show compacted world state.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "compact", Description: "Inspect or compact context.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
	{Name: "quit", Aliases: []string{"exit"}, Description: "Quit the TUI.", Owner: OwnerTUI, Surfaces: []Surface{SurfaceTUI}},
}

// Definitions returns detached definitions for a surface, with aliases expanded
// into independently displayable entries pointing at their canonical command.
func Definitions(surface Surface) []CommandDef {
	var out []CommandDef
	for _, definition := range builtinDefinitions {
		if !supportsSurface(definition, surface) {
			continue
		}
		copyDef := cloneDefinition(definition)
		out = append(out, copyDef)
		for _, alias := range definition.Aliases {
			aliasDef := copyDef
			aliasDef.Name = alias
			aliasDef.Aliases = nil
			aliasDef.AliasFor = definition.Name
			out = append(out, aliasDef)
		}
	}
	return out
}

// CanonicalBuiltin resolves a known built-in or alias. Unknown names are
// returned in canonical-key form with ok=false.
func CanonicalBuiltin(name string) (canonical string, ok bool) {
	key := CanonicalKey(name)
	for _, definition := range builtinDefinitions {
		if CanonicalKey(definition.Name) == key {
			return definition.Name, true
		}
		for _, alias := range definition.Aliases {
			if CanonicalKey(alias) == key {
				return definition.Name, true
			}
		}
	}
	return key, false
}

func supportsSurface(definition CommandDef, surface Surface) bool {
	for _, candidate := range definition.Surfaces {
		if candidate == surface {
			return true
		}
	}
	return false
}

func cloneDefinition(definition CommandDef) CommandDef {
	definition.Aliases = append([]string(nil), definition.Aliases...)
	definition.Surfaces = append([]Surface(nil), definition.Surfaces...)
	return definition
}
