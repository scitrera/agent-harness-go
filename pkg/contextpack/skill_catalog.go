package contextpack

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/sysprompt"
)

// SkillCatalog is the workspace-resolved skill surface for one turn. It
// overrides the assembler's static/default catalog only on the context carrying
// it, so one long-lived runner can safely serve multiple logical workspaces.
type SkillCatalog struct {
	Skills       []sysprompt.SkillSummary
	Bodies       SkillBodyResolver
	LoadWarnings []string
}

type skillCatalogKey struct{}

// WithSkillCatalog installs a defensive copy of the workspace-resolved skill
// listing for prompt assembly.
func WithSkillCatalog(ctx context.Context, catalog SkillCatalog) context.Context {
	catalog.Skills = append([]sysprompt.SkillSummary(nil), catalog.Skills...)
	catalog.LoadWarnings = append([]string(nil), catalog.LoadWarnings...)
	return context.WithValue(ctx, skillCatalogKey{}, catalog)
}

func skillCatalogFrom(ctx context.Context) (SkillCatalog, bool) {
	catalog, ok := ctx.Value(skillCatalogKey{}).(SkillCatalog)
	return catalog, ok
}
