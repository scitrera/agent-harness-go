// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package contextpack

import (
	"context"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// SkillRelevanceProvider selects and ranks the skills relevant to the current
// turn, and may nominate skills to auto-realize (inject their body into context
// without a load_skill tool call). It is the seam a MemoryLayer-backed strategy
// plugs into; the default (nil provider) lists the full catalog and auto-realizes
// nothing — unchanged behavior.
//
// Implementations should be fast and best-effort: RankSkills errors are swallowed
// by the assembler, which falls back to the full catalog listing. Ranking must not
// mutate the request.
type SkillRelevanceProvider interface {
	RankSkills(ctx context.Context, req SkillRelevanceRequest) (SkillRelevanceResult, error)
}

// SkillCandidate is one skill the model may use, as the provider sees it: the
// listing metadata only (name + description), never the body — relevance is judged
// from the description and the conversation, and bodies are large.
type SkillCandidate struct {
	Name        string
	Description string
}

// RealizedSkill is a skill whose body is (or recently was) in context because it
// was loaded via load_skill, with its age in turns (0 = this turn). The provider
// uses it to avoid re-nominating already-loaded skills; the assembler uses it to
// dedup auto-realization (a fresh, already-loaded skill is not re-injected).
type RealizedSkill struct {
	Name     string
	AgeTurns int
}

// SkillRelevanceRequest is the per-turn input to a provider.
type SkillRelevanceRequest struct {
	// Available is the full catalog the model may use (listing metadata only).
	Available []SkillCandidate
	// History is the recent conversation the provider judges relevance against.
	History []protocol.ChatMessage
	// Realized names skills already loaded via load_skill this session, with age.
	Realized []RealizedSkill
	// Turn is the in-flight turn number (monotonic, survives compaction).
	Turn int
}

// SkillListMode controls how a provider's ranked listing interacts with the full
// catalog in the prompt.
type SkillListMode int

const (
	// ListRefine reorders the catalog by the ranked order and annotates the ranked
	// entries; catalog skills the provider did not rank still appear, after.
	ListRefine SkillListMode = iota
	// ListReplace shows ONLY the ranked skills; unranked catalog skills are hidden.
	ListReplace
)

// RankedSkill is one entry in a provider's relevance listing. Note is an optional
// short hint surfaced next to the skill in the prompt ("matches the current task").
type RankedSkill struct {
	Name string
	Note string
}

// SkillRelevanceResult is a provider's per-turn output.
type SkillRelevanceResult struct {
	// Listed, when non-empty, is the relevance-ordered listing for the prompt's
	// skills section. Empty preserves the full catalog listing unchanged.
	Listed []RankedSkill
	// Mode decides how Listed interacts with the full catalog (refine vs replace).
	Mode SkillListMode
	// Realize names skills to inject (body) this turn. The assembler resolves each
	// body via the SkillBodyResolver, skips any that are already realized+fresh
	// (dedup) or unresolvable, and caps the total injected bytes — so a provider
	// may nominate freely.
	Realize []string
	// Suppressed counts catalog skills the provider omitted from Listed because they
	// are prerequisites of a shown skill (they auto-load with it). The assembler
	// excludes these from the "+N more available" hint — a covered prerequisite is
	// not a coverage gap. 0 when the provider does no prerequisite suppression.
	Suppressed int
}

// SkillBodyResolver returns a skill's SKILL.md body by name for auto-realization.
// The harness's skill registry satisfies it. A nil resolver disables
// auto-realization even when a provider nominates skills.
type SkillBodyResolver func(name string) (body string, ok bool)
