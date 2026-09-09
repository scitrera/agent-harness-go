// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// load_skill tool: the model loads a skill by NAME (not a guessed path). The
// tool resolves it against the configured skill roots, reads the SKILL.md body,
// and — following the skill's metadata.scitrera.prereq_skills frontmatter — also
// loads its prerequisite skills first (transitively, deduped, in dependency
// order). It replaces the error-prone read_file("skills/<name>/SKILL.md"), which
// fails when skills live at an absolute system-skill root (e.g. /skills) rather
// than under the workspace.
//
// Each load is recorded on the per-turn tools.WorldStateSink (invoked skills),
// which survives compaction — so the "which skills have I loaded, and how long
// ago" ledger persists even after the loaded bodies are compacted out of context.
package skills

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// LoadToolName is the registry name of the load_skill tool.
const LoadToolName = "load_skill"

// maxSkillBodyBytes bounds how much of a SKILL.md we read into a body.
const maxSkillBodyBytes = 256 << 10

// LoadableSkill is a resolved, body-loaded skill plus its declared prerequisites.
type LoadableSkill struct {
	Name        string
	Description string
	Path        string
	Body        string
	Prereqs     []string
	// PreferredModel is the skill's declared default model (metadata.scitrera.
	// preferred_model). When set and available, load_skill pins the thread to it —
	// no-op otherwise. "" means no preference.
	PreferredModel string
	// AllowedTools is the skill's declared tool scope (parsed from the SKILL.md
	// `allowed-tools` frontmatter key). Surfaced to the model in the load_skill
	// result only — NOT enforced; tool-call restriction against this list is a
	// turn-loop follow-up.
	AllowedTools []string
}

// Registry indexes resolved skills by name for the load_skill handler.
type Registry struct {
	byName map[string]*LoadableSkill
	order  []string
}

// DirectPrereqs returns each skill's declared direct prerequisite names (from
// metadata.scitrera.prereq_skills). Skills with no prerequisites are omitted. A
// relevance strategy uses this to avoid listing a prerequisite that a shown,
// higher-ranked dependent will auto-load. The returned map + slices are copies.
func (r *Registry) DirectPrereqs() map[string][]string {
	out := make(map[string][]string, len(r.byName))
	for name, sk := range r.byName {
		if len(sk.Prereqs) > 0 {
			out[name] = append([]string(nil), sk.Prereqs...)
		}
	}
	return out
}

// skillFrontmatter is the subset of SKILL.md YAML frontmatter load_skill needs.
// yaml.v3 ignores unknown keys, so existing SKILL.md files (no prereqs) parse
// fine and yield an empty slice.
type skillFrontmatter struct {
	Metadata struct {
		Scitrera struct {
			PrereqSkills   []string `yaml:"prereq_skills"`
			PreferredModel string   `yaml:"preferred_model"`
		} `yaml:"scitrera"`
	} `yaml:"metadata"`
}

// BuildRegistry loads each spec's body and parses its prereq frontmatter.
// Inline Content is used directly; otherwise workspaceRoot resolves relative
// paths and absolute paths (image-baked system-skill roots) are read as-is.
// Unreadable specs are skipped (best-effort).
func BuildRegistry(specs []catalog.SkillSpec, workspaceRoot string) *Registry {
	r := &Registry{byName: make(map[string]*LoadableSkill, len(specs))}
	for _, s := range specs {
		if s.Name == "" || (s.Path == "" && s.Content == "") {
			continue
		}
		if _, dup := r.byName[s.Name]; dup {
			continue // first spec wins (discovery already ordered fs-over-catalog)
		}
		body, ok := resolveSkillBody(s, workspaceRoot)
		if !ok {
			continue
		}
		// Prefer prereqs/model carried on the spec (a MemoryLayer catalog strips
		// SKILL.md frontmatter into a metadata field, so the body has none); fall
		// back to parsing the body frontmatter for the on-disk SKILL.md path.
		prereqs := s.Prereqs
		if len(prereqs) == 0 {
			prereqs = parsePrereqs(body)
		}
		preferredModel := s.PreferredModel
		if preferredModel == "" {
			preferredModel = parsePreferredModel(body)
		}
		r.byName[s.Name] = &LoadableSkill{
			Name:           s.Name,
			Description:    s.Description,
			Path:           s.Path,
			Body:           body,
			Prereqs:        prereqs,
			PreferredModel: preferredModel,
			AllowedTools:   s.AllowedTools,
		}
		r.order = append(r.order, s.Name)
	}
	return r
}

func resolveSkillBody(s catalog.SkillSpec, workspaceRoot string) (string, bool) {
	if s.Content != "" {
		body := s.Content
		if int64(len(body)) > maxSkillBodyBytes {
			body = body[:maxSkillBodyBytes]
		}
		return body, true
	}
	if s.Path == "" {
		return "", false
	}
	path := s.Path
	if !filepath.IsAbs(path) {
		path = filepath.Join(workspaceRoot, path)
	}
	body, err := readSkillBody(path, maxSkillBodyBytes)
	return body, err == nil
}

// Len reports how many skills the registry resolved.
func (r *Registry) Len() int { return len(r.order) }

// Body returns a resolved skill's SKILL.md body by name. It satisfies
// contextpack.SkillBodyResolver, so a relevance provider's auto-realized skills
// can be injected into context without a load_skill call.
func (r *Registry) Body(name string) (string, bool) {
	sk, ok := r.byName[name]
	if !ok || sk == nil {
		return "", false
	}
	return sk.Body, true
}

// Names returns the resolvable skill names in stable order (for error messages).
func (r *Registry) Names() []string { return append([]string(nil), r.order...) }

func readSkillBody(path string, max int64) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if int64(len(data)) > max {
		data = data[:max]
	}
	return string(data), nil
}

// parsePrereqs extracts metadata.scitrera.prereq_skills from a SKILL.md's leading
// `---` YAML frontmatter block.
func parsePrereqs(body string) []string {
	block, ok := frontmatterBlock(body)
	if !ok {
		return nil
	}
	var fm skillFrontmatter
	if err := yaml.Unmarshal([]byte(block), &fm); err != nil {
		return nil
	}
	var out []string
	for _, p := range fm.Metadata.Scitrera.PrereqSkills {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parsePreferredModel extracts metadata.scitrera.preferred_model from a SKILL.md's
// leading `---` YAML frontmatter block ("" when absent). load_skill switches the
// thread to this model when it is registered/available.
func parsePreferredModel(body string) string {
	block, ok := frontmatterBlock(body)
	if !ok {
		return ""
	}
	var fm skillFrontmatter
	if err := yaml.Unmarshal([]byte(block), &fm); err != nil {
		return ""
	}
	return strings.TrimSpace(fm.Metadata.Scitrera.PreferredModel)
}

// frontmatterBlock returns the YAML between the leading `---` fences, if present.
func frontmatterBlock(body string) (string, bool) {
	s := strings.TrimLeft(body, "\ufeff \t\r\n")
	if !strings.HasPrefix(s, "---") {
		return "", false
	}
	rest := s[len("---"):]
	if i := strings.IndexByte(rest, '\n'); i >= 0 {
		rest = rest[i+1:]
	} else {
		return "", false
	}
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

// loadOrder resolves the transitive prereq tree via post-order DFS: prerequisites
// precede dependents, the target is last. Cycles are broken (back-edges skipped)
// and missing prereqs are reported as warnings, not errors.
func (r *Registry) loadOrder(target string, includePrereqs bool) (order []string, warnings []string) {
	if !includePrereqs {
		return []string{target}, nil
	}
	visited := make(map[string]bool)
	onstack := make(map[string]bool)
	var visit func(name string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		if onstack[name] {
			warnings = append(warnings, fmt.Sprintf("prereq cycle broken at %q", name))
			return
		}
		sk, ok := r.byName[name]
		if !ok {
			warnings = append(warnings, fmt.Sprintf("prereq %q not found; skipped", name))
			return
		}
		onstack[name] = true
		prereqs := append([]string(nil), sk.Prereqs...)
		sort.Strings(prereqs)
		for _, p := range prereqs {
			visit(p)
		}
		delete(onstack, name)
		visited[name] = true
		order = append(order, name)
	}
	visit(target)
	return order, warnings
}

type loadedSkillOut struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Path        string `json:"path"`
	Body        string `json:"body"`
	Role        string `json:"role"` // "target" or "prerequisite"
	// AllowedTools surfaces the skill's declared tool scope (see LoadableSkill).
	// Advisory only — not enforced here.
	AllowedTools []string `json:"allowed_tools,omitempty"`
}

// LoadTool returns the load_skill handler bound to the registry. It records each
// emitted skill on the per-turn WorldStateSink so the invoked-skills ledger
// survives compaction.
func LoadTool(reg *Registry) tools.HandlerFunc {
	return func(ctx context.Context, req tools.Request) (tools.Result, error) {
		active := reg
		if contextual, ok := registryFrom(ctx); ok {
			active = contextual
		}
		var args struct {
			Name           string `json:"name"`
			IncludePrereqs *bool  `json:"include_prereqs"`
		}
		if len(req.Arguments) > 0 {
			if err := json.Unmarshal(req.Arguments, &args); err != nil {
				return errorResult(req, fmt.Sprintf("invalid load_skill arguments: %v", err))
			}
		}
		name := strings.TrimSpace(args.Name)
		if name == "" {
			return errorResult(req, "load_skill requires a non-empty skill name")
		}
		if active == nil {
			return errorResult(req, "no skill catalog is available")
		}
		if _, ok := active.byName[name]; !ok {
			return errorResult(req, fmt.Sprintf("unknown skill %q; available skills: %s",
				name, strings.Join(active.Names(), ", ")))
		}
		includePrereqs := true
		if args.IncludePrereqs != nil {
			includePrereqs = *args.IncludePrereqs
		}
		order, warnings := active.loadOrder(name, includePrereqs)

		sink, hasSink := tools.WorldStateSinkFrom(ctx)
		out := make([]loadedSkillOut, 0, len(order))
		prereqs := make([]string, 0, len(order))
		for _, n := range order {
			sk := active.byName[n]
			if sk == nil {
				continue
			}
			role := "prerequisite"
			if n == name {
				role = "target"
			} else {
				prereqs = append(prereqs, sk.Name)
			}
			out = append(out, loadedSkillOut{
				Name: sk.Name, Description: sk.Description, Path: sk.Path, Body: sk.Body, Role: role,
				AllowedTools: sk.AllowedTools,
			})
			if hasSink {
				// Record every emitted skill (target + prereqs) as invoked — they were
				// all loaded into context this turn.
				sink.RecordInvokedSkill(sk.Name, sk.Path)
			}
		}
		// Observability: how many prerequisites this load pulled in (and which). A
		// prereq_count of 0 for a skill that should have them is the signal that its
		// metadata.scitrera.prereq_skills didn't reach the registry (e.g. a skill
		// seeded before the frontmatter-metadata fix, so it must be re-seeded).
		slog.InfoContext(ctx, "load_skill: loaded skill",
			slog.String("skill", name),
			slog.Bool("include_prereqs", includePrereqs),
			slog.Int("prereq_count", len(prereqs)),
			slog.Any("prereqs", prereqs))

		res := struct {
			Skills        []loadedSkillOut `json:"skills"`
			Warnings      []string         `json:"warnings,omitempty"`
			ModelSwitched string           `json:"model_switched,omitempty"`
		}{Skills: out, Warnings: warnings}
		// Honor the TARGET skill's preferred_model: pin the thread's model when it's
		// available (best-effort — no-op without a model registry / preference fn, or
		// if the model isn't registered). Effective from the next turn, like /model.
		if target := active.byName[name]; target != nil {
			preferred := target.PreferredModel
			switched := false
			if preferred != "" {
				if pref, ok := tools.ModelPreferenceFrom(ctx); ok && pref(preferred) {
					res.ModelSwitched = preferred
					switched = true
				}
			}
			// Log the model preference outcome every load: preferred_model is ""
			// when the skill declared none (or the metadata didn't survive seeding);
			// switched=false means it was declared but unavailable/not applied.
			slog.InfoContext(ctx, "load_skill: model preference",
				slog.String("skill", name),
				slog.String("preferred_model", preferred),
				slog.Bool("switched", switched))
		}
		// Materialize the loaded skills' bundle files (utils.py, references/, assets/,
		// the shared bundle) into the sandbox's shared /skills dir so code can resolve
		// their /skills/<name>/... paths. Best-effort + idempotent; no-op without a
		// realizer (non-relay transport / no materialize dir).
		if realize, ok := tools.SkillRealizerFrom(ctx); ok {
			_, _ = realize(ctx, order)
		}
		result, err := jsonResult(req, res, false)
		if err != nil {
			return tools.Result{}, err
		}
		for _, loaded := range out {
			result.Metadata.References = append(result.Metadata.References, tools.ResultReference{
				System: "skill-catalog", Kind: loaded.Role, ID: loaded.Name,
			})
		}
		return result, nil
	}
}

// LoadDescriptor is the load_skill tool schema advertised to the model.
func LoadDescriptor() tools.Descriptor {
	return tools.Descriptor{
		Name: LoadToolName,
		Description: "Load a skill by name. Resolves the skill's SKILL.md across the configured " +
			"skill paths (use this INSTEAD of read_file for skills), and automatically loads any " +
			"prerequisite skills first, in dependency order. Returns each skill's full instructions.",
		Parameters: json.RawMessage(`{"type":"object","properties":{` +
			`"name":{"type":"string","description":"Skill name as listed under ## Skills, e.g. p0-docx-workbench"},` +
			`"include_prereqs":{"type":"boolean","description":"Also load prerequisite skills first (default true)"}` +
			`},"required":["name"]}`),
	}
}

func jsonResult(req tools.Request, payload interface{}, isErr bool) (tools.Result, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return tools.Result{}, err
	}
	return tools.Result{CallID: req.CallID, Name: req.Name, Payload: data, IsError: isErr}, nil
}

func errorResult(req tools.Request, message string) (tools.Result, error) {
	return jsonResult(req, struct {
		Error string `json:"error"`
	}{Error: message}, true)
}
