package subagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var ErrUnknownAgent = errors.New("subagent: unknown agent")

type Catalog interface {
	List(ctx context.Context) ([]Definition, error)
	Get(ctx context.Context, typ AgentType) (Definition, error)
}

type FileCatalog struct {
	dir string
}

func NewFileCatalog(dir string) *FileCatalog {
	return &FileCatalog{dir: dir}
}

func (c *FileCatalog) List(ctx context.Context) ([]Definition, error) {
	if strings.TrimSpace(c.dir) == "" {
		return nil, fmt.Errorf("%w: catalog dir is required", ErrInvalidDefinition)
	}
	paths, err := catalogJSONFiles(c.dir)
	if err != nil {
		return nil, err
	}
	defs := make([]Definition, 0, len(paths))
	seen := map[AgentType]string{}
	for _, path := range paths {
		loaded, err := loadDefinitionFile(ctx, path)
		if err != nil {
			return nil, err
		}
		for _, def := range loaded {
			if prev, ok := seen[def.Type]; ok {
				return nil, fmt.Errorf("%w: duplicate type %s in %s and %s", ErrInvalidDefinition, def.Type, prev, path)
			}
			seen[def.Type] = path
			defs = append(defs, def)
		}
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Type < defs[j].Type })
	return defs, nil
}

func (c *FileCatalog) Get(ctx context.Context, typ AgentType) (Definition, error) {
	defs, err := c.List(ctx)
	if err != nil {
		return Definition{}, err
	}
	for _, def := range defs {
		if def.Type == typ {
			return def, nil
		}
	}
	return Definition{}, fmt.Errorf("%w: %s", ErrUnknownAgent, typ)
}

func catalogJSONFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read agent catalog: %w", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

func loadDefinitionFile(ctx context.Context, path string) ([]Definition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read agent definition %s: %w", path, err)
	}
	defs, err := decodeDefinitions(data)
	if err != nil {
		return nil, fmt.Errorf("decode agent definition %s: %w", path, err)
	}
	base := filepath.Dir(path)
	for i, def := range defs {
		loaded, err := loadPrompt(ctx, base, def)
		if err != nil {
			return nil, err
		}
		if err := loaded.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		defs[i] = loaded
	}
	return defs, nil
}

func decodeDefinitions(data []byte) ([]Definition, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if looksLikeDefinition(raw) {
		var one rawDefinition
		if err := json.Unmarshal(data, &one); err != nil {
			return nil, err
		}
		return []Definition{one.definition("")}, nil
	}
	var many map[string]rawDefinition
	if err := json.Unmarshal(data, &many); err != nil {
		return nil, err
	}
	defs := make([]Definition, 0, len(many))
	keys := make([]string, 0, len(many))
	for key := range many {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		defs = append(defs, many[key].definition(key))
	}
	return defs, nil
}

func looksLikeDefinition(raw map[string]json.RawMessage) bool {
	for _, key := range []string{"type", "agentType", "prompt", "prompt_path", "instructions_path", "max_turns", "maxTurns"} {
		if _, ok := raw[key]; ok {
			return true
		}
	}
	return false
}

func loadPrompt(ctx context.Context, base string, def Definition) (Definition, error) {
	if strings.TrimSpace(def.Prompt) != "" || strings.TrimSpace(def.PromptPath) == "" {
		return def, nil
	}
	path, err := localCatalogPath(base, def.PromptPath)
	if err != nil {
		return Definition{}, err
	}
	if err := ctx.Err(); err != nil {
		return Definition{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Definition{}, fmt.Errorf("read agent prompt %s: %w", def.PromptPath, err)
	}
	def.Prompt = string(data)
	return def, nil
}

func localCatalogPath(base string, relPath string) (string, error) {
	if filepath.IsAbs(relPath) {
		return "", fmt.Errorf("%w: prompt_path must be relative", ErrInvalidDefinition)
	}
	path := filepath.Clean(filepath.Join(base, relPath))
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return "", fmt.Errorf("resolve prompt_path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: prompt_path escapes catalog dir", ErrInvalidDefinition)
	}
	return path, nil
}

type rawDefinition struct {
	Name                string         `json:"name"`
	Type                string         `json:"type"`
	AgentType           string         `json:"agentType"`
	Description         string         `json:"description"`
	WhenToUse           string         `json:"whenToUse"`
	Prompt              string         `json:"prompt"`
	PromptPath          string         `json:"prompt_path"`
	InstructionsPath    string         `json:"instructions_path"`
	Model               string         `json:"model"`
	MaxTurns            int            `json:"max_turns"`
	MaxTurnsCamel       int            `json:"maxTurns"`
	AllowedTools        []string       `json:"tool_allow"`
	Tools               []string       `json:"tools"`
	DeniedTools         []string       `json:"tool_deny"`
	DisallowedTools     []string       `json:"disallowedTools"`
	Skills              []string       `json:"skills"`
	MCPServers          []string       `json:"mcp_servers"`
	MCPServersCamel     []string       `json:"mcpServers"`
	PermissionMode      PermissionMode `json:"permission_mode"`
	PermissionModeCamel PermissionMode `json:"permissionMode"`
	ExecPolicyHint      string         `json:"exec_policy_hint"`
	ExecPolicyHintCamel string         `json:"execPolicyHint"`
	Background          bool           `json:"background"`
}

func (r rawDefinition) definition(key string) Definition {
	name := firstNonEmpty(r.Name, key)
	typ := firstNonEmpty(r.Type, r.AgentType, key)
	maxTurns := r.MaxTurns
	if maxTurns == 0 {
		maxTurns = r.MaxTurnsCamel
	}
	return Definition{
		Name:           AgentName(name),
		Type:           AgentType(typ),
		Description:    firstNonEmpty(r.Description, r.WhenToUse),
		Prompt:         r.Prompt,
		PromptPath:     firstNonEmpty(r.PromptPath, r.InstructionsPath),
		Model:          r.Model,
		MaxTurns:       maxTurns,
		AllowedTools:   cleanList(firstList(r.AllowedTools, r.Tools)),
		DeniedTools:    cleanList(firstList(r.DeniedTools, r.DisallowedTools)),
		Skills:         cleanList(r.Skills),
		MCPServers:     cleanList(firstList(r.MCPServers, r.MCPServersCamel)),
		PermissionMode: firstPermissionMode(r.PermissionMode, r.PermissionModeCamel),
		ExecPolicyHint: firstNonEmpty(r.ExecPolicyHint, r.ExecPolicyHintCamel),
		Background:     r.Background,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func firstList(values ...[]string) []string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstPermissionMode(values ...PermissionMode) PermissionMode {
	for _, value := range values {
		if strings.TrimSpace(string(value)) != "" {
			return value
		}
	}
	return ""
}

func cleanList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
