package memorylayer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"unicode"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
)

// CatalogProvider loads a workspace's enabled skills and launchable MCP servers
// from MemoryLayer into the harness's protocol-neutral catalog types. Tools are
// intentionally empty: MemoryLayer has no tools-list endpoint; tools continue
// to come from local registration, MCP discovery, or another provider.
type CatalogProvider struct {
	client *client
}

func NewCatalogProvider(cfg Config) (*CatalogProvider, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &CatalogProvider{client: c}, nil
}

// Load satisfies catalog.Provider using the configured default workspace.
func (p *CatalogProvider) Load(ctx context.Context) (catalog.Catalog, error) {
	return p.LoadWorkspace(ctx, p.client.workspace)
}

// LoadWorkspace loads a catalog for one logical workspace. It is additive to
// catalog.Provider so multi-workspace hosts can resolve the catalog per turn
// without sharing one project's skills or MCP configuration with another.
func (p *CatalogProvider) LoadWorkspace(ctx context.Context, workspace string) (catalog.Catalog, error) {
	skills, err := p.loadSkills(ctx, workspace)
	if err != nil {
		return catalog.Catalog{}, err
	}
	servers, err := p.loadMCPServers(ctx, workspace)
	if err != nil {
		return catalog.Catalog{}, err
	}
	return catalog.WithComputedRevision(catalog.Catalog{Skills: skills, MCPServers: servers})
}

type memoryLayerSkill struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	Body         string          `json:"body"`
	AllowedTools string          `json:"allowed_tools"`
	Metadata     json.RawMessage `json:"metadata"`
	Enabled      bool            `json:"enabled"`
	Revision     int             `json:"revision"`
	ETag         string          `json:"etag"`
	ManifestHash string          `json:"manifest_hash"`
	BundleHash   string          `json:"bundle_hash"`
}

func (p *CatalogProvider) loadSkills(ctx context.Context, workspace string) ([]catalog.SkillSpec, error) {
	query := p.client.workspaceQuery(workspace)
	query.Set("enabled", "true")
	query.Set("include_addenda", "true")
	var envelope struct {
		Skills []memoryLayerSkill `json:"skills"`
	}
	if err := p.client.do(ctx, http.MethodGet, "/v1/skills", query, nil, &envelope); err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]catalog.SkillSpec, 0, len(envelope.Skills))
	for _, skill := range envelope.Skills {
		if !skill.Enabled || strings.TrimSpace(skill.Name) == "" {
			continue
		}
		prereqs, preferredModel := memoryLayerSkillMetadata(skill.Metadata)
		out = append(out, catalog.SkillSpec{
			Name:           skill.Name,
			Description:    skill.Description,
			Content:        skill.Body,
			Enabled:        true,
			AllowedTools:   parseAllowedTools(skill.AllowedTools),
			Prereqs:        prereqs,
			PreferredModel: preferredModel,
			Source: &catalog.ResourceRevision{
				System: "memorylayer", ID: skill.ID, Revision: skill.Revision, ETag: skill.ETag,
				ManifestDigest: skill.ManifestHash, BundleDigest: skill.BundleHash,
			},
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func memoryLayerSkillMetadata(raw json.RawMessage) (prereqs []string, preferredModel string) {
	if len(raw) == 0 {
		return nil, ""
	}
	var metadata struct {
		Scitrera struct {
			PrereqSkills   []string `json:"prereq_skills"`
			PreferredModel string   `json:"preferred_model"`
		} `json:"scitrera"`
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, ""
	}
	return metadata.Scitrera.PrereqSkills, metadata.Scitrera.PreferredModel
}

func parseAllowedTools(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	if len(fields) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(fields))
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

type memoryLayerMCPServer struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Transport    string            `json:"transport"`
	Command      string            `json:"command"`
	Args         []string          `json:"args"`
	Env          map[string]string `json:"env"`
	Enabled      bool              `json:"enabled"`
	ManifestHash string            `json:"manifest_hash"`
}

func (p *CatalogProvider) loadMCPServers(ctx context.Context, workspace string) ([]catalog.MCPServerSpec, error) {
	query := p.client.workspaceQuery(workspace)
	query.Set("enabled", "true")
	query.Set("transport", "stdio")
	var envelope struct {
		MCPServers []memoryLayerMCPServer `json:"mcp_servers"`
	}
	if err := p.client.do(ctx, http.MethodGet, "/v1/mcp-servers", query, nil, &envelope); err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]catalog.MCPServerSpec, 0, len(envelope.MCPServers))
	for _, server := range envelope.MCPServers {
		if !server.Enabled || server.Transport != "stdio" || strings.TrimSpace(server.Name) == "" || strings.TrimSpace(server.Command) == "" {
			continue
		}
		out = append(out, catalog.MCPServerSpec{
			Name:    server.Name,
			Command: server.Command,
			Args:    append([]string(nil), server.Args...),
			Env:     sortedEnvironment(server.Env),
			Source: &catalog.ResourceRevision{
				System: "memorylayer", ID: server.ID, ManifestDigest: server.ManifestHash,
			},
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func sortedEnvironment(environment map[string]string) []string {
	if len(environment) == 0 {
		return nil
	}
	out := make([]string, 0, len(environment))
	for key, value := range environment {
		out = append(out, key+"="+value)
	}
	sort.Strings(out)
	return out
}

var _ catalog.Provider = (*CatalogProvider)(nil)
