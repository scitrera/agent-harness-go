package memorylayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestCatalogProviderLoadsWorkspaceSkillsAndMCPDeterministically(t *testing.T) {
	requests := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		if r.Header.Get("authorization") != "Bearer secret" {
			t.Errorf("authorization = %q", r.Header.Get("authorization"))
		}
		if r.URL.Query().Get("workspace_id") != "project-b" || r.URL.Query().Get("enabled") != "true" {
			t.Errorf("query for %s = %v", r.URL.Path, r.URL.Query())
		}
		requests[r.URL.Path] = true
		switch r.URL.Path {
		case "/v1/skills":
			if r.URL.Query().Get("include_addenda") != "true" {
				t.Errorf("include_addenda = %q", r.URL.Query().Get("include_addenda"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"skills": []any{
				map[string]any{"name": "zeta", "description": "Z", "body": "z body", "enabled": true},
				map[string]any{
					"name": "alpha", "description": "A", "body": "a body", "enabled": true,
					"allowed_tools": "read_file, shell  read_file",
					"metadata": map[string]any{"scitrera": map[string]any{
						"prereq_skills": []string{"base"}, "preferred_model": "reasoner",
					}},
				},
				map[string]any{"name": "disabled", "body": "off", "enabled": false},
			}})
		case "/v1/mcp-servers":
			if r.URL.Query().Get("transport") != "stdio" {
				t.Errorf("transport = %q", r.URL.Query().Get("transport"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"mcp_servers": []any{
				map[string]any{"name": "zeta-mcp", "transport": "stdio", "command": "zeta", "enabled": true},
				map[string]any{
					"name": "alpha-mcp", "transport": "stdio", "command": "alpha", "args": []string{"--serve"},
					"env": map[string]string{"ZED": "2", "ALPHA": "1"}, "enabled": true,
				},
				map[string]any{"name": "disabled", "transport": "stdio", "command": "off", "enabled": false},
				map[string]any{"name": "remote", "transport": "streamable-http", "command": "ignored", "enabled": true},
			}})
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	provider, err := NewCatalogProvider(Config{BaseURL: server.URL, APIKey: "secret", Workspace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.LoadWorkspace(context.Background(), "project-b")
	if err != nil {
		t.Fatal(err)
	}
	if !requests["/v1/skills"] || !requests["/v1/mcp-servers"] {
		t.Fatalf("requests = %#v", requests)
	}
	if len(got.Tools) != 0 || len(got.Skills) != 2 || got.Skills[0].Name != "alpha" || got.Skills[1].Name != "zeta" {
		t.Fatalf("skills = %#v tools = %#v", got.Skills, got.Tools)
	}
	alpha := got.Skills[0]
	if !reflect.DeepEqual(alpha.AllowedTools, []string{"read_file", "shell"}) || !reflect.DeepEqual(alpha.Prereqs, []string{"base"}) || alpha.PreferredModel != "reasoner" {
		t.Fatalf("alpha = %#v", alpha)
	}
	if len(got.MCPServers) != 2 || got.MCPServers[0].Name != "alpha-mcp" || got.MCPServers[1].Name != "zeta-mcp" {
		t.Fatalf("mcp servers = %#v", got.MCPServers)
	}
	if !reflect.DeepEqual(got.MCPServers[0].Env, []string{"ALPHA=1", "ZED=2"}) {
		t.Fatalf("env = %#v", got.MCPServers[0].Env)
	}
}

func TestCatalogProviderLoadUsesConfiguredWorkspaceAndToleratesMissingEndpoints(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("workspace_id") != "configured" {
			t.Errorf("workspace = %q", r.URL.Query().Get("workspace_id"))
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	provider, err := NewCatalogProvider(Config{BaseURL: server.URL, Workspace: "configured"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Skills) != 0 || len(got.MCPServers) != 0 {
		t.Fatalf("catalog = %#v", got)
	}
}
