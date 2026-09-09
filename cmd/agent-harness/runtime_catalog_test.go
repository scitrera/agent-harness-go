// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/skills"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

type fakeWorkspaceCatalogProvider struct {
	mu       sync.Mutex
	catalogs map[string]catalog.Catalog
	errors   map[string]error
	calls    map[string]int
}

func (p *fakeWorkspaceCatalogProvider) LoadWorkspace(_ context.Context, workspaceID string) (catalog.Catalog, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[workspaceID]++
	if err := p.errors[workspaceID]; err != nil {
		return catalog.Catalog{}, err
	}
	return p.catalogs[workspaceID], nil
}

func (p *fakeWorkspaceCatalogProvider) callCount(workspaceID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[workspaceID]
}

type namedCatalogToolProvider struct{ name string }

func (p namedCatalogToolProvider) ID() string { return p.name }
func (p namedCatalogToolProvider) Tools(context.Context, protocol.MessageAddress, protocol.ChatMessage) ([]tools.Descriptor, error) {
	return []tools.Descriptor{{Name: "mcp." + p.name + ".tool"}}, nil
}
func (p namedCatalogToolProvider) Invoke(_ context.Context, req tools.Request) (tools.Result, error) {
	return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"workspace":"`+p.name+`"}`))
}

func TestWorkspaceCatalogRuntimeCachesAndIsolatesWorkspaces(t *testing.T) {
	provider := &fakeWorkspaceCatalogProvider{
		catalogs: map[string]catalog.Catalog{
			"project-a": {
				Skills:     []catalog.SkillSpec{{Name: "remote-a", Content: "body a", Enabled: true}},
				MCPServers: []catalog.MCPServerSpec{{Name: "server-a", Command: "unused"}},
			},
			"project-b": {
				Skills:     []catalog.SkillSpec{{Name: "remote-b", Content: "body b", Enabled: true}},
				MCPServers: []catalog.MCPServerSpec{{Name: "server-b", Command: "unused"}},
			},
		},
		errors: map[string]error{}, calls: map[string]int{},
	}
	runtime := newWorkspaceCatalogRuntime(provider, t.TempDir(), []catalog.SkillSpec{{
		Name: "local", Content: "local body", Enabled: true,
	}}, nil)
	runtime.newToolProvider = func(servers []catalog.MCPServerSpec) (turn.ToolProvider, error) {
		if len(servers) == 0 {
			return nil, nil
		}
		return namedCatalogToolProvider{name: servers[0].Name}, nil
	}

	ctxA := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a"})
	_ = runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a"})
	ctxB := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "project-b"})
	if provider.callCount("project-a") != 1 || provider.callCount("project-b") != 1 {
		t.Fatalf("provider calls = %#v", provider.calls)
	}

	load := skills.LoadTool(skills.BuildRegistry(nil, t.TempDir()))
	resultA, err := load(ctxA, tools.Request{CallID: "a", Name: skills.LoadToolName, Arguments: json.RawMessage(`{"name":"remote-a"}`)})
	if err != nil || resultA.IsError || !strings.Contains(string(resultA.Payload), "body a") {
		t.Fatalf("workspace A skill = %s, %v", resultA.Payload, err)
	}
	leak, err := load(ctxB, tools.Request{CallID: "b", Name: skills.LoadToolName, Arguments: json.RawMessage(`{"name":"remote-a"}`)})
	if err != nil || !leak.IsError {
		t.Fatalf("workspace A skill leaked into B: %s, %v", leak.Payload, err)
	}
	local, err := load(ctxB, tools.Request{CallID: "local", Name: skills.LoadToolName, Arguments: json.RawMessage(`{"name":"local"}`)})
	if err != nil || local.IsError {
		t.Fatalf("local skill missing from workspace B: %s, %v", local.Payload, err)
	}

	toolProvider := workspaceCatalogToolProvider{runtime: runtime}
	toolsA, err := toolProvider.Tools(ctxA, protocol.MessageAddress{WorkspaceID: "project-a"}, protocol.ChatMessage{})
	if err != nil || len(toolsA) != 1 || toolsA[0].Name != "mcp.server-a.tool" {
		t.Fatalf("workspace A tools = %#v, %v", toolsA, err)
	}
	toolsB, err := toolProvider.Tools(ctxB, protocol.MessageAddress{WorkspaceID: "project-b"}, protocol.ChatMessage{})
	if err != nil || len(toolsB) != 1 || toolsB[0].Name != "mcp.server-b.tool" {
		t.Fatalf("workspace B tools = %#v, %v", toolsB, err)
	}
}

func TestWorkspaceCatalogRuntimeColdFailureUsesLocalWithoutCrossWorkspaceReuse(t *testing.T) {
	provider := &fakeWorkspaceCatalogProvider{
		catalogs: map[string]catalog.Catalog{
			"good": {Skills: []catalog.SkillSpec{{Name: "good-only", Content: "good body", Enabled: true}}},
		},
		errors: map[string]error{"bad": errors.New("catalog unavailable")}, calls: map[string]int{},
	}
	runtime := newWorkspaceCatalogRuntime(provider, t.TempDir(), []catalog.SkillSpec{{
		Name: "local", Content: "local body", Enabled: true,
	}}, nil)
	ctxGood := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "good"})
	ctxBad := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "bad"})
	_ = runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "bad"})
	if provider.callCount("bad") != 2 {
		t.Fatalf("cold failure was cached; calls = %d", provider.callCount("bad"))
	}

	load := skills.LoadTool(skills.BuildRegistry(nil, t.TempDir()))
	for _, tc := range []struct {
		ctx  context.Context
		name string
		err  bool
	}{
		{ctxGood, "good-only", false},
		{ctxBad, "local", false},
		{ctxBad, "good-only", true},
	} {
		result, err := load(tc.ctx, tools.Request{
			CallID: fmt.Sprintf("call-%s", tc.name), Name: skills.LoadToolName,
			Arguments: json.RawMessage(`{"name":"` + tc.name + `"}`),
		})
		if err != nil || result.IsError != tc.err {
			t.Fatalf("load %s: is_error=%v payload=%s err=%v", tc.name, result.IsError, result.Payload, err)
		}
	}
}

func TestWorkspaceCatalogRuntimeCoalescesConcurrentColdLoads(t *testing.T) {
	provider := &fakeWorkspaceCatalogProvider{
		catalogs: map[string]catalog.Catalog{
			"project-a": {Skills: []catalog.SkillSpec{{Name: "remote-a", Content: "body a", Enabled: true}}},
		},
		errors: map[string]error{}, calls: map[string]int{},
	}
	runtime := newWorkspaceCatalogRuntime(provider, t.TempDir(), nil, nil)

	const workers = 32
	start := make(chan struct{})
	results := make(chan *workspaceCatalogEntry, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			entry, err := runtime.resolve(context.Background(), "project-a")
			results <- entry
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("resolve returned error: %v", err)
		}
	}
	var first *workspaceCatalogEntry
	for entry := range results {
		if first == nil {
			first = entry
			continue
		}
		if entry != first {
			t.Fatal("concurrent callers received different cached entries")
		}
	}
	if got := provider.callCount("project-a"); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
}

func TestWorkspaceCatalogRuntimeBindsRevisionPerSession(t *testing.T) {
	provider := &fakeWorkspaceCatalogProvider{
		catalogs: map[string]catalog.Catalog{
			"project-a": {Skills: []catalog.SkillSpec{{Name: "remote", Content: "version one", Enabled: true}}},
		},
		errors: map[string]error{}, calls: map[string]int{},
	}
	runtime := newWorkspaceCatalogRuntime(provider, t.TempDir(), nil, nil)
	first := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-one"})
	firstRevision, ok := catalog.RevisionFrom(first)
	if !ok {
		t.Fatal("first session has no catalog revision")
	}

	provider.mu.Lock()
	provider.catalogs["project-a"] = catalog.Catalog{Skills: []catalog.SkillSpec{{Name: "remote", Content: "version two", Enabled: true}}}
	provider.mu.Unlock()
	same := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-one"})
	newSession := runtime.decorate(context.Background(), protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "session-two"})
	sameRevision, _ := catalog.RevisionFrom(same)
	newRevision, _ := catalog.RevisionFrom(newSession)
	if sameRevision != firstRevision || newRevision == "" || newRevision == firstRevision {
		t.Fatalf("revisions: first=%q same=%q new=%q", firstRevision, sameRevision, newRevision)
	}
	if got := provider.callCount("project-a"); got != 2 {
		t.Fatalf("provider calls = %d, want one per session lineage", got)
	}

	load := skills.LoadTool(skills.BuildRegistry(nil, t.TempDir()))
	for _, tc := range []struct {
		ctx  context.Context
		want string
	}{{first, "version one"}, {same, "version one"}, {newSession, "version two"}} {
		result, err := load(tc.ctx, tools.Request{CallID: tc.want, Name: skills.LoadToolName, Arguments: json.RawMessage(`{"name":"remote"}`)})
		if err != nil || result.IsError || !strings.Contains(string(result.Payload), tc.want) {
			t.Fatalf("load for %q = %s, %v", tc.want, result.Payload, err)
		}
	}
}
