package team

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

func TestFileGraphStoreDescendantsBFS_listsChildrenBreadthFirst(t *testing.T) {
	// Given: an agent graph with two children and one grandchild.
	ctx := context.Background()
	graph := NewFileGraphStore(filepath.Join(t.TempDir(), "team.json"))
	for _, node := range []AgentNode{
		{ID: "leader", Type: subagent.AgentType("lead")},
		{ID: "agent-a", Type: subagent.AgentType("local")},
		{ID: "agent-b", Type: subagent.AgentType("local")},
		{ID: "agent-c", Type: subagent.AgentType("local")},
	} {
		if err := graph.RegisterAgent(ctx, node); err != nil {
			t.Fatalf("register agent %s: %v", node.ID, err)
		}
	}
	if err := graph.AddChild(ctx, AddChildRequest{ParentID: "leader", Child: AgentNode{ID: "agent-b"}, MaxActiveChildren: 3}); err != nil {
		t.Fatalf("add agent-b: %v", err)
	}
	if err := graph.AddChild(ctx, AddChildRequest{ParentID: "leader", Child: AgentNode{ID: "agent-a"}, MaxActiveChildren: 3}); err != nil {
		t.Fatalf("add agent-a: %v", err)
	}
	if err := graph.AddChild(ctx, AddChildRequest{ParentID: "agent-a", Child: AgentNode{ID: "agent-c"}, MaxActiveChildren: 3}); err != nil {
		t.Fatalf("add agent-c: %v", err)
	}

	// When: descendants are listed from the leader.
	got, err := graph.DescendantsBFS(ctx, "leader")
	if err != nil {
		t.Fatalf("descendants bfs: %v", err)
	}

	// Then: direct children appear before grandchildren with deterministic sibling order.
	want := []AgentID{"agent-a", "agent-b", "agent-c"}
	if !sameAgentIDs(got, want) {
		t.Fatalf("descendants mismatch: got %#v want %#v", got, want)
	}
}

func TestFileGraphStoreListAgents_listsAgentsByID(t *testing.T) {
	// Given: registered agents in non-sorted order.
	ctx := context.Background()
	graph := NewFileGraphStore(filepath.Join(t.TempDir(), "team.json"))
	for _, node := range []AgentNode{
		{ID: "agent-b", Type: subagent.AgentType("local")},
		{ID: "agent-a", Type: subagent.AgentType("local")},
	} {
		if err := graph.RegisterAgent(ctx, node); err != nil {
			t.Fatalf("register agent %s: %v", node.ID, err)
		}
	}

	// When: the flat agent list is loaded.
	got, err := graph.ListAgents(ctx)
	if err != nil {
		t.Fatalf("list agents: %v", err)
	}

	// Then: callers receive a deterministic ID order.
	if !sameAgentIDs(got, []AgentID{"agent-a", "agent-b"}) {
		t.Fatalf("agents mismatch: %#v", got)
	}
}

func TestFileGraphStoreAddChild_rejectsActiveChildrenOverLimit(t *testing.T) {
	// Given: a parent already running one child.
	ctx := context.Background()
	graph := NewFileGraphStore(filepath.Join(t.TempDir(), "team.json"))
	if err := graph.RegisterAgent(ctx, AgentNode{ID: "leader", Type: subagent.AgentType("lead")}); err != nil {
		t.Fatalf("register leader: %v", err)
	}
	if err := graph.RegisterAgent(ctx, AgentNode{ID: "agent-a", Type: subagent.AgentType("local")}); err != nil {
		t.Fatalf("register agent-a: %v", err)
	}
	if err := graph.AddChild(ctx, AddChildRequest{ParentID: "leader", Child: AgentNode{ID: "agent-a"}, MaxActiveChildren: 1}); err != nil {
		t.Fatalf("add first child: %v", err)
	}

	// When: another active child would exceed the bound.
	err := graph.AddChild(ctx, AddChildRequest{
		ParentID:          "leader",
		Child:             AgentNode{ID: "agent-b", Type: subagent.AgentType("local")},
		MaxActiveChildren: 1,
	})

	// Then: the graph refuses the edge without persisting the child.
	if !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("expected concurrency limit, got %v", err)
	}
	got, listErr := graph.DescendantsBFS(ctx, "leader")
	if listErr != nil {
		t.Fatalf("descendants bfs: %v", listErr)
	}
	if !sameAgentIDs(got, []AgentID{"agent-a"}) {
		t.Fatalf("unexpected descendants: %#v", got)
	}
}

func TestFileGraphStoreLoad_rejectsMalformedState(t *testing.T) {
	// Given: a graph state file containing malformed JSON.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "team.json")
	if err := writeTestFile(path, []byte("{")); err != nil {
		t.Fatalf("write malformed state: %v", err)
	}

	// When: the graph store loads it.
	err := NewFileGraphStore(path).RegisterAgent(ctx, AgentNode{ID: "agent-a", Type: subagent.AgentType("local")})

	// Then: callers receive a typed corrupt-state error.
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected corrupt state, got %v", err)
	}
}
