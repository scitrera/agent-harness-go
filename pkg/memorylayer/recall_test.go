package memorylayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turn"
)

func newTestRecaller(t *testing.T, handler http.HandlerFunc) *Recaller {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	r, err := NewRecaller(Config{BaseURL: srv.URL, Workspace: "default"})
	if err != nil {
		t.Fatalf("new recaller: %v", err)
	}
	return r
}

func TestRecallMapsMemoriesToHits(t *testing.T) {
	var gotBody map[string]any
	r := newTestRecaller(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v1/memories/recall" {
			t.Errorf("path = %q, want /v1/memories/recall", req.URL.Path)
		}
		_ = json.NewDecoder(req.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"memories": []any{
			map[string]any{
				"id": "mem_1", "content": "the sky is blue",
				"type": "fact", "subtype": "observation",
				"tags": []string{"weather"}, "relevance_score": 0.4, "boosted_score": 0.9,
			},
			map[string]any{"id": "mem_2", "content": "grass is green", "relevance_score": 0.5},
			// Empty content is not useful to inject.
			map[string]any{"id": "mem_3", "content": "   "},
		}})
	})

	hits, err := r.Recall(context.Background(), tools.MemoryAuthority{}, "ws", "what colour is the sky?", 3)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("got %d hits, want 2 (the blank one dropped): %+v", len(hits), hits)
	}
	if hits[0].ID != "mem_1" || hits[0].Content != "the sky is blue" {
		t.Fatalf("first hit = %+v", hits[0])
	}
	// The boosted score is what the server ranked on, so it wins.
	if hits[0].Score != 0.9 {
		t.Fatalf("score = %v, want the boosted 0.9", hits[0].Score)
	}
	if hits[1].Score != 0.5 {
		t.Fatalf("fallback score = %v, want raw relevance 0.5", hits[1].Score)
	}
	if hits[0].Type != "fact" || hits[0].Subtype != "observation" || len(hits[0].Tags) != 1 {
		t.Fatalf("provenance fields lost: %+v", hits[0])
	}

	if gotBody["query"] != "what colour is the sky?" {
		t.Fatalf("query = %v", gotBody["query"])
	}
	if gotBody["limit"].(float64) != 3 {
		t.Fatalf("limit = %v, want 3", gotBody["limit"])
	}
	// The turn's workspace wins over the client default.
	if gotBody["workspace_id"] != "ws" {
		t.Fatalf("workspace_id = %v, want the turn's workspace", gotBody["workspace_id"])
	}
}

func TestRecallFallsBackToConfiguredWorkspace(t *testing.T) {
	var gotBody map[string]any
	r := newTestRecaller(t, func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewDecoder(req.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"memories": []any{}})
	})
	if _, err := r.Recall(context.Background(), tools.MemoryAuthority{}, "", "q", 0); err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if gotBody["workspace_id"] != "default" {
		t.Fatalf("workspace_id = %v, want the configured default", gotBody["workspace_id"])
	}
	if gotBody["limit"].(float64) != 10 {
		t.Fatalf("limit = %v, want the default 10", gotBody["limit"])
	}
}

// The history store already writes the transcript to MemoryLayer, so committing
// again here would duplicate every message.
func TestAppendThreadMessagesDoesNotWrite(t *testing.T) {
	called := false
	r := newTestRecaller(t, func(w http.ResponseWriter, req *http.Request) {
		called = true
		_ = json.NewEncoder(w).Encode(map[string]any{})
	})
	err := r.AppendThreadMessages(context.Background(), tools.MemoryAuthority{}, "ws", "thread-1", "workspace",
		[]protocol.ChatMessage{{ID: "m1", Role: protocol.RoleUser}})
	if err != nil {
		t.Fatalf("AppendThreadMessages: %v", err)
	}
	if called {
		t.Fatal("AppendThreadMessages wrote to the server; the history store already persists the turn")
	}
}

func TestEnsureThreadMintsCanonicalChildWithParentAndWorkspace(t *testing.T) {
	workspaceCreated := false
	threadCreates := 0
	var createBody map[string]any
	r := newTestRecaller(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("content-type", "application/json")
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/v1/workspaces/project-a":
			if !workspaceCreated {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace": map[string]any{"id": "project-a"}})
		case req.Method == http.MethodPost && req.URL.Path == "/v1/workspaces":
			workspaceCreated = true
			_ = json.NewEncoder(w).Encode(map[string]any{"workspace": map[string]any{"id": "project-a"}})
		case req.Method == http.MethodPost && req.URL.Path == "/v1/threads":
			threadCreates++
			_ = json.NewDecoder(req.Body).Decode(&createBody)
			if req.URL.Query().Get("workspace_id") != "project-a" {
				t.Errorf("workspace query = %q", req.URL.Query().Get("workspace_id"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"thread": map[string]any{
				"id": "thread_server_1", "parent_thread": "parent-1", "ownership": "workspace",
			}})
		default:
			t.Errorf("unexpected request: %s %s", req.Method, req.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	})

	id, err := r.EnsureThread(context.Background(), tools.MemoryAuthority{}, turn.ThreadSpec{
		WorkspaceID: "project-a", ParentThreadID: "parent-1", Ownership: "workspace", Origin: "subagent",
	})
	if err != nil {
		t.Fatalf("EnsureThread: %v", err)
	}
	if id != "thread_server_1" || threadCreates != 1 || !workspaceCreated {
		t.Fatalf("id=%q creates=%d workspaceCreated=%v", id, threadCreates, workspaceCreated)
	}
	if _, supplied := createBody["id"]; supplied {
		t.Fatalf("MemoryLayer thread create must not supply an id: %#v", createBody)
	}
	if createBody["workspace_id"] != "project-a" || createBody["parent_thread"] != "parent-1" || createBody["ownership"] != "workspace" {
		t.Fatalf("create body = %#v", createBody)
	}
	metadata, _ := createBody["metadata"].(map[string]any)
	scitrera, _ := metadata["scitrera"].(map[string]any)
	if scitrera["origin"] != "subagent" {
		t.Fatalf("origin metadata = %#v", createBody["metadata"])
	}
}

func TestEnsureThreadRejectsMissingCallerOwnedID(t *testing.T) {
	r := newTestRecaller(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/threads/client-id" {
			t.Errorf("unexpected request: %s %s", req.Method, req.URL.String())
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "Not Found"})
	})

	_, err := r.EnsureThread(context.Background(), tools.MemoryAuthority{}, turn.ThreadSpec{
		WorkspaceID: "project-a", ThreadID: "client-id", ParentThreadID: "parent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "server-minted") {
		t.Fatalf("error = %v, want server-minted guidance", err)
	}
}
