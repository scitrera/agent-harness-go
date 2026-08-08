package memorylayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
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
