// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package memorylayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestPromptNoteProviderUsesTypedSDKPaginationWorkspaceAndAuthority(t *testing.T) {
	var pageTokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/prompt-notes" || r.URL.Query().Get("workspace_id") != "project-b" || r.URL.Query().Get("limit") != "100" {
			t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("X-Aether-Grant-ID") != "grant-1" || r.Header.Get("X-Aether-Subject-ID") != "alice" {
			t.Errorf("headers = %#v", r.Header)
		}
		pageToken := r.URL.Query().Get("page_token")
		pageTokens = append(pageTokens, pageToken)
		w.Header().Set("content-type", "application/json")
		if pageToken == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"notes":           []any{map[string]any{"id": "n2", "key": "z", "content": "last", "enabled": true, "schema_version": 1, "revision": 2, "etag": "e2"}},
				"next_page_token": "cursor-2",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []any{map[string]any{"id": "n1", "key": "a", "content": "first", "enabled": true, "schema_version": 1, "revision": 1, "etag": "e1"}},
		})
	}))
	t.Cleanup(server.Close)

	provider, err := NewPromptNoteProvider(Config{BaseURL: server.URL, APIKey: "secret", Workspace: "configured"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "alice"})
	notes, err := provider.LoadWorkspace(ctx, "project-b")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pageTokens, []string{"", "cursor-2"}) {
		t.Fatalf("page tokens = %v", pageTokens)
	}
	if len(notes) != 2 || notes[0].Key != "z" || notes[1].Key != "a" || notes[1].Revision != 1 || notes[1].ETag != "e1" {
		t.Fatalf("notes = %#v", notes)
	}
}

func TestPromptNoteProviderRejectsCursorCycles(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"notes":[],"next_page_token":"same"}`))
	}))
	t.Cleanup(server.Close)
	provider, err := NewPromptNoteProvider(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.LoadWorkspace(context.Background(), "project"); err == nil {
		t.Fatal("expected cursor cycle to fail")
	}
}
