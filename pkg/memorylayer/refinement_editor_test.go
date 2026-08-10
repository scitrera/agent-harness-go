package memorylayer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	"github.com/scitrera/agent-harness-go/pkg/refinement"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestRefinementResourceEditorCreatesPromptNoteWithWorkspaceCASAndAuthority(t *testing.T) {
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		if request.Method != http.MethodPost || request.Path != "/prompt-notes" {
			t.Fatalf("request = %s %s", request.Method, request.Path)
		}
		if request.Header.Get("If-None-Match") != "*" || request.Header.Get("Idempotency-Key") != "edit-op" {
			t.Fatalf("conditional headers = %#v", request.Header)
		}
		if request.Header.Get("X-Aether-Grant-ID") != "grant-1" {
			t.Fatalf("authority headers = %#v", request.Header)
		}
		var body map[string]any
		if err := json.Unmarshal(request.Body, &body); err != nil {
			t.Fatal(err)
		}
		if body["workspace_id"] != "project-a" || body["key"] != "coding/conventions" || body["title"] != "Conventions" {
			t.Fatalf("body = %#v", body)
		}
		return refinementJSONResponse(http.StatusCreated, `{"note":{"id":"note-1","workspace_id":"project-a","key":"coding/conventions","title":"Conventions","content":"Run tests.","enabled":true,"schema_version":1,"metadata":{"source":"refinement"},"revision":1,"etag":"e1"}}`)
	}}
	editor, err := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{GrantID: "grant-1"})
	mutation, err := editor.Apply(ctx, "project-a", "edit-op", refinement.Edit{
		Action: refinement.ActionCreate, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "coding/conventions",
		Content: map[string]any{"title": "Conventions", "content": "Run tests.", "enabled": true, "schema_version": 1, "metadata": map[string]any{"source": "refinement"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Before != nil || mutation.After == nil || mutation.After.ResourceID != "note-1" || mutation.After.ETag != "e1" {
		t.Fatalf("mutation = %#v", mutation)
	}
	if mutation.After.Content["content"] != "Run tests." || mutation.After.Deleted {
		t.Fatalf("after snapshot = %#v", mutation.After)
	}
}

func TestRefinementResourceEditorMapsStaleETagToConflict(t *testing.T) {
	requests := 0
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		requests++
		if requests == 1 {
			return refinementJSONResponse(http.StatusOK, `{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"Old","content":"Old","enabled":true,"schema_version":1,"metadata":{},"revision":2,"etag":"current"}}`)
		}
		if request.Method != http.MethodPut || request.Header.Get("If-Match") != "stale" {
			t.Fatalf("replace request = %#v", request)
		}
		return refinementJSONResponse(http.StatusPreconditionFailed, `{"detail":"ETag does not match current revision"}`)
	}}
	editor, _ := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	_, err := editor.Apply(context.Background(), "project-a", "replace-op", refinement.Edit{
		Action: refinement.ActionReplace, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "note", ResourceID: "note-1", ExpectedETag: "stale",
		Content: map[string]any{"title": "New", "content": "New", "enabled": true, "schema_version": 1},
	})
	if !errors.Is(err, refinement.ErrConflict) {
		t.Fatalf("replace error = %v", err)
	}
}

func TestRefinementResourceEditorReplayRecoversBeforeSnapshotFromHistory(t *testing.T) {
	requests := 0
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		requests++
		switch requests {
		case 1:
			return refinementJSONResponse(http.StatusOK, `{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"New","content":"New","enabled":true,"schema_version":1,"metadata":{},"revision":2,"etag":"e2"}}`)
		case 2:
			return refinementJSONResponse(http.StatusOK, `{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"New","content":"New","enabled":true,"schema_version":1,"metadata":{},"revision":2,"etag":"e2"},"replayed":true}`)
		case 3:
			if request.Path != "/prompt-notes/note-1/revisions" {
				t.Fatalf("history path = %s", request.Path)
			}
			return refinementJSONResponse(http.StatusOK, `{"revisions":[{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"New","content":"New","enabled":true,"schema_version":1,"metadata":{},"revision":2,"etag":"e2"},"action":"replace","operation_id":"replace-op"},{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"Old","content":"Old","enabled":true,"schema_version":1,"metadata":{},"revision":1,"etag":"e1"},"action":"create","operation_id":"create-op"}]}`)
		default:
			t.Fatalf("unexpected request %d: %s %s", requests, request.Method, request.Path)
			return nil
		}
	}}
	editor, _ := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	mutation, err := editor.Apply(context.Background(), "project-a", "replace-op", refinement.Edit{
		Action: refinement.ActionReplace, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "note", ResourceID: "note-1", ExpectedETag: "e1",
		Content: map[string]any{"title": "New", "content": "New", "enabled": true, "schema_version": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !mutation.Replayed || mutation.Before == nil || mutation.Before.ETag != "e1" || mutation.Before.Content["title"] != "Old" || mutation.After.ETag != "e2" {
		t.Fatalf("replayed mutation = %#v", mutation)
	}
}

func TestRefinementResourceEditorRestoresPromptNoteTombstone(t *testing.T) {
	requests := 0
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		requests++
		switch requests {
		case 1:
			if request.Method != http.MethodGet || request.Query.Get("include_deleted") != "true" {
				t.Fatalf("get tombstone request = %#v", request)
			}
			return refinementJSONResponse(http.StatusOK, `{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"Old","content":"Old","enabled":true,"schema_version":1,"metadata":{},"revision":2,"etag":"e2","deleted_at":"2026-08-09T00:00:00Z"}}`)
		case 2:
			if request.Method != http.MethodPost || request.Path != "/prompt-notes/note-1/restore" || request.Header.Get("If-Match") != "e2" {
				t.Fatalf("restore request = %#v", request)
			}
			return refinementJSONResponse(http.StatusOK, `{"note":{"id":"note-1","workspace_id":"project-a","key":"note","title":"Old","content":"Old","enabled":true,"schema_version":1,"metadata":{},"revision":3,"etag":"e3"}}`)
		default:
			t.Fatalf("unexpected request %d: %#v", requests, request)
			return nil
		}
	}}
	editor, _ := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	mutation, err := editor.Apply(context.Background(), "project-a", "restore-op", refinement.Edit{
		Action: refinement.ActionRestore, ResourceKind: refinement.ResourcePromptNote, ResourceKey: "note", ResourceID: "note-1", ExpectedETag: "e2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Before == nil || !mutation.Before.Deleted || mutation.Before.ETag != "e2" || mutation.After == nil || mutation.After.Deleted || mutation.After.ETag != "e3" {
		t.Fatalf("restore mutation = %#v", mutation)
	}
}

func TestRefinementResourceEditorCreatesNativeSkillManifest(t *testing.T) {
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		if request.Method != http.MethodPost || request.Path != "/skills" {
			t.Fatalf("request = %s %s", request.Method, request.Path)
		}
		if request.Header.Get("If-None-Match") != "*" || request.Header.Get("Idempotency-Key") != "skill-op" {
			t.Fatalf("conditional headers = %#v", request.Header)
		}
		var body map[string]any
		if err := json.Unmarshal(request.Body, &body); err != nil {
			t.Fatal(err)
		}
		if body["workspace_id"] != "project-a" || body["name"] != "review-workflow" {
			t.Fatalf("body = %#v", body)
		}
		return refinementJSONResponse(http.StatusCreated, `{"skill":{"id":"skl-1","workspace_id":"project-a","name":"review-workflow","description":"Review changes","version":"0.1.0","body":"## Process","metadata":{"source":"refinement"},"source_mode":"server","manifest_hash":"mh","bundle_hash":"","enabled":true,"revision":1,"etag":"s1"}}`)
	}}
	editor, err := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := editor.Apply(context.Background(), "project-a", "skill-op", refinement.Edit{
		Action: refinement.ActionCreate, ResourceKind: refinement.ResourceSkill, ResourceKey: "review-workflow",
		Content: map[string]any{
			"description": "Review changes", "version": "0.1.0", "body": "## Process",
			"metadata": map[string]any{"source": "refinement"}, "source_mode": "server",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.After == nil || mutation.After.ResourceID != "skl-1" || mutation.After.ResourceKey != "review-workflow" || mutation.After.ETag != "s1" {
		t.Fatalf("mutation = %#v", mutation)
	}
}

func TestRefinementResourceEditorRestoresNativeSkillTombstone(t *testing.T) {
	requests := 0
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		requests++
		switch requests {
		case 1:
			if request.Path != "/skills/skl-1" || request.Query.Get("include_deleted") != "true" {
				t.Fatalf("get tombstone request = %#v", request)
			}
			return refinementJSONResponse(http.StatusOK, `{"skill":{"id":"skl-1","workspace_id":"project-a","name":"review-workflow","description":"Review changes","version":"0.1.0","body":"## Process","metadata":{},"source_mode":"server","manifest_hash":"mh","bundle_hash":"bh","enabled":true,"revision":2,"etag":"s2","deleted_at":"2026-08-09T00:00:00Z"}}`)
		case 2:
			if request.Path != "/skills/skl-1/restore" || request.Header.Get("If-Match") != "s2" {
				t.Fatalf("restore request = %#v", request)
			}
			return refinementJSONResponse(http.StatusOK, `{"skill":{"id":"skl-1","workspace_id":"project-a","name":"review-workflow","description":"Review changes","version":"0.1.0","body":"## Process","metadata":{},"source_mode":"server","manifest_hash":"mh","bundle_hash":"bh","enabled":true,"revision":3,"etag":"s3"}}`)
		default:
			t.Fatalf("unexpected request %d: %#v", requests, request)
			return nil
		}
	}}
	editor, _ := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	mutation, err := editor.Apply(context.Background(), "project-a", "restore-skill", refinement.Edit{
		Action: refinement.ActionRestore, ResourceKind: refinement.ResourceSkill,
		ResourceKey: "review-workflow", ResourceID: "skl-1", ExpectedETag: "s2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.Before == nil || !mutation.Before.Deleted || mutation.After == nil || mutation.After.Deleted || mutation.After.ETag != "s3" {
		t.Fatalf("mutation = %#v", mutation)
	}
}

func TestRefinementResourceEditorRejectsUnknownContentFieldsBeforeMutation(t *testing.T) {
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		t.Fatalf("unexpected request: %#v", request)
		return nil
	}}
	editor, _ := NewRefinementResourceEditor(Config{}, WithRefinementResourceTransport(transport))
	_, err := editor.Apply(context.Background(), "project", "op", refinement.Edit{
		Action: refinement.ActionCreate, ResourceKind: refinement.ResourceAgentSpecification, ResourceKey: "reviewer",
		Content: map[string]any{"name": "Reviewer", "description": "Reviews", "instructions": "Review", "permission_mod": "read_only"},
	})
	if !errors.Is(err, refinement.ErrInvalid) {
		t.Fatalf("unknown-field error = %v", err)
	}
}

type refinementTransport struct {
	roundTrip func(*memorylayersdk.Request) *memorylayersdk.Response
}

func (t *refinementTransport) RoundTrip(_ context.Context, request *memorylayersdk.Request) (*memorylayersdk.Response, error) {
	return t.roundTrip(request), nil
}

func (t *refinementTransport) Close() error { return nil }

func refinementJSONResponse(status int, body string) *memorylayersdk.Response {
	return &memorylayersdk.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: []byte(body)}
}
