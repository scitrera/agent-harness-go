package memorylayer

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

func TestWorkspaceViewPublisherPersistsAndAuthorizesExactHostWithoutLocalPath(t *testing.T) {
	const (
		workspaceID = "project-a"
		viewID      = "view-a"
		observerID  = "us::drew::window-1"
		rootRef     = "root:view-a"
	)
	created := false
	requests := 0
	transport := &refinementTransport{roundTrip: func(request *memorylayersdk.Request) *memorylayersdk.Response {
		requests++
		viewJSON := `{"id":"vrs-view","tenant_id":"_default","workspace_id":"project-a","view_id":"view-a","kind":"git_worktree","display_name":"agent-harness","capabilities":["file.read"],"metadata":{},"schema_version":1,"revision":1,"sequence":1,"etag":"view-etag","created_at":"2026-08-10T12:00:00Z","updated_at":"2026-08-10T12:00:00Z"}`
		observationJSON := `{"id":"vrs-observation","tenant_id":"_default","workspace_id":"project-a","view_id":"view-a","observer_id":"us::drew::window-1","generation":"generation-a","sequence":1,"tool_host_id":"us::drew::window-1","execution_site":"client","root_ref":"root:view-a","capabilities":["file.read"],"vcs":{"kind":"git","head_revision":"abc123","dirty":false,"detached":false},"observed_at":"2026-08-10T12:00:00Z","metadata":{},"schema_version":1,"resource_revision":1,"resource_sequence":1,"etag":"observation-etag","created_at":"2026-08-10T12:00:00Z","updated_at":"2026-08-10T12:00:00Z"}`
		switch {
		case request.Method == http.MethodGet && request.Path == "/workspaces/project-a/views/view-a":
			if !created {
				return refinementJSONResponse(http.StatusNotFound, `{"detail":"not found"}`)
			}
			return refinementJSONResponse(http.StatusOK, `{"view":`+viewJSON+`}`)
		case request.Method == http.MethodPost && request.Path == "/workspaces/project-a/views":
			if strings.Contains(string(request.Body), "/private/client/checkout") {
				t.Fatalf("view write leaked host-local root: %s", request.Body)
			}
			created = true
			return refinementJSONResponse(http.StatusCreated, `{"view":`+viewJSON+`,"replayed":false}`)
		case request.Method == http.MethodPut && strings.Contains(request.Path, "/observations/"):
			if strings.Contains(string(request.Body), "/private/client/checkout") {
				t.Fatalf("observation leaked host-local root: %s", request.Body)
			}
			return refinementJSONResponse(http.StatusCreated, `{"observation":`+observationJSON+`,"replayed":false}`)
		case request.Method == http.MethodGet && strings.Contains(request.Path, "/observations/"):
			return refinementJSONResponse(http.StatusOK, `{"observation":`+observationJSON+`}`)
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.Path)
			return refinementJSONResponse(http.StatusInternalServerError, `{}`)
		}
	}}
	store, err := New(Config{Transport: transport, Workspace: workspaceID})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.ensured[workspaceID] = true
	store.mu.Unlock()
	publisher, err := NewWorkspaceViewPublisher(Config{Transport: transport, Workspace: workspaceID}, store)
	if err != nil {
		t.Fatal(err)
	}
	view := workspacepkg.View{
		Descriptor: spec.WorkspaceViewDescriptor{
			WorkspaceID: workspaceID, ViewID: viewID, Kind: spec.WorkspaceViewKindGitWorktree,
			DisplayName: "agent-harness", Capabilities: []string{"file.read"}, Revision: "abc123",
		},
		Root: "/private/client/checkout", RootRef: rootRef,
		VCS: &workspacepkg.VCSObservation{Kind: "git", HeadRevision: "abc123"},
	}
	observation := workspacepkg.ViewObservation{
		ObserverID: observerID, Generation: "generation-a", Sequence: 1,
		ToolHostID: observerID, ExecutionSite: spec.ExecutionSiteClient, RootRef: rootRef,
		Capabilities: []string{"file.read"}, VCS: view.VCS,
	}
	if err := publisher.PublishWorkspaceView(context.Background(), view, observation); err != nil {
		t.Fatal(err)
	}
	binding := spec.NewExecutionBinding(workspaceID, viewID, observerID, spec.ExecutionSiteClient)
	binding.RootRef = rootRef
	binding.Revision = "abc123"
	validator, err := NewWorkspaceBindingValidator(Config{Transport: transport, Workspace: workspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.ValidateExecutionBinding(context.Background(), binding); err != nil {
		t.Fatalf("authority-only exact validation: %v", err)
	}
	if err := publisher.AuthorizeExecutionBinding(context.Background(), workspacepkg.ExecutionBindingAuthorizationRequest{
		Binding: binding, SourceTopic: "sv::platform-bridge::edge-1", RequestUserID: "drew",
	}); err == nil {
		t.Fatal("standalone authorizer accepted a different source topic")
	}
	if err := publisher.AuthorizeExecutionBinding(context.Background(), workspacepkg.ExecutionBindingAuthorizationRequest{
		Binding: binding, SourceTopic: observerID, RequestUserID: "drew",
	}); err != nil {
		t.Fatal(err)
	}
	if requests != 7 {
		t.Fatalf("requests = %d, want 7", requests)
	}

	wrongHost := binding
	wrongHost.ToolHostID = "us::drew::window-2"
	if err := validator.ValidateExecutionBinding(context.Background(), wrongHost); err == nil {
		t.Fatal("validator substituted another observer for the requested host")
	}
	if data, _ := json.Marshal(wrongHost); strings.Contains(string(data), "/private/client/checkout") {
		t.Fatal("binding serialized a host-local path")
	}
}
