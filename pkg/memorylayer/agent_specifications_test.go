package memorylayer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func TestAgentSpecificationProviderMapsTypedSDKPaginationWorkspaceAndAuthority(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/v1/agent-specifications" || r.URL.Query().Get("workspace_id") != "requested" {
			t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("X-Aether-Grant-ID") != "grant-1" || r.Header.Get("X-Aether-Subject-ID") != "user-1" {
			t.Errorf("authority headers = %#v", r.Header)
		}
		if requests == 1 {
			_, _ = fmt.Fprint(w, `{"specifications":[{"id":"one","key":"reviewer","name":"Reviewer","description":"Review evidence","instructions":"Cite evidence","invocation_guidance":"Before completion","model":"strong","max_turns":4,"allowed_tools":["read_file"],"denied_tools":["shell"],"skills":["review"],"mcp_servers":[],"permission_mode":"read_only","exec_policy_hint":"","background":false,"enabled":true,"schema_version":1,"metadata":{},"revision":2,"etag":"e1"},{"id":"off","key":"disabled","name":"Disabled","description":"Disabled","instructions":"Disabled","enabled":false,"schema_version":99,"metadata":{},"revision":1,"etag":"e2"}],"next_page_token":"next"}`)
			return
		}
		if r.URL.Query().Get("page_token") != "next" {
			t.Errorf("page token = %q", r.URL.Query().Get("page_token"))
		}
		_, _ = fmt.Fprint(w, `{"specifications":[{"id":"two","key":"analyst","name":"Analyst","description":"Analyze","instructions":"Analyze carefully","allowed_tools":[],"denied_tools":[],"skills":[],"mcp_servers":[],"permission_mode":"inherit","enabled":true,"schema_version":1,"metadata":{},"revision":1,"etag":"e3"}],"next_page_token":null}`)
	}))
	defer server.Close()

	provider, err := NewAgentSpecificationProvider(Config{BaseURL: server.URL, Workspace: "configured"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithMemoryAuthority(context.Background(), tools.MemoryAuthority{GrantID: "grant-1", SubjectType: "user", SubjectID: "user-1"})
	definitions, err := provider.LoadWorkspace(ctx, "requested")
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 2 || definitions[0].Type != "analyst" || definitions[1].Type != "reviewer" {
		t.Fatalf("definitions = %#v", definitions)
	}
	reviewer := definitions[1]
	if reviewer.Name != "Reviewer" || reviewer.Model != "strong" || reviewer.MaxTurns != 4 || reviewer.PermissionMode != "read_only" {
		t.Fatalf("reviewer = %#v", reviewer)
	}
	if reviewer.Description != "Review evidence\nWhen to use: Before completion" || reviewer.Prompt != "Cite evidence" {
		t.Fatalf("reviewer prompt fields = %#v", reviewer)
	}
}

func TestAgentSpecificationProviderRejectsCursorCyclesAndUnsupportedEnabledSchemas(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "cursor cycle", body: `{"specifications":[],"next_page_token":"same"}`, want: "cursor cycle"},
		{name: "schema", body: `{"specifications":[{"id":"bad","key":"bad","name":"Bad","description":"Bad","instructions":"Bad","enabled":true,"schema_version":2,"metadata":{},"revision":1,"etag":"e"}]}`, want: "unsupported schema version"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, test.body) }))
			defer server.Close()
			provider, err := NewAgentSpecificationProvider(Config{BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.LoadWorkspace(context.Background(), "workspace")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
