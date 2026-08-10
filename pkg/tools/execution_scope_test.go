package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

func TestRegistryExecutionScopeDeniesPotentialViewMutation(t *testing.T) {
	registry := NewRegistry()
	for _, name := range []string{"read_file", "write_file", "edit_file", "shell", "python"} {
		name := name
		if err := registry.Register(name, HandlerFunc(func(_ context.Context, req Request) (Result, error) {
			return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
		})); err != nil {
			t.Fatal(err)
		}
	}
	binding := spec.NewExecutionBinding("project-a", "view-a", "worker-a", spec.ExecutionSiteWorker)
	scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := workspacepkg.WithExecutionScope(context.Background(), scope)
	if _, err := registry.Invoke(ctx, Request{CallID: "read", Name: "read_file"}); err != nil {
		t.Fatalf("read_file denied: %v", err)
	}
	for _, name := range []string{"write_file", "edit_file", "shell", "python"} {
		_, err := registry.Invoke(ctx, Request{CallID: name, Name: name, Approved: true})
		if err == nil || !strings.Contains(err.Error(), "requires write admission") {
			t.Fatalf("%s error = %v", name, err)
		}
	}
}
