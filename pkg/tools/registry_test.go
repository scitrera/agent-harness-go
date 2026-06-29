package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func Test_Registry_Invoke_updates_soul_when_edit_file_tool_called(t *testing.T) {
	// Given
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "SOUL.md"), []byte("# Scitrera\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	ws, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}
	env := protocol.ToolInvokeEnvelope{
		CallID: "call-1",
		Name:   "edit_file",
		Args:   protocol.RawToArgs(json.RawMessage(`{"path":"SOUL.md","old_text":"Scitrera","new_text":"Winick"}`)),
	}

	// When
	result, err := reg.Invoke(ctx, RequestFromEnvelope(env))

	// Then
	if err != nil {
		t.Fatalf("invoke edit_file: %v", err)
	}
	if result.CallID != "call-1" || result.Name != "edit_file" {
		t.Fatalf("unexpected result identity: %#v", result)
	}
	if len(result.Metadata.FileChanges) != 1 || result.Metadata.FileChanges[0].Path != "SOUL.md" || result.Metadata.FileChanges[0].Kind != "edit" {
		t.Fatalf("unexpected file-change metadata: %#v", result.Metadata.FileChanges)
	}
	updated, err := os.ReadFile(filepath.Join(root, "SOUL.md"))
	if err != nil {
		t.Fatalf("read updated file: %v", err)
	}
	if string(updated) != "# Winick\n" {
		t.Fatalf("unexpected file content: %q", string(updated))
	}
}

func Test_Registry_Invoke_returns_unknown_tool_when_name_missing(t *testing.T) {
	// Given
	reg := NewRegistry()

	// When
	_, err := reg.Invoke(context.Background(), Request{Name: "missing"})

	// Then
	if !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("expected ErrUnknownTool, got %v", err)
	}
}

func Test_RegisterLocal_web_search_sends_placeholder_auth_when_exa_configured(t *testing.T) {
	// Given
	ctx := context.Background()
	root := t.TempDir()
	ws, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("authorization")
		_, _ = w.Write([]byte(`{"results":[{"title":"ok"}]}`))
	}))
	defer server.Close()
	exa, err := localtools.NewExaClient(server.URL, "placeholder-rewrite", false, server.Client())
	if err != nil {
		t.Fatalf("exa client: %v", err)
	}
	reg := NewRegistry()
	if err := RegisterLocal(reg, LocalConfig{Workspace: ws, Exa: exa}); err != nil {
		t.Fatalf("register local tools: %v", err)
	}

	// When
	result, err := reg.Invoke(ctx, Request{
		CallID:    "call-search",
		Name:      "web_search",
		Arguments: json.RawMessage(`{"query":"scitrera"}`),
	})

	// Then
	if err != nil {
		t.Fatalf("invoke web_search: %v", err)
	}
	if !strings.Contains(string(result.Payload), "results") {
		t.Fatalf("expected search body in payload, got %s", string(result.Payload))
	}
	if gotAuth != "Bearer placeholder-rewrite" {
		t.Fatalf("unexpected authorization header: %q", gotAuth)
	}
}
