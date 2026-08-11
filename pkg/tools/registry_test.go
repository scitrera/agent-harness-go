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

func Test_Registry_Prepare_admits_without_executing(t *testing.T) {
	reg := NewRegistry()
	invocations := 0
	if err := reg.Register("read", HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		invocations++
		return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"ok":true}`))
	})); err != nil {
		t.Fatal(err)
	}
	prepared, err := reg.Prepare(context.Background(), Request{CallID: "call-1", Name: "read"})
	if err != nil {
		t.Fatal(err)
	}
	if invocations != 0 {
		t.Fatal("Prepare executed the tool body")
	}
	if _, err := prepared.Invoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if invocations != 1 {
		t.Fatalf("invocations = %d", invocations)
	}
	if _, err := prepared.Invoke(context.Background()); !errors.Is(err, ErrInvalidTool) {
		t.Fatalf("second invocation error = %v", err)
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

// Test_Registry_SetExcluded_hides_and_blocks asserts an excluded tool never
// enters the registry: Register/Describe no-op, it is absent from Names +
// Descriptors (so the model never sees it), and Invoke returns ErrUnknownTool —
// while non-excluded tools register normally.
func Test_Registry_SetExcluded_hides_and_blocks(t *testing.T) {
	reg := NewRegistry()
	reg.SetExcluded([]string{"web_search", "  "}) // blank entries ignored
	noop := HandlerFunc(func(_ context.Context, req Request) (Result, error) {
		return NewJSONResult(req.CallID, req.Name, json.RawMessage(`{}`))
	})

	if err := reg.Register("web_search", noop); err != nil {
		t.Fatalf("excluded Register should silently no-op, got %v", err)
	}
	reg.Describe(Descriptor{Name: "web_search", Description: "search"})
	if err := reg.Register("read_file", noop); err != nil {
		t.Fatalf("non-excluded Register: %v", err)
	}

	for _, n := range reg.Names() {
		if n == "web_search" {
			t.Fatalf("excluded tool present in Names(): %v", reg.Names())
		}
	}
	for _, d := range reg.Descriptors() {
		if d.Name == "web_search" {
			t.Fatalf("excluded tool present in Descriptors()")
		}
	}
	if _, err := reg.Invoke(context.Background(), Request{Name: "web_search"}); !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("excluded Invoke = %v, want ErrUnknownTool", err)
	}
	// The non-excluded tool registered normally.
	var haveReadFile bool
	for _, n := range reg.Names() {
		if n == "read_file" {
			haveReadFile = true
		}
	}
	if !haveReadFile {
		t.Fatalf("non-excluded tool missing from registry: %v", reg.Names())
	}
}
