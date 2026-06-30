package localtools

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func Test_Workspace_EditFile_changes_SOUL_name_when_user_requests_rename(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	if err := ws.WriteFile(ctx, "SOUL.md", "# SOUL.md - Scitrera\n"); err != nil {
		t.Fatalf("write soul: %v", err)
	}

	err := ws.EditFile(ctx, "SOUL.md", "Scitrera", "Winick")

	if err != nil {
		t.Fatalf("edit soul: %v", err)
	}
	got, err := ws.ReadFile(ctx, "SOUL.md", 0)
	if err != nil {
		t.Fatalf("read soul: %v", err)
	}
	if !strings.Contains(got, "Winick") || strings.Contains(got, "Scitrera") {
		t.Fatalf("unexpected soul content %q", got)
	}
}

func Test_Workspace_ReadFile_rejects_path_traversal(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)

	_, err := ws.ReadFile(ctx, "../secret", 0)

	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("expected traversal rejection, got %v", err)
	}
}

func Test_Workspace_ReadFile_accepts_absolute_path_inside_root(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	if err := ws.WriteFile(ctx, "work/test.txt", "hello"); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The model commonly passes an absolute path under the workspace root
	// (e.g. /workspace/work/test.txt); it must resolve, not be rejected.
	abs := filepath.Join(ws.root, "work", "test.txt")
	got, err := ws.ReadFile(ctx, abs, 0)
	if err != nil {
		t.Fatalf("read absolute in-root: %v", err)
	}
	if got != "hello" {
		t.Fatalf("unexpected content %q", got)
	}
}

func Test_Workspace_ReadFile_rejects_absolute_path_outside_root(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside: %v", err)
	}

	_, err := ws.ReadFile(ctx, outside, 0)

	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("expected ErrPathOutsideRoot for absolute path outside root, got %v", err)
	}
}

func Test_Workspace_ReadFile_rejects_symlink_escape(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(ws.root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err := ws.ReadFile(ctx, "link", 0)

	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("expected ErrPathOutsideRoot, got %v", err)
	}
}

func Test_Workspace_RunPython_computes_inside_workspace(t *testing.T) {
	ctx := context.Background()
	ws := newTestWorkspace(t)

	got, err := ws.RunPython(ctx, "python3", "print(6*7)", CommandSpec{Timeout: 5 * time.Second, MaxOutput: 1024})

	if err != nil {
		t.Fatalf("python: %v", err)
	}
	if strings.TrimSpace(got.Output) != "42" {
		t.Fatalf("expected 42, got %q", got.Output)
	}
}

func Test_ExaClient_uses_placeholder_auth(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("authorization")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()
	client, err := NewExaClient(server.URL, "placeholder-exa-sidecar-rewrite", false, server.Client())
	if err != nil {
		t.Fatalf("new exa client: %v", err)
	}

	result, err := client.Search(context.Background(), "test")

	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if !strings.Contains(gotAuth, "placeholder-exa-sidecar-rewrite") {
		t.Fatalf("expected placeholder auth in Authorization header, got %q", gotAuth)
	}
	if result.Body == "" {
		t.Fatal("expected body")
	}
}

func Test_ExaClient_rejects_real_key(t *testing.T) {
	_, err := NewExaClient("http://127.0.0.1", "exa_real_key", false, nil)

	if !errors.Is(err, ErrExternalAPIKey) {
		t.Fatalf("expected ErrExternalAPIKey, got %v", err)
	}
}

func Test_ExaClient_allowDirect_accepts_real_key_and_sends_x_api_key(t *testing.T) {
	var gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client, err := NewExaClient(server.URL, "exa_real_key", true, server.Client())
	if err != nil {
		t.Fatalf("allow-direct should accept a real key: %v", err)
	}
	if _, err := client.Search(context.Background(), "test"); err != nil {
		t.Fatalf("search: %v", err)
	}
	if gotKey != "exa_real_key" {
		t.Fatalf("expected real key in x-api-key header, got %q", gotKey)
	}
}

func newTestWorkspace(t *testing.T) *Workspace {
	t.Helper()
	ws, err := NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	return ws
}
