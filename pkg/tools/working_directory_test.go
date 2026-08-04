package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestWorkingDirectoryMetadataRoundTrip(t *testing.T) {
	cwd := filepath.Join(t.TempDir(), "code")
	message := protocol.ChatMessage{}
	StampWorkingDirectory(&message, cwd)

	got, ok := MessageWorkingDirectory(message)
	if !ok || got != cwd {
		t.Fatalf("working directory = %q, %v; want %q, true", got, ok, cwd)
	}
}

func TestLocalToolsResolveRelativePathsFromTurnWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	if err := os.WriteFile(filepath.Join(external, "marker.txt"), []byte("external cwd"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	workspace, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	if err := workspace.GrantWorkingDirectory(external); err != nil {
		t.Fatalf("grant cwd: %v", err)
	}
	registry := NewRegistry()
	if err := RegisterLocal(registry, LocalConfig{Workspace: workspace}); err != nil {
		t.Fatalf("register tools: %v", err)
	}
	ctx := WithWorkingDirectory(context.Background(), external)

	readResult, err := registry.Invoke(ctx, Request{CallID: "read", Name: "read_file", Arguments: json.RawMessage(`{"path":"marker.txt"}`)})
	if err != nil {
		t.Fatalf("relative read: %v", err)
	}
	if !strings.Contains(string(readResult.Payload), "external cwd") {
		t.Fatalf("read payload = %s", readResult.Payload)
	}

	listResult, err := registry.Invoke(ctx, Request{CallID: "list", Name: "list_dir", Arguments: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("default list cwd: %v", err)
	}
	if !strings.Contains(string(listResult.Payload), "marker.txt") {
		t.Fatalf("list payload = %s", listResult.Payload)
	}

	shellResult, err := registry.Invoke(ctx, Request{CallID: "shell", Name: "shell", Arguments: json.RawMessage(`{"command":"pwd"}`)})
	if err != nil {
		t.Fatalf("default shell cwd: %v", err)
	}
	if !strings.Contains(string(shellResult.Payload), external) {
		t.Fatalf("shell payload = %s", shellResult.Payload)
	}
}

func TestLocalWriteResolvesIntoActiveWorkspaceSubdirectory(t *testing.T) {
	root := t.TempDir()
	subdir := filepath.Join(root, "src")
	if err := os.Mkdir(subdir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	workspace, err := localtools.NewWorkspace(root)
	if err != nil {
		t.Fatalf("workspace: %v", err)
	}
	registry := NewRegistry()
	if err := RegisterLocal(registry, LocalConfig{Workspace: workspace}); err != nil {
		t.Fatalf("register tools: %v", err)
	}
	ctx := WithWorkingDirectory(context.Background(), subdir)

	if _, err := registry.Invoke(ctx, Request{CallID: "write", Name: "write_file", Arguments: json.RawMessage(`{"path":"new.txt","content":"hello"}`)}); err != nil {
		t.Fatalf("relative write: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(subdir, "new.txt"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("written data = %q, err %v", data, err)
	}
}
