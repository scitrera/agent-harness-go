package aether_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
	mlaether "github.com/scitrera/memorylayer/memorylayer-sdk-go/aether"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/memorylayer"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// TestLiveAetherClientWorkspaceToolRouting is an opt-in deterministic E2E. It
// exercises the real gateway and MemoryLayer service without relying on an LLM
// to choose a tool. Run it against oss/e2e with AETHER_E2E_ADDR set.
func TestLiveAetherClientWorkspaceToolRouting(t *testing.T) {
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live Aether/MemoryLayer tool-routing test")
	}
	aetherWorkspace := os.Getenv("AETHER_E2E_WORKSPACE")
	if aetherWorkspace == "" {
		aetherWorkspace = "default"
	}
	memoryLayerTarget := os.Getenv("MEMORYLAYER_E2E_TARGET")
	if memoryLayerTarget == "" {
		memoryLayerTarget = "sv::memorylayer"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	logicalWorkspace := "e2e-view-" + suffix
	specifier := "view-test-" + suffix
	windowID := "window-" + suffix

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	worker, err := aetherchan.New(aetherchan.Config{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: logicalWorkspace,
		Specifier: specifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Close() }()
	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: logicalWorkspace,
		AgentSpecifier: specifier, UserID: "workspace-e2e", WindowID: windowID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	clientMLConfig := memorylayer.Config{
		Transport: mlaether.NewTransport(client, mlaether.WithTarget(memoryLayerTarget)),
		Workspace: logicalWorkspace,
	}
	clientStore, err := memorylayer.New(clientMLConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientPublisher, err := memorylayer.NewWorkspaceViewPublisher(clientMLConfig, clientStore)
	if err != nil {
		t.Fatal(err)
	}
	workerMLConfig := memorylayer.Config{
		Transport: mlaether.NewTransport(worker, mlaether.WithTarget(memoryLayerTarget)),
		Workspace: logicalWorkspace,
	}
	workerStore, err := memorylayer.New(workerMLConfig)
	if err != nil {
		t.Fatal(err)
	}
	workerPublisher, err := memorylayer.NewWorkspaceViewPublisher(workerMLConfig, workerStore)
	if err != nil {
		t.Fatal(err)
	}
	worker.SetExecutionBindingAuthorizer(workerPublisher)

	clientRoot := t.TempDir()
	workerRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(clientRoot, "identity.txt"), []byte("client checkout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workerRoot, "identity.txt"), []byte("worker checkout"), 0o644); err != nil {
		t.Fatal(err)
	}
	host, err := aetherchan.NewClientToolHost(ctx, aetherchan.ClientToolHostConfig{
		WorkspaceID: logicalWorkspace, WorkspaceRoot: clientRoot, StateDir: t.TempDir(),
		ToolHostID: client.ToolHostID(), AgentTopic: client.AgentTopic(), Publisher: clientPublisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.SetToolHost(host)
	binding, err := host.ExecutionBindingForDirectory(ctx, clientRoot)
	if err != nil {
		t.Fatal(err)
	}
	message := protocol.ChatMessage{
		ID: "user-task-1", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{WorkspaceID: logicalWorkspace, ThreadID: "thread-1", TaskID: "task-1"},
	}
	if err := spec.PutExecutionBinding(&message, binding); err != nil {
		t.Fatal(err)
	}
	if err := client.Enqueue(ctx, channel.Inbound{Addr: message.Addr, Message: message}); err != nil {
		t.Fatal(err)
	}
	inbound, err := worker.FetchTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workerWorkspace, err := localtools.NewWorkspace(workerRoot)
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := tools.RegisterLocal(registry, tools.LocalConfig{Workspace: workerWorkspace}); err != nil {
		t.Fatal(err)
	}
	turnCtx := worker.TurnContext(ctx, inbound.Addr)
	result, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "call-live-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	if output.Text != "client checkout" {
		t.Fatalf("live routed read = %q, want client checkout", output.Text)
	}

	client.SetToolHost(nil)
	if _, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "call-live-2", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	}); err == nil {
		t.Fatal("missing live client host silently fell back to worker checkout")
	}
}
