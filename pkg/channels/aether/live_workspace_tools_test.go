package aether_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
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

	// Start a real client-hosted process, wait until it records its PID, then
	// cancel from the worker side. A fast worker return alone would not prove
	// the reverse cancellation crossed Aether; the client process must exit too.
	cancelCtx, cancelTool := context.WithCancel(turnCtx)
	cancelDone := make(chan error, 1)
	pidFile := filepath.Join(clientRoot, "cancel.pid")
	go func() {
		_, invokeErr := registry.Invoke(cancelCtx, tools.Request{
			CallID: "call-live-cancel", Name: "shell",
			Arguments: json.RawMessage(`{"command":"/bin/sh","args":["-c","echo $$ > cancel.pid; exec sleep 20"]}`),
			Addr:      inbound.Addr,
		})
		cancelDone <- invokeErr
	}()
	var pid int
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		contents, readErr := os.ReadFile(pidFile)
		if readErr == nil {
			pid, err = strconv.Atoi(strings.TrimSpace(string(contents)))
			if err != nil {
				t.Fatalf("parse client tool pid: %v", err)
			}
			break
		}
		if !os.IsNotExist(readErr) {
			t.Fatal(readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("live client tool did not start")
	}
	cancelTool()
	select {
	case invokeErr := <-cancelDone:
		if !errors.Is(invokeErr, context.Canceled) {
			t.Fatalf("live cancelled tool error = %v", invokeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live worker tool call did not return after cancellation")
	}
	processPath := filepath.Join("/proc", strconv.Itoa(pid))
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if _, statErr := os.Stat(processPath); os.IsNotExist(statErr) {
			pid = 0
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid != 0 {
		t.Fatalf("client-hosted process %d survived reverse cancellation", pid)
	}

	client.SetToolHost(nil)
	if _, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "call-live-2", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	}); err == nil {
		t.Fatal("missing live client host silently fell back to worker checkout")
	}
}

// TestLiveAetherScheduledWorkerView verifies the WorkflowEngine path without an
// LLM: a one-shot schedule creates an exact-target BACKGROUND task, the worker
// reconstructs its versioned envelope, and ingress retains the published view.
func TestLiveAetherScheduledWorkerView(t *testing.T) {
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live Aether scheduled-view test")
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
	logicalWorkspace := "e2e-schedule-" + suffix

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	worker, err := aetherchan.New(aetherchan.Config{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: logicalWorkspace,
		Specifier: "schedule-test-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Close() }()
	mlConfig := memorylayer.Config{
		Transport: mlaether.NewTransport(worker, mlaether.WithTarget(memoryLayerTarget)),
		Workspace: logicalWorkspace,
	}
	mlStore, err := memorylayer.New(mlConfig)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := memorylayer.NewWorkspaceViewPublisher(mlConfig, mlStore)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "identity.txt"), []byte("scheduled worker view"), 0o644); err != nil {
		t.Fatal(err)
	}
	host, err := aetherchan.NewWorkerToolHost(ctx, aetherchan.WorkerToolHostConfig{
		WorkspaceID: logicalWorkspace, WorkspaceRoot: root, StateDir: t.TempDir(),
		ToolHostID: worker.Topic(), Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.SetWorkerToolHost(host)
	binding, err := host.ScheduledExecutionBindingForDirectory(ctx, root, true, false)
	if err != nil {
		t.Fatal(err)
	}
	fireAt := time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339)
	if err := worker.EnableScheduledTurns(ctx, []aetherchan.ScheduledTurnRegistration{{
		ID: "once-" + suffix, Name: "E2E scheduled view", Enabled: true,
		ScheduleType: "once", ScheduleExpression: fireAt, MissPolicy: "fire_once",
		ThreadID: "scheduled-e2e", Prompt: "Inspect the exact worker view", Binding: binding,
		ViewPolicy: aetherchan.ScheduledViewPolicy{
			WriteAccess: workspacepkg.ViewWriteAccessReadOnly, AllowMutableView: true,
		},
	}}, nil, 0); err != nil {
		t.Fatal(err)
	}
	inbound, err := worker.FetchTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inbound.Addr.WorkspaceID != logicalWorkspace || inbound.Addr.ThreadID != "scheduled-e2e" ||
		inbound.Addr.TaskID == "" || !aetherchan.IsScheduledTurnMessage(inbound.Message) {
		t.Fatalf("scheduled inbound = %+v", inbound)
	}
	gotBinding, err := spec.GetExecutionBinding(inbound.Message)
	if err != nil || gotBinding == nil || gotBinding.ViewID != binding.ViewID ||
		gotBinding.ToolHostID != worker.Topic() || gotBinding.ExecutionSite != spec.ExecutionSiteWorker {
		t.Fatalf("scheduled binding = %+v err=%v", gotBinding, err)
	}
	fallback, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := tools.RegisterLocal(registry, tools.LocalConfig{Workspace: fallback}); err != nil {
		t.Fatal(err)
	}
	turnCtx := worker.TurnContext(ctx, inbound.Addr)
	readResult, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "scheduled-read", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	})
	if err != nil || !strings.Contains(string(readResult.Payload), "scheduled worker view") {
		t.Fatalf("scheduled exact-view read = %s err=%v", readResult.Payload, err)
	}
	if _, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "scheduled-write", Name: "write_file",
		Arguments: json.RawMessage(`{"path":"forbidden.txt","content":"no"}`), Addr: inbound.Addr,
	}); err == nil || !strings.Contains(err.Error(), "requires write admission") {
		t.Fatalf("scheduled read-only write error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "forbidden.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("scheduled write escaped policy: %v", statErr)
	}
	if err := worker.FailTask(context.Background(), inbound.Addr.TaskID, "E2E inspection complete"); err != nil {
		t.Fatal(err)
	}
}
