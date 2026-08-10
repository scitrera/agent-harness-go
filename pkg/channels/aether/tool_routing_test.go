package aether

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/aetherwire"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

func TestClientBoundToolExecutesOnExactClientWithoutWorkerFallback(t *testing.T) {
	clientRoot := t.TempDir()
	workerRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(clientRoot, "identity.txt"), []byte("client checkout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workerRoot, "identity.txt"), []byte("worker checkout"), 0o644); err != nil {
		t.Fatal(err)
	}

	client, _ := newTestClient(t)
	host, err := NewClientToolHost(context.Background(), ClientToolHostConfig{
		WorkspaceID:   "default",
		WorkspaceRoot: clientRoot,
		StateDir:      t.TempDir(),
		ToolHostID:    client.ToolHostID(),
		AgentTopic:    client.AgentTopic(),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.SetToolHost(host)
	binding, err := host.ExecutionBindingForDirectory(context.Background(), clientRoot)
	if err != nil {
		t.Fatal(err)
	}

	worker, _ := newTestChannel(t)
	addr := protocol.MessageAddress{WorkspaceID: "default", ThreadID: "thread-1", TaskID: "task-1"}
	worker.mu.Lock()
	worker.executionBindings[addr.TaskID] = binding
	worker.executionAccess[addr.TaskID] = workspacepkg.ExecutionBindingAuthorizationRequest{
		Binding: binding, SourceTopic: binding.ToolHostID,
	}
	worker.replyTo[addr.TaskID] = client.ToolHostID()
	worker.mu.Unlock()
	worker.sendToolMessage = func(topic string, payload []byte) error {
		if topic != client.ToolHostID() {
			t.Fatalf("tool target = %q", topic)
		}
		client.handleToolCall(context.Background(), &sdk.Message{SourceTopic: worker.Topic(), Payload: payload})
		return nil
	}
	client.sendToolMessage = func(topic string, payload []byte) error {
		if topic != worker.Topic() {
			t.Fatalf("tool response target = %q", topic)
		}
		return worker.onToolCallMessage(context.Background(), &sdk.Message{
			SourceTopic: client.ToolHostID(), Payload: payload,
		})
	}

	localWorkspace, err := localtools.NewWorkspace(workerRoot)
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := tools.RegisterLocal(registry, tools.LocalConfig{Workspace: localWorkspace}); err != nil {
		t.Fatal(err)
	}
	ctx := worker.TurnContext(context.Background(), addr)
	result, err := registry.Invoke(ctx, tools.Request{
		CallID: "call-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
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
		t.Fatalf("read %q, want client checkout", output.Text)
	}

	// A detached child captures the already-authorized delegate. Parent turn
	// cleanup may remove its task-indexed maps, but that must not redirect or
	// strand the child's exact-host calls.
	worker.mu.Lock()
	delete(worker.executionBindings, addr.TaskID)
	delete(worker.executionAccess, addr.TaskID)
	worker.mu.Unlock()
	if _, err := registry.Invoke(ctx, tools.Request{
		CallID: "call-detached", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
	}); err != nil {
		t.Fatalf("captured detached delegate: %v", err)
	}

	// Once bound, losing the client host is an error. The worker's similarly
	// named local file must never be used as a silent fallback.
	client.SetToolHost(nil)
	_, err = registry.Invoke(ctx, tools.Request{
		CallID: "call-2", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
	})
	if err == nil {
		t.Fatal("missing client host silently fell back to the worker")
	}
}

func TestClientToolHostEnforcesReadOnlyScopeBeforeWrite(t *testing.T) {
	root := t.TempDir()
	host, err := NewClientToolHost(context.Background(), ClientToolHostConfig{
		WorkspaceID: "default", WorkspaceRoot: root, StateDir: t.TempDir(),
		ToolHostID: "us::owner::window-1", AgentTopic: "ag::sahara",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := host.ExecutionBindingForDirectory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	bindingJSON, _ := json.Marshal(binding)
	policyJSON, _ := workspacepkg.EncodeExecutionViewPolicy(workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadOnly,
	})
	envelope := spec.ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion, CallID: "call-write", Name: "write_file",
		Args: map[string]json.RawMessage{
			"path": json.RawMessage(`"forbidden.txt"`), "content": json.RawMessage(`"no"`),
		},
		Addr: spec.MessageAddress{WorkspaceID: "default", TaskID: "task-write"},
		Meta: map[string]json.RawMessage{
			spec.ExecutionBindingMetaKey: bindingJSON, workspacepkg.ExecutionViewPolicyMetaKey: policyJSON,
		},
	}
	_, err = host.invoke(context.Background(), ClientToolAccessRequest{AgentTopic: "ag::sahara"}, envelope)
	if err == nil || !strings.Contains(err.Error(), "requires write admission") {
		t.Fatalf("read-only write error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "forbidden.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("write escaped read-only scope: %v", statErr)
	}
}

func TestInternalCompletionEnqueueRestoresExecutionScope(t *testing.T) {
	worker, _ := newTestChannel(t)
	binding := spec.NewExecutionBinding("project-a", "view-a", "window-a", spec.ExecutionSiteClient)
	scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent", TaskID: "completion-task"}
	message := protocol.ChatMessage{ID: "completion", Role: protocol.RoleUser, Addr: addr}
	if err := workspacepkg.PutExecutionScope(&message, scope); err != nil {
		t.Fatal(err)
	}
	if err := worker.Enqueue(context.Background(), channel.Inbound{Addr: addr, Message: message}); err != nil {
		t.Fatal(err)
	}
	turnCtx := worker.TurnContext(context.Background(), addr)
	got, ok := workspacepkg.ExecutionScopeFrom(turnCtx)
	if !ok || !workspacepkg.ExecutionScopesEqual(&scope, &got) || tools.ToolDelegateFrom(turnCtx) == nil {
		t.Fatalf("restored execution scope = %+v ok=%v delegate=%T", got, ok, tools.ToolDelegateFrom(turnCtx))
	}
}

func TestTurnContextInvalidPolicyStillBlocksWorkspaceFallback(t *testing.T) {
	worker, _ := newTestChannel(t)
	addr := protocol.MessageAddress{WorkspaceID: "project-a", TaskID: "task-invalid-policy"}
	binding := spec.NewExecutionBinding("project-a", "view-a", worker.Topic(), spec.ExecutionSiteWorker)
	worker.mu.Lock()
	worker.executionBindings[addr.TaskID] = binding
	worker.executionPolicies[addr.TaskID] = ScheduledViewPolicy{WriteAccess: "future_access"}
	worker.mu.Unlock()

	ctx := worker.TurnContext(context.Background(), addr)
	if tools.ToolDelegateFrom(ctx) == nil {
		t.Fatal("invalid bound policy dropped the exact-host delegate")
	}
	registry := tools.NewRegistry()
	calledFallback := false
	if err := registry.Register("read_file", tools.HandlerFunc(func(context.Context, tools.Request) (tools.Result, error) {
		calledFallback = true
		return tools.Result{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Invoke(ctx, tools.Request{CallID: "read", Name: "read_file", Addr: addr}); err == nil {
		t.Fatal("invalid exact-view policy unexpectedly invoked a workspace tool")
	}
	if calledFallback {
		t.Fatal("invalid exact-view policy fell back to the worker registry")
	}
}

type testBindingAuthorizer struct{ err error }

func (a testBindingAuthorizer) AuthorizeExecutionBinding(_ context.Context, _ workspacepkg.ExecutionBindingAuthorizationRequest) error {
	return a.err
}

func TestAuthoritativeBindingCanAdmitDynamicWorkspace(t *testing.T) {
	worker, err := New(Config{
		ServerAddr: "127.0.0.1:1", Workspace: "routing", SessionWorkspace: "project-a",
		Specifier: "test", WorkspaceResolver: testWorkspaceResolver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.SetExecutionBindingAuthorizer(testBindingAuthorizer{})
	binding := spec.NewExecutionBinding("project-b", "view-b", "us::drew::w1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-b"
	message := protocol.ChatMessage{
		ID: "user-1", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "thread-1", TaskID: "task-1"},
	}
	if err := spec.PutExecutionBinding(&message, binding); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.onMessage(context.Background(), &sdk.Message{SourceTopic: binding.ToolHostID, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in, err := worker.FetchTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if in.Addr.WorkspaceID != "project-b" {
		t.Fatalf("dynamic workspace = %q", in.Addr.WorkspaceID)
	}
}

func TestRejectedAuthoritativeBindingIsNotEnqueued(t *testing.T) {
	worker, _ := newTestChannel(t)
	rejection := make(chan sentMessage, 1)
	worker.sendMessage = func(topic string, payload []byte) error {
		rejection <- sentMessage{Topic: topic, Payload: payload}
		return nil
	}
	worker.SetExecutionBindingAuthorizer(testBindingAuthorizer{err: errors.New("not authoritative")})
	binding := spec.NewExecutionBinding("default", "view-a", "us::drew::w1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-a"
	message := protocol.ChatMessage{
		ID: "user-1", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{WorkspaceID: "default", ThreadID: "thread-1", TaskID: "task-1"},
	}
	if err := spec.PutExecutionBinding(&message, binding); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(message)
	if err := worker.onMessage(context.Background(), &sdk.Message{SourceTopic: binding.ToolHostID, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case in := <-worker.tasks:
		t.Fatalf("rejected binding enqueued: %+v", in)
	default:
	}
	var sent sentMessage
	select {
	case sent = <-rejection:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for terminal rejection")
	}
	_, streamEvent, ok, err := aetherwire.ParseStreamEnvelope(sent.Payload)
	if err != nil || !ok {
		t.Fatalf("decode rejection: ok=%v err=%v", ok, err)
	}
	final, ok := streamEvent.(spec.MessageFinalizedEvent)
	if !ok || len(final.Message.Content) != 1 {
		t.Fatalf("rejection event = %#v", streamEvent)
	}
	text, ok := final.Message.Content[0].AsText()
	if !ok || text.Text != "Request rejected: workspace tool access was denied." {
		t.Fatalf("rejection text = %q ok=%v", text.Text, ok)
	}
}

func TestClientToolHostDefaultPolicyRejectsDifferentAgent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "identity.txt"), []byte("private"), 0o644); err != nil {
		t.Fatal(err)
	}
	host, err := NewClientToolHost(context.Background(), ClientToolHostConfig{
		WorkspaceID: "default", WorkspaceRoot: root, StateDir: t.TempDir(),
		ToolHostID: "us::owner::window-1", AgentTopic: "ag::sahara",
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := host.ExecutionBindingForDirectory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	bindingJSON, _ := json.Marshal(binding)
	envelope := spec.ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion,
		CallID:        "call-denied",
		Name:          "read_file",
		Args:          map[string]json.RawMessage{"path": json.RawMessage(`"identity.txt"`)},
		Addr:          spec.MessageAddress{WorkspaceID: "default", TaskID: "task-denied"},
		Meta:          map[string]json.RawMessage{spec.ExecutionBindingMetaKey: bindingJSON},
	}
	_, err = host.invoke(context.Background(), ClientToolAccessRequest{AgentTopic: "ag::other"}, envelope)
	if err == nil || !strings.Contains(err.Error(), "unexpected agent") {
		t.Fatalf("different agent error = %v", err)
	}
}

type recordingClientAccessAuthorizer struct {
	request ClientToolAccessRequest
}

func (a *recordingClientAccessAuthorizer) AuthorizeClientTool(_ context.Context, request ClientToolAccessRequest) error {
	a.request = request
	return nil
}

func TestClientToolHostPolicyReceivesCallerAndOBOSubject(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "identity.txt"), []byte("shared"), 0o644); err != nil {
		t.Fatal(err)
	}
	authorizer := &recordingClientAccessAuthorizer{}
	host, err := NewClientToolHost(context.Background(), ClientToolHostConfig{
		WorkspaceID: "shared", WorkspaceRoot: root, StateDir: t.TempDir(),
		ToolHostID: "us::owner::window-1", AccessAuthorizer: authorizer,
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := host.ExecutionBindingForDirectory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	bindingJSON, _ := json.Marshal(binding)
	envelope := spec.ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion, CallID: "call-shared", Name: "read_file",
		Args: map[string]json.RawMessage{"path": json.RawMessage(`"identity.txt"`)},
		Addr: spec.MessageAddress{WorkspaceID: "shared", TaskID: "task-shared"},
		Meta: map[string]json.RawMessage{spec.ExecutionBindingMetaKey: bindingJSON},
	}
	_, err = host.invoke(context.Background(), ClientToolAccessRequest{
		AgentTopic: "ag::sahara",
		OnBehalfOf: workspacepkg.Principal{Type: "user", ID: "requester"},
	}, envelope)
	if err != nil {
		t.Fatal(err)
	}
	if authorizer.request.AgentTopic != "ag::sahara" || authorizer.request.OnBehalfOf.ID != "requester" ||
		authorizer.request.Binding.ViewID != binding.ViewID {
		t.Fatalf("client authorization request = %+v", authorizer.request)
	}
}

type recordingToolAuthorizationProvider struct {
	access workspacepkg.ExecutionBindingAuthorizationRequest
	tool   string
	auth   *pb.AuthorizationContext
}

func (p *recordingToolAuthorizationProvider) AuthorizationForClientTool(
	_ context.Context,
	access workspacepkg.ExecutionBindingAuthorizationRequest,
	toolName string,
) (*pb.AuthorizationContext, error) {
	p.access = access
	p.tool = toolName
	return p.auth, nil
}

func TestReverseToolCallCarriesProviderAuthorization(t *testing.T) {
	worker, _ := newTestChannel(t)
	binding := spec.NewExecutionBinding("shared", "view-1", "us::owner::window-1", spec.ExecutionSiteClient)
	binding.RootRef = "root:view-1"
	access := workspacepkg.ExecutionBindingAuthorizationRequest{
		Binding: binding, SourceTopic: "us::requester::window-2",
		OnBehalfOf: workspacepkg.Principal{Type: "user", ID: "requester"},
	}
	addr := spec.MessageAddress{WorkspaceID: "shared", ThreadID: "thread-1", TaskID: "task-1"}
	worker.mu.Lock()
	worker.executionBindings[addr.TaskID] = binding
	worker.executionAccess[addr.TaskID] = access
	worker.mu.Unlock()
	auth := &pb.AuthorizationContext{
		AuthorityMode: "on_behalf_of",
		Subject:       &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "requester"},
		GrantId:       "grant-1",
	}
	provider := &recordingToolAuthorizationProvider{auth: auth}
	worker.SetToolCallAuthorizationProvider(provider)
	worker.sendAuthorizedToolMessage = func(topic string, payload []byte, got *pb.AuthorizationContext) error {
		if topic != binding.ToolHostID || got != auth {
			t.Fatalf("authorized send topic=%q auth=%#v", topic, got)
		}
		var envelope spec.ToolInvokeEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatal(err)
		}
		body := spec.ToolResultPartBody{
			Type: string(spec.PartToolResult), CallID: envelope.CallID, Name: envelope.Name,
			Output: json.RawMessage(`{"ok":true}`),
			Meta:   map[string]json.RawMessage{toolTaskIDMetaKey: json.RawMessage(`"task-1"`)},
		}
		response, _ := json.Marshal(body)
		return worker.onToolCallMessage(context.Background(), &sdk.Message{
			SourceTopic: binding.ToolHostID, Payload: response,
		})
	}
	result, err := worker.invokeClientTool(context.Background(), binding, tools.Request{
		CallID: "call-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Payload) != `{"ok":true}` {
		t.Fatalf("result = %s", result.Payload)
	}
	if provider.tool != "read_file" || provider.access.OnBehalfOf.ID != "requester" {
		t.Fatalf("provider request = %+v tool=%q", provider.access, provider.tool)
	}
}

func TestCrossHostReverseToolCallRejectsMissingOBOGrant(t *testing.T) {
	worker, _ := newTestChannel(t)
	binding := spec.NewExecutionBinding("shared", "view-1", "us::owner::window-1", spec.ExecutionSiteClient)
	access := workspacepkg.ExecutionBindingAuthorizationRequest{
		Binding: binding, SourceTopic: "us::requester::window-2",
	}
	addr := spec.MessageAddress{WorkspaceID: "shared", ThreadID: "thread-1", TaskID: "task-1"}
	worker.mu.Lock()
	worker.executionBindings[addr.TaskID] = binding
	worker.executionAccess[addr.TaskID] = access
	worker.mu.Unlock()
	worker.SetToolCallAuthorizationProvider(&recordingToolAuthorizationProvider{})
	worker.sendAuthorizedToolMessage = func(string, []byte, *pb.AuthorizationContext) error {
		t.Fatal("cross-host call sent without an OBO grant")
		return nil
	}
	_, err := worker.invokeClientTool(context.Background(), binding, tools.Request{
		CallID: "call-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
	})
	if err == nil || !strings.Contains(err.Error(), "on-behalf-of grant is required") {
		t.Fatalf("missing OBO error = %v", err)
	}
}

func TestCrossHostReverseToolCallRejectsMissingAuthorizationProvider(t *testing.T) {
	worker, _ := newTestChannel(t)
	binding := spec.NewExecutionBinding("shared", "view-1", "us::owner::window-1", spec.ExecutionSiteClient)
	access := workspacepkg.ExecutionBindingAuthorizationRequest{
		Binding: binding, SourceTopic: "us::requester::window-2",
	}
	addr := spec.MessageAddress{WorkspaceID: "shared", ThreadID: "thread-1", TaskID: "task-1"}
	worker.mu.Lock()
	worker.executionBindings[addr.TaskID] = binding
	worker.executionAccess[addr.TaskID] = access
	worker.mu.Unlock()
	worker.sendToolMessage = func(string, []byte) error {
		t.Fatal("cross-host call sent without an authorization provider")
		return nil
	}

	_, err := worker.invokeClientTool(context.Background(), binding, tools.Request{
		CallID: "call-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: addr,
	})
	if err == nil || !strings.Contains(err.Error(), "authorization provider is required") {
		t.Fatalf("missing provider error = %v", err)
	}
}
