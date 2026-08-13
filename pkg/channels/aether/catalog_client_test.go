package aether

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/catalog/catalogrpc"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const testCatalogClientRoute = "ag::project-a::sahara::one"

func TestCatalogClientQueryUsesOBOContinuationAndExactReplySource(t *testing.T) {
	sender := &catalogHostRecordingSender{}
	var client *CatalogClient
	sender.handle = func(options sdk.SendMessageOptions) error {
		if options.TargetTopic != defaultCatalogServiceTopic || options.MessageType != sdk.MessageTypeChat {
			t.Fatalf("query send = %+v", options)
		}
		if options.Authorization.GetGrantId() != "parent-1" ||
			options.Authorization.GetSubject().GetPrincipalId() != "alice" {
			t.Fatalf("query authorization = %+v", options.Authorization)
		}
		if options.AuthorityContinuation.GetScopeMode() != pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT {
			t.Fatalf("query continuation = %+v", options.AuthorityContinuation)
		}
		var request catalogHostRuntimeEnvelope
		if err := json.Unmarshal(options.Payload, &request); err != nil {
			return err
		}
		if _, ok := request.Arguments[catalogrpc.MethodQuery]; !ok {
			t.Fatalf("query arguments = %v", request.Arguments)
		}
		result, _ := json.Marshal(catalog.ResolvedCatalogPage{
			SchemaVersion: spec.ToolCatalogSchemaVersion, SnapshotID: "snapshot-1",
			CatalogRevision: "sha256:catalog-1",
		})
		reply, _ := json.Marshal(catalogHostRuntimeEnvelope{
			Arguments: map[string]json.RawMessage{"result": result}, RequestID: request.RequestID,
		})
		if client.TryHandle(&sdk.Message{SourceTopic: "sv::other::one", Payload: reply}) {
			t.Fatal("forged reply source was accepted")
		}
		if !client.TryHandle(&sdk.Message{SourceTopic: "sv::tool-catalog::one", Payload: reply}) {
			t.Fatal("exact catalog instance reply was not accepted")
		}
		return nil
	}
	var err error
	client, err = NewCatalogClient(CatalogClientConfig{Route: testCatalogClientRoute, Tenant: "tenant-1", Sender: sender})
	if err != nil {
		t.Fatal(err)
	}
	page, err := client.QueryCatalog(context.Background(), spec.ToolCatalogQuery{
		Context: spec.ToolCatalogContext{WorkspaceID: "project-a"}, Limit: 10,
	}, testCatalogClientAuthority())
	if err != nil {
		t.Fatal(err)
	}
	if page.SnapshotID != "snapshot-1" || page.CatalogRevision != "sha256:catalog-1" {
		t.Fatalf("query page = %+v", page)
	}
}

func TestCatalogClientInvokeChecksExactEntryAndAttenuatesCallerAuthority(t *testing.T) {
	sender := &catalogHostRecordingSender{}
	var client *CatalogClient
	profile := testCatalogAuthorityProfile()
	envelope := testCatalogClientInvocation("call-1")
	sender.handle = func(options sdk.SendMessageOptions) error {
		if options.TargetTopic != testCatalogHostRoute || options.MessageType != sdk.MessageTypeToolCall {
			t.Fatalf("invoke send = %+v", options)
		}
		if options.CheckedAccess.GetResourceType() != "tool-catalog/entry" ||
			options.CheckedAccess.GetOperation() != catalog.CatalogActionInvokeRead ||
			options.CheckedAccess.GetCorrelationId() != envelope.CallID ||
			options.CheckedAccess.GetWorkspace() != "project-a" ||
			options.CheckedAccess.GetRequiredAccessLevel() != 10 {
			t.Fatalf("checked access = %+v", options.CheckedAccess)
		}
		if options.AuthorityContinuation.GetScopeMode() != pb.AuthorityContinuationRequest_SCOPE_MODE_ATTENUATE ||
			options.AuthorityContinuation.GetBindingId() != envelope.CallID ||
			options.AuthorityContinuation.GetScope().GetMaxAccessLevel() != profile.MaxAccessLevel {
			t.Fatalf("invoke continuation = %+v", options.AuthorityContinuation)
		}
		body, _ := json.Marshal(spec.ToolResultPartBody{
			Type: "tool_result", CallID: envelope.CallID, Output: json.RawMessage(`{"vfs_ref":"vfs://result"}`),
		})
		reply, _ := json.Marshal(catalogHostRuntimeEnvelope{
			Arguments: map[string]json.RawMessage{catalogToolResultArgument: body}, RequestID: envelope.CallID,
		})
		if client.TryHandle(&sdk.Message{SourceTopic: "ag::project-a::tool-host::forged", Payload: reply}) {
			t.Fatal("forged provider reply was accepted")
		}
		if !client.TryHandle(&sdk.Message{SourceTopic: testCatalogHostRoute, Payload: reply}) {
			t.Fatal("exact provider reply was not accepted")
		}
		return nil
	}
	var err error
	client, err = NewCatalogClient(CatalogClientConfig{Route: testCatalogClientRoute, Sender: sender})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.InvokeCatalogTool(
		context.Background(), testCatalogHostRoute,
		spec.ToolCatalogContext{WorkspaceID: "project-a"}, spec.ToolEffectRead,
		&profile, envelope, testCatalogClientAuthority(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result), "vfs://result") {
		t.Fatalf("result = %s", result)
	}
}

func TestCatalogClientCancellationReusesCheckedParentAuthorityWithoutContinuation(t *testing.T) {
	sender := &catalogHostRecordingSender{}
	client, err := NewCatalogClient(CatalogClientConfig{Route: testCatalogClientRoute, Sender: sender})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.InvokeCatalogTool(
		ctx, testCatalogHostRoute, spec.ToolCatalogContext{WorkspaceID: "project-a"},
		spec.ToolEffectRead, nil, testCatalogClientInvocation("call-cancel"), testCatalogClientAuthority(),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("invoke error = %v", err)
	}
	sender.mu.Lock()
	options := append([]sdk.SendMessageOptions(nil), sender.options...)
	sender.mu.Unlock()
	if len(options) != 2 {
		t.Fatalf("send count = %d, want invoke + cancel", len(options))
	}
	cancelOptions := options[1]
	if cancelOptions.Authorization.GetGrantId() != "parent-1" || cancelOptions.CheckedAccess == nil ||
		cancelOptions.AuthorityContinuation != nil {
		t.Fatalf("cancel options = %+v", cancelOptions)
	}
	var runtimeEnvelope catalogHostRuntimeEnvelope
	if err := json.Unmarshal(cancelOptions.Payload, &runtimeEnvelope); err != nil {
		t.Fatal(err)
	}
	var cancellation spec.ToolCancelEnvelope
	if err := json.Unmarshal(runtimeEnvelope.Arguments[catalogToolCancelArgument], &cancellation); err != nil {
		t.Fatal(err)
	}
	if cancellation.CallID != "call-cancel" || cancellation.Reason != "caller_cancelled" || cancellation.Addr.TaskID != "task-1" {
		t.Fatalf("cancellation = %+v", cancellation)
	}
}

func TestCatalogClientRejectsMissingOBOAndNonAttenuableUserHost(t *testing.T) {
	client, err := NewCatalogClient(CatalogClientConfig{Route: testCatalogClientRoute, Sender: &catalogHostRecordingSender{}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.QueryCatalog(context.Background(), spec.ToolCatalogQuery{
		Context: spec.ToolCatalogContext{WorkspaceID: "project-a"}, Limit: 10,
	}, tools.MemoryAuthority{})
	if err == nil || !strings.Contains(err.Error(), "user OBO authority") {
		t.Fatalf("missing OBO error = %v", err)
	}
	profile := testCatalogAuthorityProfile()
	_, err = client.InvokeCatalogTool(
		context.Background(), "us::alice::window-1",
		spec.ToolCatalogContext{WorkspaceID: "project-a", ToolHostID: "us::alice::window-1", SurfaceKind: "tui", SurfaceInstanceID: "window-1"},
		spec.ToolEffectRead, &profile, testCatalogClientInvocation("call-user"), testCatalogClientAuthority(),
	)
	if err == nil || !strings.Contains(err.Error(), "exact service or agent") {
		t.Fatalf("user-session attenuation error = %v", err)
	}
}

func testCatalogClientAuthority() tools.MemoryAuthority {
	return tools.MemoryAuthority{SubjectType: "user", SubjectID: "alice", GrantID: "parent-1"}
}

func testCatalogClientInvocation(callID string) spec.ToolInvokeEnvelope {
	ref := spec.ToolReference{
		ProviderID: "documents", RegistrationID: "tool-host-one", Generation: "generation-1",
		Name: "vfs_search", Revision: "sha256:vfs-search-v1",
	}
	envelope := spec.NewToolInvokeEnvelope(callID, ref.Name)
	envelope.ToolRef = &ref
	envelope.Addr = spec.MessageAddress{
		WorkspaceID: "project-a", ThreadID: "thread-1", TaskID: "task-1", UserID: "alice",
	}
	envelope.Args = map[string]json.RawMessage{"query": json.RawMessage(`"contracts"`)}
	return envelope
}

func TestCatalogClientExactServiceSourceDoesNotAcceptSibling(t *testing.T) {
	client, err := NewCatalogClient(CatalogClientConfig{
		Route: testCatalogClientRoute, CatalogServiceTopic: "sv::tool-catalog::one",
		Sender: &catalogHostRecordingSender{},
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter, err := client.register("request-1", "sv::tool-catalog::one")
	if err != nil {
		t.Fatal(err)
	}
	defer client.release("request-1", waiter)
	payload := []byte(`{"request_id":"request-1"}`)
	if client.TryHandle(&sdk.Message{SourceTopic: "sv::tool-catalog::two", Payload: payload}) {
		t.Fatal("sibling catalog instance was accepted")
	}
	if !client.TryHandle(&sdk.Message{SourceTopic: "sv::tool-catalog::one", Payload: payload}) {
		t.Fatal("configured exact catalog instance was rejected")
	}
	select {
	case <-waiter.result:
	case <-time.After(time.Second):
		t.Fatal("matched reply was not delivered")
	}
}
