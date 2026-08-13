package aether

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/catalog/catalogrpc"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	testCatalogHostRoute   = "ag::project-a::tool-host::one"
	testCatalogCallerRoute = "ag::project-a::sahara::one"
)

type catalogHostRecordingSender struct {
	mu      sync.Mutex
	options []sdk.SendMessageOptions
	handle  func(sdk.SendMessageOptions) error
}

func (s *catalogHostRecordingSender) SendWithOptions(options sdk.SendMessageOptions) error {
	s.mu.Lock()
	s.options = append(s.options, options)
	s.mu.Unlock()
	if s.handle != nil {
		return s.handle(options)
	}
	return nil
}

func TestCatalogToolHostInstallsOnlyReviewedInvocationAuthority(t *testing.T) {
	sender := &catalogHostRecordingSender{}
	seen := make(chan tools.MemoryAuthority, 1)
	registry := testCatalogRegistry(t, tools.HandlerFunc(func(ctx context.Context, req tools.Request) (tools.Result, error) {
		fromContext, ok := tools.MemoryAuthorityFrom(ctx)
		if !ok || fromContext != req.Authority {
			t.Fatalf("handler authority context=%+v request=%+v", fromContext, req.Authority)
		}
		seen <- req.Authority
		return tools.NewJSONResult(req.CallID, req.Name, json.RawMessage(`{"vfs_ref":"vfs://result"}`))
	}))
	profile := testCatalogAuthorityProfile()
	host := testCatalogToolHost(t, sender, registry, &profile)
	message := testCatalogInvocationMessage(t, host, "call-1", &profile)
	if err := host.Handle(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	select {
	case authority := <-seen:
		if authority.GrantID != "child-call-1" || authority.SubjectID != "alice" {
			t.Fatalf("handler authority = %+v", authority)
		}
	case <-time.After(time.Second):
		t.Fatal("exported handler was not invoked")
	}
	body := awaitCatalogToolResult(t, sender)
	if body.IsError || body.Error != nil || string(body.Output) != `{"vfs_ref":"vfs://result"}` {
		t.Fatalf("tool result = %+v", body)
	}
}

func TestCatalogToolHostRejectsUnreviewedOrMismatchedContinuation(t *testing.T) {
	expected := testCatalogAuthorityProfile()
	wrong := testCatalogAuthorityProfile()
	wrong.OperationScope = []string{"vfs.delete"}
	for name, testCase := range map[string]struct {
		profile          *catalog.InvocationAuthorityProfile
		forwardedProfile *catalog.InvocationAuthorityProfile
		want             string
	}{
		"unreviewed":  {profile: nil, forwardedProfile: ptrCatalogProfile(testCatalogAuthorityProfile()), want: "unreviewed forwarded authority"},
		"wrong scope": {profile: &expected, forwardedProfile: &wrong, want: "wrong operation scope"},
	} {
		t.Run(name, func(t *testing.T) {
			sender := &catalogHostRecordingSender{}
			invoked := false
			registry := testCatalogRegistry(t, tools.HandlerFunc(func(context.Context, tools.Request) (tools.Result, error) {
				invoked = true
				return tools.Result{}, nil
			}))
			host := testCatalogToolHost(t, sender, registry, testCase.profile)
			message := testCatalogInvocationMessage(t, host, "call-denied", testCase.forwardedProfile)
			if err := host.Handle(context.Background(), message); err != nil {
				t.Fatal(err)
			}
			body := awaitCatalogToolResult(t, sender)
			if invoked || !body.IsError || body.Error == nil || !strings.Contains(body.Error.Message, testCase.want) {
				t.Fatalf("invoked=%v result=%+v", invoked, body)
			}
		})
	}
}

func TestCatalogToolHostCheckedCancellationStopsExactCall(t *testing.T) {
	sender := &catalogHostRecordingSender{}
	started := make(chan struct{})
	registry := testCatalogRegistry(t, tools.HandlerFunc(func(ctx context.Context, req tools.Request) (tools.Result, error) {
		close(started)
		<-ctx.Done()
		return tools.Result{}, ctx.Err()
	}))
	profile := testCatalogAuthorityProfile()
	host := testCatalogToolHost(t, sender, registry, &profile)
	message := testCatalogInvocationMessage(t, host, "call-cancel", &profile)
	if err := host.Handle(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("tool did not start")
	}
	var request catalogHostRuntimeEnvelope
	if err := json.Unmarshal(message.Payload, &request); err != nil {
		t.Fatal(err)
	}
	var invocation spec.ToolInvokeEnvelope
	if err := json.Unmarshal(request.Arguments[catalogToolInvokeArgument], &invocation); err != nil {
		t.Fatal(err)
	}
	cancelEnvelope := spec.NewToolCancelEnvelope(invocation.CallID, invocation.Addr)
	cancelRaw, _ := json.Marshal(cancelEnvelope)
	cancelPayload, _ := json.Marshal(catalogHostRuntimeEnvelope{
		Arguments: map[string]json.RawMessage{catalogToolCancelArgument: cancelRaw}, RequestID: invocation.CallID,
	})
	cancelMessage := *message
	cancelMessage.Payload = cancelPayload
	cancelMessage.ForwardedAuthorization = nil
	if err := host.Handle(context.Background(), &cancelMessage); err != nil {
		t.Fatal(err)
	}
	body := awaitCatalogToolResult(t, sender)
	if body.Error == nil || body.Error.Type != "tool_cancelled" || !body.IsError {
		t.Fatalf("cancel result = %+v", body)
	}
}

func TestCatalogToolHostPublishesWorkspaceWidePrivateRouteAndLifecycle(t *testing.T) {
	sender := &catalogHostRecordingSender{}
	registry := testCatalogRegistry(t, tools.HandlerFunc(func(context.Context, tools.Request) (tools.Result, error) {
		return tools.Result{}, nil
	}))
	host := testCatalogToolHost(t, sender, registry, nil)
	var methods []string
	var sequences []uint64
	sender.handle = func(options sdk.SendMessageOptions) error {
		if options.TargetTopic != defaultCatalogServiceTopic || options.CheckedAccess == nil ||
			options.CheckedAccess.GetResourceType() != catalogToolProviderResourceType {
			t.Fatalf("catalog send options = %+v", options)
		}
		var request catalogHostRuntimeEnvelope
		if err := json.Unmarshal(options.Payload, &request); err != nil {
			return err
		}
		for _, method := range []string{catalogrpc.MethodPublish, catalogrpc.MethodRenew, catalogrpc.MethodRevoke} {
			if raw, ok := request.Arguments[method]; ok {
				methods = append(methods, method)
				var projection struct {
					Binding     catalogrpc.MutationBinding   `json:"binding"`
					Publication *spec.ToolCatalogPublication `json:"publication"`
					Request     json.RawMessage              `json:"request"`
				}
				if err := json.Unmarshal(raw, &projection); err != nil {
					return err
				}
				if projection.Binding.ProviderRoute != testCatalogHostRoute || projection.Binding.RequiredContext.ToolHostID != "" {
					t.Fatalf("private binding = %+v", projection.Binding)
				}
				if projection.Publication != nil {
					sequences = append(sequences, projection.Publication.Sequence)
				} else {
					var mutation struct {
						Sequence uint64 `json:"sequence"`
					}
					if err := json.Unmarshal(projection.Request, &mutation); err != nil {
						return err
					}
					sequences = append(sequences, mutation.Sequence)
				}
			}
		}
		result, _ := json.Marshal(spec.ToolCatalogMutationResult{
			SchemaVersion: spec.ToolCatalogSchemaVersion,
			ProviderID:    host.providerID, RegistrationID: host.registrationID,
			Generation: host.generation, AcceptedSequence: host.sequence,
			CatalogRevision: "sha256:revision",
		})
		reply, _ := json.Marshal(catalogHostRuntimeEnvelope{
			Arguments: map[string]json.RawMessage{"result": result}, RequestID: request.RequestID,
		})
		return host.Handle(context.Background(), &sdk.Message{
			SourceTopic: "sv::tool-catalog::one", Payload: reply, MessageType: pb.MessageType_CHAT,
		})
	}
	if err := host.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := host.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := host.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(methods, ",") != "tool.catalog.publish,tool.catalog.renew,tool.catalog.revoke" {
		t.Fatalf("methods = %v", methods)
	}
	if len(sequences) != 3 || sequences[0] != 1 || sequences[1] != 2 || sequences[2] != 3 {
		t.Fatalf("sequences = %v", sequences)
	}
}

func testCatalogRegistry(t *testing.T, handler tools.Handler) *tools.Registry {
	t.Helper()
	registry := tools.NewRegistry()
	if err := registry.Register("vfs_search", handler); err != nil {
		t.Fatal(err)
	}
	registry.Describe(tools.Descriptor{
		Name: "vfs_search", Description: "Search workspace documents and return a VFS reference.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}}}`),
		CatalogKind: "remote", Effect: spec.ToolEffectRead,
	})
	return registry
}

func testCatalogToolHost(
	t *testing.T,
	sender *catalogHostRecordingSender,
	registry *tools.Registry,
	profile *catalog.InvocationAuthorityProfile,
) *CatalogToolHost {
	t.Helper()
	host, err := NewCatalogToolHost(CatalogToolHostConfig{
		Route: testCatalogHostRoute, ProviderID: "documents", RegistrationID: "tool-host-one",
		Generation: "generation-1", Context: spec.ToolCatalogContext{WorkspaceID: "project-a"},
		Registry: registry, Sender: sender,
		Exports: []CatalogToolExport{{
			Name: "vfs_search", Revision: "sha256:vfs-search-v1",
			Effect: spec.ToolEffectRead, InvocationAuthority: profile,
		}},
		LeaseDuration: time.Minute, RenewInterval: 20 * time.Second, RPCTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func testCatalogAuthorityProfile() catalog.InvocationAuthorityProfile {
	return catalog.InvocationAuthorityProfile{
		Mode: catalog.InvocationAuthorityModeCallerOBO,
		ResourceScope: []catalog.InvocationAuthorityResourceScope{
			{ResourceType: "vfs", Patterns: []string{"workspaces/project-a/*"}},
			{ResourceType: "memorylayer/thread", Patterns: []string{"project-a/*"}},
		},
		OperationScope: []string{"vfs.read", "memory.read"}, MaxAccessLevel: 10,
	}
}

func ptrCatalogProfile(profile catalog.InvocationAuthorityProfile) *catalog.InvocationAuthorityProfile {
	return &profile
}

func testCatalogInvocationMessage(
	t *testing.T,
	host *CatalogToolHost,
	callID string,
	forwardedProfile *catalog.InvocationAuthorityProfile,
) *sdk.Message {
	t.Helper()
	entry := host.entries[0]
	envelope := spec.NewToolInvokeEnvelope(callID, entry.Ref.Name)
	envelope.ToolRef = &entry.Ref
	envelope.Addr = spec.MessageAddress{
		WorkspaceID: "project-a", ThreadID: "thread-1", TaskID: "task-1", UserID: "alice",
	}
	envelope.Args = map[string]json.RawMessage{"query": json.RawMessage(`"contracts"`)}
	invokeRaw, _ := json.Marshal(envelope)
	payload, _ := json.Marshal(catalogHostRuntimeEnvelope{
		Source:    catalogHostRuntimeAddress{Workspace: "project-a", Agent: "sahara"},
		Arguments: map[string]json.RawMessage{catalogToolInvokeArgument: invokeRaw}, RequestID: callID,
	})
	subject := &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"}
	resourceID, err := catalog.EntryResourceID(host.catalogContext, entry.Ref)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &pb.AccessDecisionReceipt{
		Allowed: true, Decision: "ALLOW", EffectiveAccessLevel: 10,
		Request: &pb.ResourceAccessRequest{
			ResourceType: "tool-catalog/entry", ResourceId: resourceID,
			Operation: catalog.CatalogActionInvokeRead, Workspace: "project-a",
			RequiredAccessLevel: 10, CorrelationId: callID,
		},
		Actor:   &pb.PrincipalRef{PrincipalType: "agent", PrincipalId: testCatalogCallerRoute},
		Subject: subject, AuthorityMode: "on_behalf_of", GrantId: "parent-call",
		RootGrantId: "root-call", DeliveryTarget: host.route,
		ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(),
	}
	message := &sdk.Message{
		SourceTopic: testCatalogCallerRoute, Payload: payload, MessageType: pb.MessageType_TOOL_CALL,
		OnBehalfSubject: subject, AccessReceipt: receipt,
	}
	if forwardedProfile != nil {
		resources := make([]*pb.ACLAuthorityGrantResourceScopeEntry, 0, len(forwardedProfile.ResourceScope))
		for _, resource := range forwardedProfile.ResourceScope {
			resources = append(resources, &pb.ACLAuthorityGrantResourceScopeEntry{
				ResourceType: resource.ResourceType, Patterns: append([]string(nil), resource.Patterns...),
			})
		}
		message.ForwardedAuthorization = &pb.ForwardedAuthorization{
			Authorization: &pb.AuthorizationContext{
				AuthorityMode: "on_behalf_of", Subject: subject, GrantId: "child-" + callID,
			},
			RootGrantId: "root-call", ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(),
			DeliveryTarget: host.route, BindingId: callID,
			Scope: &pb.AuthorityContinuationScope{
				WorkspaceScope: []string{"project-a"}, ResourceScope: resources,
				OperationScope: append([]string(nil), forwardedProfile.OperationScope...),
				MaxAccessLevel: forwardedProfile.MaxAccessLevel,
			},
		}
	}
	return message
}

func awaitCatalogToolResult(t *testing.T, sender *catalogHostRecordingSender) spec.ToolResultPartBody {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sender.mu.Lock()
		options := append([]sdk.SendMessageOptions(nil), sender.options...)
		sender.mu.Unlock()
		for _, option := range options {
			if option.TargetTopic != testCatalogCallerRoute {
				continue
			}
			var envelope catalogHostRuntimeEnvelope
			if err := json.Unmarshal(option.Payload, &envelope); err != nil {
				t.Fatal(err)
			}
			var body spec.ToolResultPartBody
			if err := json.Unmarshal(envelope.Arguments[catalogToolResultArgument], &body); err != nil {
				t.Fatal(err)
			}
			return body
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for catalog tool result")
	return spec.ToolResultPartBody{}
}
