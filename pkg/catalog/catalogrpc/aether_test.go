package catalogrpc

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

func TestAetherHandlerUsesGatewayIdentityAndCorrelatesRuntimeReply(t *testing.T) {
	service, err := NewService(newTestResolver(t), ServiceOptions{
		MutationSourcePrefixes: []string{"sv::platform-bridge::"}, PolicyEpoch: "policy-1",
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	sender := &recordingSender{}
	handler, err := NewAetherHandler(service, sender, "tool-catalog")
	if err != nil {
		t.Fatalf("NewAetherHandler: %v", err)
	}
	query := spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		Context:       spec.ToolCatalogContext{WorkspaceID: "project-a"}, Limit: 10,
	}
	request := runtimeEnvelope{
		Source:    runtimeAddress{Tenant: "tenant-a", Workspace: "", Agent: "agent-harness", Key: "agent-harness"},
		Target:    runtimeAddress{Tenant: "tenant-a", Workspace: "", Agent: "tool-catalog", Key: "tool-catalog"},
		Content:   runtimeContent{Role: "user", End: true},
		Arguments: map[string]json.RawMessage{MethodQuery: mustJSON(t, query)},
		RequestID: "request-1",
	}
	if err := handler.Handle(context.Background(), &sdk.Message{
		SourceTopic: "sv::agent-harness::worker-1", Payload: mustJSON(t, request),
		OnBehalfSubject: &pb.PrincipalRef{PrincipalType: "user", PrincipalId: "alice"},
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if sender.options.TargetTopic != "sv::agent-harness::worker-1" || sender.options.MessageType != sdk.MessageTypeChat {
		t.Fatalf("send options = %+v", sender.options)
	}
	var response runtimeEnvelope
	if err := json.Unmarshal(sender.options.Payload, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.RequestID != "request-1" || response.Target.Agent != "agent-harness" || response.Source.Agent != "tool-catalog" {
		t.Fatalf("response routing = %+v", response)
	}
	if _, ok := response.Arguments["result"]; !ok {
		t.Fatalf("response arguments = %s", sender.options.Payload)
	}
}

type recordingSender struct {
	options sdk.SendMessageOptions
}

func (s *recordingSender) SendWithOptions(options sdk.SendMessageOptions) error {
	s.options = options
	return nil
}
