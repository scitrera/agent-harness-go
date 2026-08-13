package catalogrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// messageSender is the small SDK seam needed by AetherHandler.
type messageSender interface {
	SendWithOptions(sdk.SendMessageOptions) error
}

// AetherHandler adapts the existing runtime MessageEnvelope dialect used by
// Platform Bridge and Sahara to the private catalog service. Aether remains
// transport: the portable catalog payloads and Go LiveService own semantics.
type AetherHandler struct {
	service        *Service
	sender         messageSender
	implementation string
	serviceTopic   string
}

func NewAetherHandler(service *Service, sender messageSender, serviceTopic string) (*AetherHandler, error) {
	if service == nil || sender == nil {
		return nil, fmt.Errorf("catalogrpc: service and Aether sender are required")
	}
	serviceTopic = strings.TrimSpace(serviceTopic)
	parts := strings.Split(serviceTopic, "::")
	if len(parts) != 3 || parts[0] != "sv" || parts[1] == "" || parts[2] == "" {
		return nil, fmt.Errorf("catalogrpc: exact Aether service topic is required")
	}
	return &AetherHandler{service: service, sender: sender, implementation: parts[1], serviceTopic: serviceTopic}, nil
}

// Handle processes one gateway-authenticated Aether message. Register it via
// sdk.AsyncMessageHandler because catalog operations use synchronous KV calls.
func (h *AetherHandler) Handle(ctx context.Context, message *sdk.Message) error {
	if message == nil || strings.TrimSpace(message.SourceTopic) == "" {
		return fmt.Errorf("catalogrpc: authenticated source topic is required")
	}
	var request runtimeEnvelope
	if err := json.Unmarshal(message.Payload, &request); err != nil {
		return fmt.Errorf("catalogrpc: decode runtime envelope: %w", err)
	}
	method, payload, err := catalogMethod(request.Arguments)
	if err != nil {
		return h.reply(message.SourceTopic, request, nil, err)
	}
	caller := Caller{
		SourceTopic: message.SourceTopic, AccessReceipt: message.AccessReceipt,
		DeliveryTarget: h.serviceTopic,
	}
	if message.OnBehalfSubject != nil && message.OnBehalfSubject.GetPrincipalType() == "user" {
		caller.SubjectID = message.OnBehalfSubject.GetPrincipalId()
	}
	if message.ForwardedAuthorization != nil {
		if err := h.validateForwardedAuthorization(message); err != nil {
			return h.reply(message.SourceTopic, request, nil, err)
		}
		caller.ForwardedAuthorization = message.ForwardedAuthorization
	}
	result, callErr := h.service.HandleJSON(ctx, caller, method, payload)
	return h.reply(message.SourceTopic, request, result, callErr)
}

func (h *AetherHandler) validateForwardedAuthorization(message *sdk.Message) error {
	forwarded := message.ForwardedAuthorization
	if forwarded.GetDeliveryTarget() != h.serviceTopic {
		return fmt.Errorf("catalogrpc: forwarded authorization is bound to another service target")
	}
	if forwarded.GetExpiresAtMs() <= time.Now().UnixMilli() {
		return fmt.Errorf("catalogrpc: forwarded authorization is expired")
	}
	if strings.TrimSpace(forwarded.GetRootGrantId()) == "" {
		return fmt.Errorf("catalogrpc: forwarded authorization root grant is required")
	}
	authorization := forwarded.GetAuthorization()
	if authorization == nil || authorization.GetAuthorityMode() != "on_behalf_of" ||
		authorization.GetSubject() == nil || strings.TrimSpace(authorization.GetGrantId()) == "" {
		return fmt.Errorf("catalogrpc: forwarded authorization context is invalid")
	}
	if message.OnBehalfSubject == nil ||
		!strings.EqualFold(message.OnBehalfSubject.GetPrincipalType(), authorization.GetSubject().GetPrincipalType()) ||
		message.OnBehalfSubject.GetPrincipalId() != authorization.GetSubject().GetPrincipalId() {
		return fmt.Errorf("catalogrpc: forwarded authorization subject does not match gateway subject")
	}
	return nil
}

func (h *AetherHandler) reply(targetTopic string, request runtimeEnvelope, result json.RawMessage, callErr error) error {
	arguments := make(map[string]json.RawMessage, 1)
	if callErr != nil {
		errorValue := errorProjection(callErr)
		encoded, err := json.Marshal(errorValue)
		if err != nil {
			return fmt.Errorf("catalogrpc: encode error response: %w", err)
		}
		arguments["error"] = encoded
	} else {
		arguments["result"] = result
	}
	response := runtimeEnvelope{
		Source: runtimeAddress{
			Tenant: request.Target.Tenant, Workspace: "",
			Agent: h.implementation, Key: h.implementation,
		},
		Target:    request.Source,
		Content:   runtimeContent{Role: "assistant", End: true},
		Arguments: arguments,
		RequestID: request.RequestID,
		Initiator: request.Initiator,
	}
	if response.RequestID == "" {
		response.RequestID = request.Target.RequestID
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("catalogrpc: encode runtime reply: %w", err)
	}
	return h.sender.SendWithOptions(sdk.SendMessageOptions{
		TargetTopic: targetTopic, Payload: encoded, MessageType: sdk.MessageTypeChat,
	})
}

func catalogMethod(arguments map[string]json.RawMessage) (string, json.RawMessage, error) {
	var selected string
	var payload json.RawMessage
	for _, method := range []string{MethodPublish, MethodRenew, MethodRevoke, MethodQuery, MethodDescribe} {
		if value, ok := arguments[method]; ok {
			if selected != "" {
				return "", nil, fmt.Errorf("catalogrpc: exactly one catalog method is required")
			}
			selected, payload = method, value
		}
	}
	if selected == "" {
		return "", nil, fmt.Errorf("catalogrpc: no catalog method in runtime envelope")
	}
	if len(arguments) != 1 {
		return "", nil, fmt.Errorf("catalogrpc: runtime envelope contains non-catalog arguments")
	}
	return selected, payload, nil
}

func errorProjection(err error) any {
	if protocol, ok := catalog.AsToolCatalogError(err); ok {
		return protocol
	}
	return spec.ToolCatalogError{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		Code:          "service_error", Message: err.Error(), RestartRequired: false,
	}
}

type runtimeEnvelope struct {
	Source    runtimeAddress             `json:"source"`
	Content   runtimeContent             `json:"content"`
	Arguments map[string]json.RawMessage `json:"arguments,omitempty"`
	Target    runtimeAddress             `json:"target,omitempty"`
	RequestID string                     `json:"request_id,omitempty"`
	Initiator *runtimeAddress            `json:"initiator,omitempty"`
}

type runtimeAddress struct {
	Tenant    string `json:"tenant,omitempty"`
	Workspace string `json:"workspace"`
	User      string `json:"user,omitempty"`
	Agent     string `json:"agent,omitempty"`
	Key       string `json:"key,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type runtimeContent struct {
	Text string `json:"text,omitempty"`
	End  bool   `json:"end"`
	Role string `json:"role"`
}
