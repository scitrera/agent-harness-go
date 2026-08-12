// Package catalogrpc exposes the Go live catalog through a small private
// service boundary. Its payloads compose the portable ecosystem catalog types;
// trusted mutation bindings and caller identity remain transport-owned and are
// deliberately not additions to the ecosystem messaging specification.
package catalogrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	MethodPublish  = "tool.catalog.publish"
	MethodRenew    = "tool.catalog.renew"
	MethodRevoke   = "tool.catalog.revoke"
	MethodQuery    = "tool.catalog.query"
	MethodDescribe = "tool.catalog.describe"
)

// Caller is gateway-authenticated transport state. SourceTopic and SubjectID
// must never be populated from the JSON request envelope.
type Caller struct {
	SourceTopic            string
	SubjectID              string
	ForwardedAuthorization *pb.ForwardedAuthorization
}

// MutationBinding is the private JSON form of catalog.MutationBinding. The
// authenticated edge supplies it only after binding a provider publication to
// the actual connection route and checked mutation receipt.
type MutationBinding struct {
	ProviderID            string                  `json:"provider_id"`
	RegistrationID        string                  `json:"registration_id"`
	Generation            string                  `json:"generation"`
	RequiredContext       spec.ToolCatalogContext `json:"required_context"`
	GenerationReplacement bool                    `json:"generation_replacement,omitempty"`
}

func (b MutationBinding) catalogBinding() catalog.MutationBinding {
	return catalog.MutationBinding{
		ProviderID: b.ProviderID, RegistrationID: b.RegistrationID,
		Generation: b.Generation, RequiredContext: b.RequiredContext,
		GenerationReplacement: b.GenerationReplacement,
	}
}

type PublishRequest struct {
	Binding     MutationBinding             `json:"binding"`
	Publication spec.ToolCatalogPublication `json:"publication"`
}

type RenewRequest struct {
	Binding MutationBinding              `json:"binding"`
	Request spec.ToolCatalogRenewRequest `json:"request"`
}

type RevokeRequest struct {
	Binding MutationBinding               `json:"binding"`
	Request spec.ToolCatalogRevokeRequest `json:"request"`
}

// WorkspaceResolver supplies one workspace-partitioned service. A production
// adapter normally constructs catalog.AetherLiveBackend instances lazily;
// tests and standalone adapters can return memory-backed services.
type WorkspaceResolver interface {
	ResolveCatalogWorkspace(ctx context.Context, workspace string) (*catalog.LiveService, error)
}

type WorkspaceResolverFunc func(context.Context, string) (*catalog.LiveService, error)

func (f WorkspaceResolverFunc) ResolveCatalogWorkspace(ctx context.Context, workspace string) (*catalog.LiveService, error) {
	return f(ctx, workspace)
}

type ServiceOptions struct {
	MutationSourcePrefixes []string
	PolicyEpoch            string
}

// Service validates the private edge binding before delegating portable
// mutations and queries to the workspace's Go LiveService.
type Service struct {
	resolver               WorkspaceResolver
	mutationSourcePrefixes []string
	policyEpoch            string
}

func NewService(resolver WorkspaceResolver, options ServiceOptions) (*Service, error) {
	if resolver == nil {
		return nil, fmt.Errorf("catalogrpc: workspace resolver is required")
	}
	prefixes := make([]string, 0, len(options.MutationSourcePrefixes))
	for _, prefix := range options.MutationSourcePrefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" || strings.ContainsRune(prefix, '\x00') {
			return nil, fmt.Errorf("catalogrpc: mutation source prefix must be canonical")
		}
		prefixes = append(prefixes, prefix)
	}
	if len(prefixes) == 0 {
		return nil, fmt.Errorf("catalogrpc: at least one trusted mutation source prefix is required")
	}
	policyEpoch := strings.TrimSpace(options.PolicyEpoch)
	if policyEpoch == "" || strings.ContainsRune(policyEpoch, '\x00') {
		return nil, fmt.Errorf("catalogrpc: policy epoch is required")
	}
	return &Service{resolver: resolver, mutationSourcePrefixes: prefixes, policyEpoch: policyEpoch}, nil
}

// HandleJSON dispatches exactly one private method and returns its JSON result.
// Known catalog failures retain their stable ecosystem error projection.
func (s *Service) HandleJSON(ctx context.Context, caller Caller, method string, payload json.RawMessage) (json.RawMessage, error) {
	var result any
	var err error
	switch method {
	case MethodPublish:
		result, err = s.publish(ctx, caller, payload)
	case MethodRenew:
		result, err = s.renew(ctx, caller, payload)
	case MethodRevoke:
		result, err = s.revoke(ctx, caller, payload)
	case MethodQuery:
		result, err = s.query(ctx, caller, payload)
	case MethodDescribe:
		result, err = s.describe(ctx, caller, payload)
	default:
		return nil, fmt.Errorf("catalogrpc: unknown method %q", method)
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("catalogrpc: encode %s result: %w", method, err)
	}
	return encoded, nil
}

func (s *Service) publish(ctx context.Context, caller Caller, payload json.RawMessage) (spec.ToolCatalogMutationResult, error) {
	if err := s.authorizeMutationCaller(caller); err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	var request PublishRequest
	if err := decodeRequest(payload, &request); err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	service, err := s.workspace(ctx, request.Publication.Context.WorkspaceID)
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return service.Publish(ctx, request.Binding.catalogBinding(), request.Publication)
}

func (s *Service) renew(ctx context.Context, caller Caller, payload json.RawMessage) (spec.ToolCatalogMutationResult, error) {
	if err := s.authorizeMutationCaller(caller); err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	var request RenewRequest
	if err := decodeRequest(payload, &request); err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	service, err := s.workspace(ctx, request.Binding.RequiredContext.WorkspaceID)
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return service.Renew(ctx, request.Binding.catalogBinding(), request.Request)
}

func (s *Service) revoke(ctx context.Context, caller Caller, payload json.RawMessage) (spec.ToolCatalogMutationResult, error) {
	if err := s.authorizeMutationCaller(caller); err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	var request RevokeRequest
	if err := decodeRequest(payload, &request); err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	service, err := s.workspace(ctx, request.Binding.RequiredContext.WorkspaceID)
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return service.Revoke(ctx, request.Binding.catalogBinding(), request.Request)
}

func (s *Service) query(ctx context.Context, caller Caller, payload json.RawMessage) (catalog.ResolvedCatalogPage, error) {
	if err := requireQueryAuthority(caller); err != nil {
		return catalog.ResolvedCatalogPage{}, err
	}
	var query spec.ToolCatalogQuery
	if err := decodeRequest(payload, &query); err != nil {
		return catalog.ResolvedCatalogPage{}, err
	}
	service, err := s.workspace(ctx, query.Context.WorkspaceID)
	if err != nil {
		return catalog.ResolvedCatalogPage{}, err
	}
	binding := catalog.QueryBinding{
		SubjectID: caller.SubjectID, PolicyEpoch: s.policyEpoch,
		AuthorityLineageID: caller.ForwardedAuthorization.GetRootGrantId(), Context: query.Context,
	}
	ctx = context.WithValue(ctx, forwardedAuthorizationContextKey{}, caller.ForwardedAuthorization)
	return service.QueryResolved(ctx, binding, query)
}

func (s *Service) describe(ctx context.Context, caller Caller, payload json.RawMessage) (catalog.ResolvedCatalogDescribeResult, error) {
	if err := requireQueryAuthority(caller); err != nil {
		return catalog.ResolvedCatalogDescribeResult{}, err
	}
	var request spec.ToolCatalogDescribeRequest
	if err := decodeRequest(payload, &request); err != nil {
		return catalog.ResolvedCatalogDescribeResult{}, err
	}
	service, err := s.workspace(ctx, request.Context.WorkspaceID)
	if err != nil {
		return catalog.ResolvedCatalogDescribeResult{}, err
	}
	binding := catalog.QueryBinding{
		SubjectID: caller.SubjectID, PolicyEpoch: s.policyEpoch,
		AuthorityLineageID: caller.ForwardedAuthorization.GetRootGrantId(), Context: request.Context,
	}
	ctx = context.WithValue(ctx, forwardedAuthorizationContextKey{}, caller.ForwardedAuthorization)
	return service.DescribeResolved(ctx, binding, request)
}

func (s *Service) workspace(ctx context.Context, workspace string) (*catalog.LiveService, error) {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" || strings.ContainsRune(workspace, '\x00') {
		return nil, fmt.Errorf("catalogrpc: workspace is required")
	}
	service, err := s.resolver.ResolveCatalogWorkspace(ctx, workspace)
	if err != nil {
		return nil, fmt.Errorf("catalogrpc: resolve workspace %q: %w", workspace, err)
	}
	if service == nil {
		return nil, fmt.Errorf("catalogrpc: resolver returned no service for workspace %q", workspace)
	}
	return service, nil
}

func (s *Service) authorizeMutationCaller(caller Caller) error {
	for _, prefix := range s.mutationSourcePrefixes {
		if strings.HasPrefix(caller.SourceTopic, prefix) {
			return nil
		}
	}
	return fmt.Errorf("catalogrpc: mutation caller %q is not a trusted catalog edge", caller.SourceTopic)
}

func requireSubject(caller Caller) error {
	if strings.TrimSpace(caller.SubjectID) == "" || strings.TrimSpace(caller.SubjectID) != caller.SubjectID || strings.ContainsRune(caller.SubjectID, '\x00') {
		return fmt.Errorf("catalogrpc: authenticated OBO subject is required")
	}
	return nil
}

type forwardedAuthorizationContextKey struct{}

func requireQueryAuthority(caller Caller) error {
	if err := requireSubject(caller); err != nil {
		return err
	}
	forwarded := caller.ForwardedAuthorization
	if forwarded == nil || forwarded.GetAuthorization() == nil ||
		strings.TrimSpace(forwarded.GetRootGrantId()) == "" ||
		strings.TrimSpace(forwarded.GetAuthorization().GetGrantId()) == "" {
		return fmt.Errorf("catalogrpc: gateway-forwarded authorization is required")
	}
	authorization := forwarded.GetAuthorization()
	if authorization.GetAuthorityMode() != "on_behalf_of" || authorization.GetSubject() == nil ||
		!strings.EqualFold(authorization.GetSubject().GetPrincipalType(), "user") ||
		authorization.GetSubject().GetPrincipalId() != caller.SubjectID {
		return fmt.Errorf("catalogrpc: forwarded authorization subject does not match authenticated caller")
	}
	return nil
}

func decodeRequest(payload json.RawMessage, target any) error {
	if len(payload) == 0 {
		return fmt.Errorf("catalogrpc: request payload is required")
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("catalogrpc: decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("catalogrpc: decode request: multiple JSON values")
		}
		return fmt.Errorf("catalogrpc: decode request: %w", err)
	}
	return nil
}
