package catalog

import (
	"context"
	"fmt"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const InvocationAuthorityModeCallerOBO = "caller_obo"

// InvocationAuthorityResourceScope is the catalog-private projection of one
// Aether authority-grant resource family. It deliberately does not live in the
// portable tool descriptor or invocation envelope.
type InvocationAuthorityResourceScope struct {
	ResourceType string   `json:"resource_type"`
	Patterns     []string `json:"patterns"`
}

// InvocationAuthorityProfile is a server-owned maximum downstream authority
// for one exact catalog entry. Workspace scope is always the exact trusted
// ToolCatalogContext workspace and is therefore not provider-configurable.
type InvocationAuthorityProfile struct {
	Mode           string                             `json:"mode"`
	ResourceScope  []InvocationAuthorityResourceScope `json:"resource_scope"`
	OperationScope []string                           `json:"operation_scope"`
	MaxAccessLevel int32                              `json:"max_access_level"`
}

func (p InvocationAuthorityProfile) Validate() error {
	if p.Mode != InvocationAuthorityModeCallerOBO {
		return fmt.Errorf("catalog: unsupported invocation authority mode %q", p.Mode)
	}
	if len(p.ResourceScope) == 0 || len(p.OperationScope) == 0 {
		return fmt.Errorf("catalog: caller OBO profile requires resource and operation scopes")
	}
	switch p.MaxAccessLevel {
	case 10, 20, 30, 40, 50:
	default:
		return fmt.Errorf("catalog: caller OBO profile has invalid max access level %d", p.MaxAccessLevel)
	}
	resourceTypes := make(map[string]struct{}, len(p.ResourceScope))
	for _, resource := range p.ResourceScope {
		if err := validateAuthorityProfileValue("resource type", resource.ResourceType); err != nil {
			return err
		}
		if _, exists := resourceTypes[resource.ResourceType]; exists {
			return fmt.Errorf("catalog: caller OBO profile repeats resource type %q", resource.ResourceType)
		}
		resourceTypes[resource.ResourceType] = struct{}{}
		if err := validateAuthorityProfileValues("resource pattern", resource.Patterns); err != nil {
			return err
		}
	}
	return validateAuthorityProfileValues("operation", p.OperationScope)
}

func validateAuthorityProfileValues(label string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("catalog: caller OBO profile requires at least one %s", label)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateAuthorityProfileValue(label, value); err != nil {
			return err
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("catalog: caller OBO profile repeats %s %q", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateAuthorityProfileValue(label, value string) error {
	if value == "" || value != strings.TrimSpace(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("catalog: caller OBO profile has invalid %s", label)
	}
	return nil
}

// InvocationAuthorityResolver supplies trusted deployment policy for an exact
// live entry. nil means no forwarded caller authority.
type InvocationAuthorityResolver interface {
	ResolveInvocationAuthority(context.Context, spec.ToolCatalogContext, spec.ToolCatalogEntry) (*InvocationAuthorityProfile, error)
}

type InvocationAuthorityResolverFunc func(context.Context, spec.ToolCatalogContext, spec.ToolCatalogEntry) (*InvocationAuthorityProfile, error)

func (f InvocationAuthorityResolverFunc) ResolveInvocationAuthority(ctx context.Context, catalogContext spec.ToolCatalogContext, entry spec.ToolCatalogEntry) (*InvocationAuthorityProfile, error) {
	return f(ctx, catalogContext, entry)
}

// InvocationAuthorityRule binds a profile to one exact immutable tool
// reference. Revisions or generations changing require an explicit policy
// update rather than silently carrying authority onto new provider code.
type InvocationAuthorityRule struct {
	Ref     spec.ToolReference         `json:"ref"`
	Profile InvocationAuthorityProfile `json:"profile"`
}

type StaticInvocationAuthorityPolicy struct {
	profiles map[string]InvocationAuthorityProfile
}

func NewStaticInvocationAuthorityPolicy(rules []InvocationAuthorityRule) (*StaticInvocationAuthorityPolicy, error) {
	policy := &StaticInvocationAuthorityPolicy{profiles: make(map[string]InvocationAuthorityProfile, len(rules))}
	for _, rule := range rules {
		if err := rule.Ref.Validate(); err != nil {
			return nil, fmt.Errorf("catalog: invalid invocation authority reference: %w", err)
		}
		if err := rule.Profile.Validate(); err != nil {
			return nil, err
		}
		key := exactReferenceKey(rule.Ref)
		if _, exists := policy.profiles[key]; exists {
			return nil, fmt.Errorf("catalog: duplicate invocation authority rule for %s/%s", rule.Ref.ProviderID, rule.Ref.Name)
		}
		policy.profiles[key] = cloneInvocationAuthorityProfile(rule.Profile)
	}
	return policy, nil
}

func (p *StaticInvocationAuthorityPolicy) ResolveInvocationAuthority(_ context.Context, _ spec.ToolCatalogContext, entry spec.ToolCatalogEntry) (*InvocationAuthorityProfile, error) {
	if p == nil {
		return nil, nil
	}
	profile, ok := p.profiles[exactReferenceKey(entry.Ref)]
	if !ok {
		return nil, nil
	}
	cloned := cloneInvocationAuthorityProfile(profile)
	return &cloned, nil
}

func cloneInvocationAuthorityProfile(profile InvocationAuthorityProfile) InvocationAuthorityProfile {
	out := profile
	out.OperationScope = append([]string(nil), profile.OperationScope...)
	out.ResourceScope = make([]InvocationAuthorityResourceScope, len(profile.ResourceScope))
	for i, resource := range profile.ResourceScope {
		out.ResourceScope[i] = InvocationAuthorityResourceScope{
			ResourceType: resource.ResourceType,
			Patterns:     append([]string(nil), resource.Patterns...),
		}
	}
	return out
}

func exactReferenceKey(ref spec.ToolReference) string {
	return strings.Join([]string{ref.ProviderID, ref.RegistrationID, ref.Generation, ref.Name, ref.Revision}, "\x00")
}
