package turn

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/provider"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// A provider catalog is assembled before the first model call and must remain
// live across model reasoning, approval, and execution. Thirty minutes is still
// a bounded, turn-confined lease while avoiding brittle expiry during a long
// interactive approval. Externally supplied LiveServices may clamp it lower.
const turnToolCatalogLease = 30 * time.Minute

// ToolCatalogBindingFunc derives the trusted subject and policy epoch for one
// turn-confined catalog context. The returned binding must carry catalogContext
// exactly. Hosts use this seam to bind discovery and invocation to their own
// authenticated identity/ACL epoch; nil selects the OSS single-user default.
type ToolCatalogBindingFunc func(
	ctx context.Context,
	addr protocol.MessageAddress,
	user protocol.ChatMessage,
	catalogContext spec.ToolCatalogContext,
) (catalog.QueryBinding, error)

type discoveredProviderTools struct {
	provider     ToolProvider
	providerID   string
	registration string
	descriptors  []tools.Descriptor
}

type resolvedCatalogTools struct {
	specs          []provider.ToolSpec
	providerByTool map[string]ToolProvider
	trustByTool    map[string]tools.TrustLevel
	concurrency    map[string]tools.ConcurrencyClass
	refByTool      map[string]protocol.ToolReference
	binding        catalog.QueryBinding
}

// resolveProviderCatalog adapts the legacy ToolProvider discovery seam into
// the ecosystem live-catalog protocol. Publications are unique to this turn,
// queried through the catalog authorizer, and then converted back to the model
// API. Invocation later re-resolves the exact selected ref against live state.
func (r *Runner) resolveProviderCatalog(
	ctx context.Context,
	addr protocol.MessageAddress,
	user protocol.ChatMessage,
	groups []discoveredProviderTools,
) (resolvedCatalogTools, error) {
	if len(groups) == 0 {
		return resolvedCatalogTools{}, nil
	}
	if r.toolCatalog == nil || strings.TrimSpace(r.toolCatalogGen) == "" {
		return resolvedCatalogTools{}, fmt.Errorf("turn: provider tool catalog is not initialized")
	}

	turnNumber := r.toolCatalogTurn.Add(1)
	catalogContext := spec.ToolCatalogContext{
		WorkspaceID:       addr.WorkspaceID,
		SurfaceKind:       "sahara.turn",
		SurfaceInstanceID: fmt.Sprintf("%s:%d", r.toolCatalogGen, turnNumber),
	}
	if addr.WorkspaceID != "" {
		catalogContext.ThreadID = addr.ThreadID
	}
	binding, err := r.catalogQueryBinding(ctx, addr, user, catalogContext)
	if err != nil {
		return resolvedCatalogTools{}, err
	}

	now := time.Now().UTC()
	if r.now != nil {
		now = r.now().UTC()
	}
	for groupIndex := range groups {
		group := &groups[groupIndex]
		group.providerID = canonicalCatalogIdentifier(group.provider.ID(), fmt.Sprintf("provider-%d", groupIndex+1))
		group.registration = fmt.Sprintf("%s:turn:%d:provider:%d", r.toolCatalogGen, turnNumber, groupIndex+1)
		publication, err := providerPublication(*group, r.toolCatalogGen, catalogContext, now)
		if err != nil {
			return resolvedCatalogTools{}, err
		}
		_, err = r.toolCatalog.Publish(ctx, catalog.MutationBinding{
			ProviderID:      publication.ProviderID,
			RegistrationID:  publication.RegistrationID,
			Generation:      publication.Generation,
			RequiredContext: catalogContext,
		}, publication)
		if err != nil {
			return resolvedCatalogTools{}, fmt.Errorf("turn: publish provider %q tool catalog: %w", group.provider.ID(), err)
		}
	}

	resolvedByRef := make(map[string]spec.ToolCatalogEntry)
	query := spec.ToolCatalogQuery{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		Context:       catalogContext,
		Limit:         100,
	}
	for {
		page, err := r.toolCatalog.Query(ctx, binding, query)
		if err != nil {
			return resolvedCatalogTools{}, fmt.Errorf("turn: query provider tool catalog: %w", err)
		}
		for _, entry := range page.Entries {
			key := toolReferenceKey(entry.Ref)
			if _, duplicate := resolvedByRef[key]; duplicate {
				return resolvedCatalogTools{}, fmt.Errorf("turn: catalog returned duplicate exact tool reference %q", entry.Ref.Name)
			}
			resolvedByRef[key] = entry
		}
		if page.NextCursor == "" {
			break
		}
		query.Cursor = page.NextCursor
	}

	resolved := resolvedCatalogTools{
		providerByTool: make(map[string]ToolProvider),
		trustByTool:    make(map[string]tools.TrustLevel),
		concurrency:    make(map[string]tools.ConcurrencyClass),
		refByTool:      make(map[string]protocol.ToolReference),
		binding:        binding,
	}
	for _, group := range groups {
		for _, descriptor := range group.descriptors {
			ref := spec.ToolReference{
				ProviderID: group.providerID, RegistrationID: group.registration,
				Generation: r.toolCatalogGen, Name: descriptor.Name,
			}
			entry, ok := findResolvedEntry(resolvedByRef, ref)
			if !ok {
				continue // independently unauthorized for discovery
			}
			parameters, err := json.Marshal(entry.Descriptor.InputSchema)
			if err != nil {
				return resolvedCatalogTools{}, fmt.Errorf("turn: encode resolved schema for tool %q: %w", descriptor.Name, err)
			}
			resolved.specs = append(resolved.specs, provider.ToolSpec{
				Name: entry.Descriptor.Name, Description: entry.Descriptor.Description, Parameters: parameters,
			})
			resolved.providerByTool[descriptor.Name] = group.provider
			resolved.refByTool[descriptor.Name] = entry.Ref
			if descriptor.Trust != tools.TrustDefault {
				resolved.trustByTool[descriptor.Name] = descriptor.Trust
			}
			if descriptor.Concurrency != tools.ConcurrencyUnspecified {
				resolved.concurrency[descriptor.Name] = descriptor.Concurrency
			}
		}
	}
	if len(resolved.providerByTool) == 0 {
		return resolvedCatalogTools{}, nil
	}
	return resolved, nil
}

func (r *Runner) catalogQueryBinding(
	ctx context.Context,
	addr protocol.MessageAddress,
	user protocol.ChatMessage,
	catalogContext spec.ToolCatalogContext,
) (catalog.QueryBinding, error) {
	if r.toolCatalogBind != nil {
		binding, err := r.toolCatalogBind(ctx, addr, user, catalogContext)
		if err != nil {
			return catalog.QueryBinding{}, fmt.Errorf("turn: bind provider tool catalog: %w", err)
		}
		return binding, nil
	}

	authority, _ := tools.MemoryAuthorityFrom(ctx)
	subjectID := strings.TrimSpace(authority.SubjectID)
	policyEpoch := strings.TrimSpace(authority.GrantID)
	if subjectID == "" {
		subjectID = strings.TrimSpace(addr.UserID)
	}
	if subjectID == "" {
		return catalog.StandaloneQueryBinding(catalogContext), nil
	}
	if policyEpoch == "" {
		policyEpoch = "unversioned"
	}
	return catalog.QueryBinding{SubjectID: subjectID, PolicyEpoch: policyEpoch, Context: catalogContext}, nil
}

func providerPublication(group discoveredProviderTools, generation string, catalogContext spec.ToolCatalogContext, now time.Time) (spec.ToolCatalogPublication, error) {
	entries := make([]spec.ToolCatalogEntry, 0, len(group.descriptors))
	for _, descriptor := range group.descriptors {
		entry, err := catalogEntry(group.providerID, group.registration, generation, descriptor)
		if err != nil {
			return spec.ToolCatalogPublication{}, err
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Ref.Name < entries[j].Ref.Name })
	return spec.ToolCatalogPublication{
		SchemaVersion:  spec.ToolCatalogSchemaVersion,
		ProviderID:     group.providerID,
		RegistrationID: group.registration,
		Generation:     generation,
		Sequence:       1,
		LeaseExpiresAt: now.Add(turnToolCatalogLease).UTC().Format(time.RFC3339Nano),
		Context:        catalogContext,
		Capabilities:   []string{spec.ToolReferenceCapability},
		Entries:        entries,
	}, nil
}

func catalogEntry(providerID, registrationID, generation string, source tools.Descriptor) (spec.ToolCatalogEntry, error) {
	name := strings.TrimSpace(source.Name)
	if name == "" {
		return spec.ToolCatalogEntry{}, fmt.Errorf("turn: provider tool name is required")
	}
	description := source.Description
	if strings.TrimSpace(description) == "" {
		description = "Invoke " + name
	}
	kind := strings.TrimSpace(source.CatalogKind)
	if kind == "" {
		kind = "remote"
	}
	effect := source.Effect
	if effect == "" {
		effect = spec.ToolEffectExecute
	}
	inputSchema := make(map[string]json.RawMessage)
	if len(source.Parameters) == 0 {
		inputSchema["type"] = json.RawMessage(`"object"`)
	} else if err := json.Unmarshal(source.Parameters, &inputSchema); err != nil {
		return spec.ToolCatalogEntry{}, fmt.Errorf("turn: decode input schema for provider tool %q: %w", name, err)
	}
	if inputSchema == nil {
		inputSchema = map[string]json.RawMessage{"type": json.RawMessage(`"object"`)}
	}
	descriptor := spec.ToolDescriptor{
		Name: name, Description: description, InputSchema: inputSchema,
		Kind: kind, AwaitsResult: true, Meta: cloneCatalogMeta(source.CatalogMeta),
	}
	revision, err := catalogEntryRevision(descriptor, effect)
	if err != nil {
		return spec.ToolCatalogEntry{}, err
	}
	entry := spec.ToolCatalogEntry{
		Ref: spec.ToolReference{
			ProviderID: providerID, RegistrationID: registrationID, Generation: generation,
			Name: name, Revision: revision,
		},
		Descriptor: descriptor,
		Effect:     effect,
		Provenance: &spec.ToolCatalogProvenance{Source: "sahara.tool-provider", Host: providerID},
	}
	if err := entry.Validate(); err != nil {
		return spec.ToolCatalogEntry{}, fmt.Errorf("turn: invalid provider tool %q: %w", name, err)
	}
	return entry, nil
}

func cloneCatalogMeta(source map[string]json.RawMessage) map[string]json.RawMessage {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		out[key] = append(json.RawMessage(nil), value...)
	}
	return out
}

func catalogEntryRevision(descriptor spec.ToolDescriptor, effect spec.ToolEffect) (string, error) {
	canonical, err := json.Marshal(struct {
		Descriptor spec.ToolDescriptor `json:"descriptor"`
		Effect     spec.ToolEffect     `json:"effect"`
	}{descriptor, effect})
	if err != nil {
		return "", fmt.Errorf("turn: encode provider tool revision: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func canonicalCatalogIdentifier(value, fallback string) string {
	if value != "" && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00') && value != "!" {
		return value
	}
	digest := sha256.Sum256([]byte(value))
	return fallback + "-" + hex.EncodeToString(digest[:8])
}

func findResolvedEntry(entries map[string]spec.ToolCatalogEntry, partial spec.ToolReference) (spec.ToolCatalogEntry, bool) {
	for _, entry := range entries {
		if entry.Ref.ProviderID == partial.ProviderID &&
			entry.Ref.RegistrationID == partial.RegistrationID &&
			entry.Ref.Generation == partial.Generation &&
			entry.Ref.Name == partial.Name {
			return entry, true
		}
	}
	return spec.ToolCatalogEntry{}, false
}

func toolReferenceKey(ref spec.ToolReference) string {
	return strings.Join([]string{ref.ProviderID, ref.RegistrationID, ref.Generation, ref.Name, ref.Revision}, "\x00")
}

func toolReferencesEqual(left, right spec.ToolReference) bool {
	return toolReferenceKey(left) == toolReferenceKey(right)
}

// bindCatalogToolCall attaches the exact reference selected during discovery.
// A caller-supplied ref must match exactly; provider tools never fall back to a
// bare name, and static tools cannot smuggle a provider-qualified reference.
func bindCatalogToolCall(call protocol.ToolInvokeEnvelope, tt turnTools) (protocol.ToolInvokeEnvelope, error) {
	expected, isCatalogTool := tt.refByTool[call.Name]
	_, isProviderTool := tt.providerByTool[call.Name]
	if !isCatalogTool {
		if isProviderTool {
			return protocol.ToolInvokeEnvelope{}, fmt.Errorf("turn: provider tool %q has no admitted catalog reference", call.Name)
		}
		if call.ToolRef != nil {
			return protocol.ToolInvokeEnvelope{}, fmt.Errorf("turn: static tool %q cannot use a catalog reference", call.Name)
		}
		return call, nil
	}
	if call.ToolRef != nil && !toolReferencesEqual(*call.ToolRef, expected) {
		return protocol.ToolInvokeEnvelope{}, fmt.Errorf("turn: tool %q catalog reference does not match the admitted entry", call.Name)
	}
	bound := expected
	call.ToolRef = &bound
	return call, nil
}

func (r *Runner) resolveCatalogInvocation(ctx context.Context, call protocol.ToolInvokeEnvelope, tt turnTools) (spec.ToolCatalogEntry, error) {
	if call.ToolRef == nil || r.toolCatalog == nil {
		return spec.ToolCatalogEntry{}, fmt.Errorf("turn: provider tool %q has no live catalog reference", call.Name)
	}
	entry, err := r.toolCatalog.ResolveInvocation(ctx, tt.catalogBinding, *call.ToolRef)
	if err != nil {
		return spec.ToolCatalogEntry{}, fmt.Errorf("turn: resolve provider tool %q: %w", call.Name, err)
	}
	if entry.Ref.Name != call.Name {
		return spec.ToolCatalogEntry{}, fmt.Errorf("turn: resolved catalog entry name %q does not match invocation %q", entry.Ref.Name, call.Name)
	}
	return entry, nil
}
