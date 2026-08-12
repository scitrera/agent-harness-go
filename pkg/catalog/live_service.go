package catalog

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	// Canonical catalog resource operations used by EntryAuthorizer.
	CatalogActionDiscover          = "tool.discover"
	CatalogActionDescribe          = "tool.describe"
	CatalogActionInvokeRead        = "tool.invoke.read"
	CatalogActionInvokeWrite       = "tool.invoke.write"
	CatalogActionInvokeExecute     = "tool.invoke.execute"
	CatalogActionInvokeExternal    = "tool.invoke.external"
	CatalogActionInvokeInteraction = "tool.invoke.interaction"

	CatalogErrorStaleGeneration  = "stale_generation"
	CatalogErrorStaleSequence    = "stale_sequence"
	CatalogErrorLeaseExpired     = "lease_expired"
	CatalogErrorStaleCursor      = "stale_cursor"
	CatalogErrorStaleSnapshot    = "stale_snapshot"
	CatalogErrorAmbiguousRef     = "ambiguous_reference"
	CatalogErrorUnauthorized     = "unauthorized"
	CatalogErrorInvalidRequest   = "invalid_request"
	CatalogErrorConcurrentUpdate = "concurrent_update"

	cursorVersion = "1"
)

// MutationBinding is trusted transport state for one provider registration.
// RequiredContext is an authenticated confinement boundary: every non-empty
// field must be present and equal in a publication. GenerationReplacement is
// set only for a trusted reconnect/registration transition.
type MutationBinding struct {
	ProviderID            string
	RegistrationID        string
	Generation            string
	RequiredContext       spec.ToolCatalogContext
	GenerationReplacement bool
}

// QueryBinding is trusted edge state. SubjectID and PolicyEpoch are never
// copied from catalog payloads; they bind retained snapshots and cursors to the
// authenticated caller and current authorization epoch.
type QueryBinding struct {
	SubjectID   string
	PolicyEpoch string
	Context     spec.ToolCatalogContext
}

// ResolvedCatalogEntry retains the authenticated publication context beside
// an ecosystem catalog entry. The portable query page intentionally carries
// only entries; service adapters need this backend-owned context to route an
// exact invocation without copying provider routes into model-owned arguments.
type ResolvedCatalogEntry struct {
	Entry   spec.ToolCatalogEntry   `json:"entry"`
	Context spec.ToolCatalogContext `json:"context"`
}

// ResolvedCatalogPage is the service-private projection of one deterministic
// catalog page. AmbiguousNames is computed over the complete retained snapshot,
// not merely this page, so a same-name collision that straddles a cursor
// boundary can never be exposed as an apparently unique bare tool name.
type ResolvedCatalogPage struct {
	SchemaVersion   string                 `json:"schema_version"`
	SnapshotID      string                 `json:"snapshot_id"`
	CatalogRevision string                 `json:"catalog_revision"`
	Records         []ResolvedCatalogEntry `json:"records"`
	AmbiguousNames  []string               `json:"ambiguous_names,omitempty"`
	NextCursor      string                 `json:"next_cursor,omitempty"`
}

// StandaloneQueryBinding supplies explicit single-user authority for the
// dependency-free runtime while retaining the same snapshot/cursor boundary.
func StandaloneQueryBinding(c spec.ToolCatalogContext) QueryBinding {
	return QueryBinding{SubjectID: "standalone", PolicyEpoch: "standalone", Context: c}
}

// EntryAuthorizer independently decides discovery and describe access for one
// exact provider-qualified entry. Enterprise adapters should delegate this to
// Aether decisions; standalone mode uses AllowAllEntryAuthorizer explicitly.
type EntryAuthorizer interface {
	AuthorizeCatalogEntry(ctx context.Context, action string, binding QueryBinding, entry spec.ToolCatalogEntry) (bool, error)
}

// EntryAuthorizerFunc adapts a function to EntryAuthorizer.
type EntryAuthorizerFunc func(context.Context, string, QueryBinding, spec.ToolCatalogEntry) (bool, error)

func (f EntryAuthorizerFunc) AuthorizeCatalogEntry(ctx context.Context, action string, binding QueryBinding, entry spec.ToolCatalogEntry) (bool, error) {
	return f(ctx, action, binding, entry)
}

// AllowAllEntryAuthorizer is intended for the explicitly single-user
// standalone runtime. Distributed/enterprise composition should supply an
// Aether-backed authorizer instead.
type AllowAllEntryAuthorizer struct{}

func (AllowAllEntryAuthorizer) AuthorizeCatalogEntry(context.Context, string, QueryBinding, spec.ToolCatalogEntry) (bool, error) {
	return true, nil
}

var denyAllEntryAuthorizer EntryAuthorizer = EntryAuthorizerFunc(
	func(context.Context, string, QueryBinding, spec.ToolCatalogEntry) (bool, error) { return false, nil },
)

// PublicationLiveness lets an adapter add a trusted route/presence check on
// top of the protocol lease. nil means that the backend's live publication is
// routable, which is the correct standalone behavior.
type PublicationLiveness interface {
	CatalogPublicationRoutable(ctx context.Context, publication spec.ToolCatalogPublication) (bool, error)
}

// PublicationLivenessFunc adapts a function to PublicationLiveness.
type PublicationLivenessFunc func(context.Context, spec.ToolCatalogPublication) (bool, error)

func (f PublicationLivenessFunc) CatalogPublicationRoutable(ctx context.Context, publication spec.ToolCatalogPublication) (bool, error) {
	return f(ctx, publication)
}

// LiveServiceOptions controls bounded leases, replay protection, retained
// snapshots, and cursor integrity. CursorKey should be stable across replicas;
// when omitted a process-local random key is generated (appropriate for
// standalone mode only).
type LiveServiceOptions struct {
	Authorizer         EntryAuthorizer
	Liveness           PublicationLiveness
	CursorKey          []byte
	MaxLease           time.Duration
	TombstoneRetention time.Duration
	SnapshotRetention  time.Duration
	MaxCASRetries      int
	Now                func() time.Time
}

// LiveService implements the ecosystem tool-catalog publication, lease,
// deterministic query/cursor, and exact describe semantics over LiveBackend.
type LiveService struct {
	backend            LiveBackend
	authorizer         EntryAuthorizer
	liveness           PublicationLiveness
	cursorKey          []byte
	maxLease           time.Duration
	tombstoneRetention time.Duration
	snapshotRetention  time.Duration
	maxCASRetries      int
	now                func() time.Time
}

// CatalogError projects a stable ecosystem catalog error while remaining a Go
// error suitable for errors.As.
type CatalogError struct {
	Protocol spec.ToolCatalogError
}

func (e *CatalogError) Error() string {
	if e == nil {
		return "catalog: unknown error"
	}
	if e.Protocol.Message == "" {
		return "catalog: " + e.Protocol.Code
	}
	return "catalog: " + e.Protocol.Code + ": " + e.Protocol.Message
}

// AsToolCatalogError returns the stable transport projection for err.
func AsToolCatalogError(err error) (spec.ToolCatalogError, bool) {
	var catalogErr *CatalogError
	if !errors.As(err, &catalogErr) {
		return spec.ToolCatalogError{}, false
	}
	return catalogErr.Protocol, true
}

// NewLiveService constructs a service over any backend. A missing authorizer
// fails closed. Use NewStandaloneLiveService for the explicit single-user
// allow-all configuration.
func NewLiveService(backend LiveBackend, options LiveServiceOptions) (*LiveService, error) {
	if backend == nil {
		return nil, fmt.Errorf("catalog: live backend is required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxLease <= 0 {
		options.MaxLease = 5 * time.Minute
	}
	if options.TombstoneRetention <= 0 {
		options.TombstoneRetention = 15 * time.Minute
	}
	if options.SnapshotRetention <= 0 {
		options.SnapshotRetention = 2 * time.Minute
	}
	if options.SnapshotRetention < time.Second {
		return nil, fmt.Errorf("catalog: snapshot retention must be at least one second")
	}
	if options.MaxCASRetries <= 0 {
		options.MaxCASRetries = 16
	}
	if options.Authorizer == nil {
		options.Authorizer = denyAllEntryAuthorizer
	}
	cursorKey := append([]byte(nil), options.CursorKey...)
	if len(cursorKey) == 0 {
		cursorKey = make([]byte, 32)
		if _, err := rand.Read(cursorKey); err != nil {
			return nil, fmt.Errorf("catalog: generate cursor key: %w", err)
		}
	}
	if len(cursorKey) < 32 {
		return nil, fmt.Errorf("catalog: cursor key must be at least 32 bytes")
	}
	return &LiveService{
		backend:            backend,
		authorizer:         options.Authorizer,
		liveness:           options.Liveness,
		cursorKey:          cursorKey,
		maxLease:           options.MaxLease,
		tombstoneRetention: options.TombstoneRetention,
		snapshotRetention:  options.SnapshotRetention,
		maxCASRetries:      options.MaxCASRetries,
		now:                options.Now,
	}, nil
}

// NewStandaloneLiveService constructs the dependency-free, single-user
// service. A supplied Authorizer is honored; otherwise standalone allow-all is
// selected explicitly.
func NewStandaloneLiveService(options LiveServiceOptions) (*LiveService, error) {
	if options.Authorizer == nil {
		options.Authorizer = AllowAllEntryAuthorizer{}
	}
	return NewLiveService(newMemoryBackend(optionsNow(options)), options)
}

func optionsNow(options LiveServiceOptions) func() time.Time {
	if options.Now != nil {
		return options.Now
	}
	return time.Now
}

// Publish atomically replaces one registration after validating its trusted
// provider binding, generation transition, sequence, lease, and immutable refs.
func (s *LiveService) Publish(ctx context.Context, binding MutationBinding, publication spec.ToolCatalogPublication) (spec.ToolCatalogMutationResult, error) {
	if err := validateMutationBinding(binding); err != nil {
		return spec.ToolCatalogMutationResult{}, invalidRequest(err.Error())
	}
	if publication.SchemaVersion == "" {
		publication.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := publication.Validate(); err != nil {
		return spec.ToolCatalogMutationResult{}, invalidRequest(err.Error())
	}
	if err := bindingMatchesPublication(binding, publication); err != nil {
		return spec.ToolCatalogMutationResult{}, unauthorized(err.Error())
	}
	now := s.now().UTC()
	lease, err := s.admitLease(publication.LeaseExpiresAt, now)
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	publication.LeaseExpiresAt = lease

	state, err := s.mutate(ctx, now, func(state *LiveState) error {
		currentIndex := findPublication(state.Publications, publication.ProviderID, publication.RegistrationID)
		if currentIndex >= 0 {
			current := state.Publications[currentIndex]
			currentExpired := !now.Before(mustCatalogTime(current.LeaseExpiresAt))
			if current.Generation == publication.Generation {
				if currentExpired {
					return catalogProtocolError(CatalogErrorStaleGeneration, "provider generation lease has expired", false)
				}
				if publication.Sequence <= current.Sequence {
					return catalogProtocolError(CatalogErrorStaleSequence, "publication sequence is not newer", false)
				}
				if err := validateImmutableRevisions(current.Entries, publication.Entries); err != nil {
					return invalidRequest(err.Error())
				}
			} else {
				if !currentExpired && !binding.GenerationReplacement {
					return catalogProtocolError(CatalogErrorStaleGeneration, "a live provider generation already owns this registration", false)
				}
				appendTombstone(state, current, now.Add(s.tombstoneRetention))
			}
			state.Publications = append(state.Publications[:currentIndex], state.Publications[currentIndex+1:]...)
		}
		if findTombstone(state.Tombstones, publication.ProviderID, publication.RegistrationID, publication.Generation) >= 0 {
			return catalogProtocolError(CatalogErrorStaleGeneration, "provider generation has been replaced or revoked", false)
		}
		state.Publications = append(state.Publications, publication)
		return nil
	})
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return mutationResult(publication.ProviderID, publication.RegistrationID, publication.Generation, publication.Sequence, state)
}

// Renew advances the current generation's sequence and extends its bounded
// lease without changing descriptors.
func (s *LiveService) Renew(ctx context.Context, binding MutationBinding, request spec.ToolCatalogRenewRequest) (spec.ToolCatalogMutationResult, error) {
	if err := validateMutationBinding(binding); err != nil {
		return spec.ToolCatalogMutationResult{}, invalidRequest(err.Error())
	}
	if request.SchemaVersion == "" {
		request.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := request.Validate(); err != nil {
		return spec.ToolCatalogMutationResult{}, invalidRequest(err.Error())
	}
	if binding.ProviderID != request.ProviderID || binding.RegistrationID != request.RegistrationID || binding.Generation != request.Generation {
		return spec.ToolCatalogMutationResult{}, unauthorized("renew identity does not match authenticated provider binding")
	}
	now := s.now().UTC()
	lease, err := s.admitLease(request.LeaseExpiresAt, now)
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	state, err := s.mutate(ctx, now, func(state *LiveState) error {
		index := findPublication(state.Publications, request.ProviderID, request.RegistrationID)
		if index < 0 || state.Publications[index].Generation != request.Generation {
			return catalogProtocolError(CatalogErrorStaleGeneration, "provider generation is not current", false)
		}
		current := &state.Publications[index]
		if !now.Before(mustCatalogTime(current.LeaseExpiresAt)) {
			return catalogProtocolError(CatalogErrorStaleGeneration, "provider generation lease has expired", false)
		}
		if request.Sequence <= current.Sequence {
			return catalogProtocolError(CatalogErrorStaleSequence, "renew sequence is not newer", false)
		}
		current.Sequence = request.Sequence
		current.LeaseExpiresAt = lease
		return nil
	})
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return mutationResult(request.ProviderID, request.RegistrationID, request.Generation, request.Sequence, state)
}

// Revoke advances the sequence, removes the current generation, and writes a
// replay tombstone.
func (s *LiveService) Revoke(ctx context.Context, binding MutationBinding, request spec.ToolCatalogRevokeRequest) (spec.ToolCatalogMutationResult, error) {
	if err := validateMutationBinding(binding); err != nil {
		return spec.ToolCatalogMutationResult{}, invalidRequest(err.Error())
	}
	if request.SchemaVersion == "" {
		request.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := request.Validate(); err != nil {
		return spec.ToolCatalogMutationResult{}, invalidRequest(err.Error())
	}
	if binding.ProviderID != request.ProviderID || binding.RegistrationID != request.RegistrationID || binding.Generation != request.Generation {
		return spec.ToolCatalogMutationResult{}, unauthorized("revoke identity does not match authenticated provider binding")
	}
	now := s.now().UTC()
	state, err := s.mutate(ctx, now, func(state *LiveState) error {
		index := findPublication(state.Publications, request.ProviderID, request.RegistrationID)
		if index < 0 || state.Publications[index].Generation != request.Generation {
			return catalogProtocolError(CatalogErrorStaleGeneration, "provider generation is not current", false)
		}
		current := state.Publications[index]
		if request.Sequence <= current.Sequence {
			return catalogProtocolError(CatalogErrorStaleSequence, "revoke sequence is not newer", false)
		}
		current.Sequence = request.Sequence
		appendTombstone(state, current, now.Add(s.tombstoneRetention))
		state.Publications = append(state.Publications[:index], state.Publications[index+1:]...)
		return nil
	})
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return mutationResult(request.ProviderID, request.RegistrationID, request.Generation, request.Sequence, state)
}

// Query returns a deterministic authorized page. Continuations load the exact
// retained snapshot and never merge it with current catalog state.
func (s *LiveService) Query(ctx context.Context, binding QueryBinding, query spec.ToolCatalogQuery) (spec.ToolCatalogPage, error) {
	resolved, err := s.QueryResolved(ctx, binding, query)
	if err != nil {
		return spec.ToolCatalogPage{}, err
	}
	entries := make([]spec.ToolCatalogEntry, 0, len(resolved.Records))
	for _, record := range resolved.Records {
		entries = append(entries, record.Entry)
	}
	return spec.ToolCatalogPage{
		SchemaVersion: resolved.SchemaVersion, SnapshotID: resolved.SnapshotID,
		CatalogRevision: resolved.CatalogRevision, Entries: entries,
		NextCursor: resolved.NextCursor,
	}, nil
}

// QueryResolved returns the same deterministic snapshot/cursor result as
// Query while retaining each entry's trusted publication context for an edge
// adapter. It is an internal service projection, not a new ecosystem message.
func (s *LiveService) QueryResolved(ctx context.Context, binding QueryBinding, query spec.ToolCatalogQuery) (ResolvedCatalogPage, error) {
	if query.SchemaVersion == "" {
		query.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := query.Validate(); err != nil {
		return ResolvedCatalogPage{}, invalidRequest(err.Error())
	}
	if err := validateQueryBinding(binding, query.Context); err != nil {
		return ResolvedCatalogPage{}, unauthorized(err.Error())
	}
	bindingDigest, err := digestJSON(binding)
	if err != nil {
		return ResolvedCatalogPage{}, err
	}
	queryDigest, err := digestJSON(struct {
		Context spec.ToolCatalogContext    `json:"context"`
		Query   string                     `json:"query"`
		Extra   map[string]json.RawMessage `json:"extra,omitempty"`
	}{query.Context, strings.TrimSpace(query.Query), query.Extra})
	if err != nil {
		return ResolvedCatalogPage{}, err
	}

	if query.Cursor != "" {
		cursor, err := s.decodeCursor(query.Cursor)
		if err != nil {
			return ResolvedCatalogPage{}, catalogProtocolError(CatalogErrorStaleCursor, "cursor is invalid or has been modified", true)
		}
		if cursor.BindingDigest != bindingDigest || cursor.QueryDigest != queryDigest || cursor.Limit != query.Limit {
			return ResolvedCatalogPage{}, catalogProtocolError(CatalogErrorStaleCursor, "cursor does not match this subject, context, policy epoch, query, or limit", true)
		}
		snapshot, found, err := s.backend.LoadSnapshot(ctx, cursor.SnapshotID)
		if err != nil {
			return ResolvedCatalogPage{}, fmt.Errorf("catalog: load retained snapshot: %w", err)
		}
		if !found {
			return ResolvedCatalogPage{}, catalogProtocolError(CatalogErrorStaleSnapshot, "retained catalog snapshot is no longer available", true)
		}
		if err := validateRetainedSnapshot(snapshot); err != nil {
			return ResolvedCatalogPage{}, fmt.Errorf("catalog: invalid retained snapshot: %w", err)
		}
		if snapshot.BindingDigest != bindingDigest || snapshot.QueryDigest != queryDigest || snapshot.Limit != query.Limit {
			return ResolvedCatalogPage{}, catalogProtocolError(CatalogErrorStaleCursor, "cursor binding does not match retained snapshot", true)
		}
		if !s.now().UTC().Before(mustCatalogTime(snapshot.ExpiresAt)) || cursor.Position < 0 || cursor.Position > len(snapshot.Records) {
			return ResolvedCatalogPage{}, catalogProtocolError(CatalogErrorStaleSnapshot, "retained catalog snapshot has expired", true)
		}
		return s.resolvedPage(snapshot, cursor.Position)
	}

	snapshot, err := s.createQuerySnapshot(ctx, binding, bindingDigest, queryDigest, query)
	if err != nil {
		return ResolvedCatalogPage{}, err
	}
	return s.resolvedPage(snapshot, 0)
}

// Describe resolves one exact immutable reference, optionally within a
// retained query snapshot, and independently authorizes describe access.
func (s *LiveService) Describe(ctx context.Context, binding QueryBinding, request spec.ToolCatalogDescribeRequest) (spec.ToolCatalogDescribeResult, error) {
	resolved, err := s.DescribeResolved(ctx, binding, request)
	if err != nil {
		return spec.ToolCatalogDescribeResult{}, err
	}
	return spec.ToolCatalogDescribeResult{
		SchemaVersion: resolved.SchemaVersion, SnapshotID: resolved.SnapshotID,
		CatalogRevision: resolved.CatalogRevision, Entry: resolved.Record.Entry,
	}, nil
}

// ResolvedCatalogDescribeResult retains the trusted publication context for a
// service adapter while preserving Describe's independent authorization.
type ResolvedCatalogDescribeResult struct {
	SchemaVersion   string               `json:"schema_version"`
	SnapshotID      string               `json:"snapshot_id"`
	CatalogRevision string               `json:"catalog_revision"`
	Record          ResolvedCatalogEntry `json:"record"`
}

// DescribeResolved is Describe's service-private context-preserving form.
func (s *LiveService) DescribeResolved(ctx context.Context, binding QueryBinding, request spec.ToolCatalogDescribeRequest) (ResolvedCatalogDescribeResult, error) {
	if request.SchemaVersion == "" {
		request.SchemaVersion = spec.ToolCatalogSchemaVersion
	}
	if err := request.Validate(); err != nil {
		return ResolvedCatalogDescribeResult{}, invalidRequest(err.Error())
	}
	if err := validateQueryBinding(binding, request.Context); err != nil {
		return ResolvedCatalogDescribeResult{}, unauthorized(err.Error())
	}
	bindingDigest, err := digestJSON(binding)
	if err != nil {
		return ResolvedCatalogDescribeResult{}, err
	}

	var snapshot RetainedSnapshot
	if request.SnapshotID != "" {
		var found bool
		snapshot, found, err = s.backend.LoadSnapshot(ctx, request.SnapshotID)
		if err != nil {
			return ResolvedCatalogDescribeResult{}, fmt.Errorf("catalog: load retained snapshot: %w", err)
		}
		if !found || !s.now().UTC().Before(mustCatalogTime(snapshot.ExpiresAt)) {
			return ResolvedCatalogDescribeResult{}, catalogProtocolError(CatalogErrorStaleSnapshot, "retained catalog snapshot is no longer available", true)
		}
		if err := validateRetainedSnapshot(snapshot); err != nil {
			return ResolvedCatalogDescribeResult{}, fmt.Errorf("catalog: invalid retained snapshot: %w", err)
		}
		if snapshot.BindingDigest != bindingDigest {
			return ResolvedCatalogDescribeResult{}, unauthorized("snapshot belongs to a different subject, context, or policy epoch")
		}
	} else {
		snapshot, err = s.createDescribeSnapshot(ctx, binding, bindingDigest, request)
		if err != nil {
			return ResolvedCatalogDescribeResult{}, err
		}
	}

	record, count := findExactRecord(snapshot.Records, request.Ref)
	if count == 0 {
		return ResolvedCatalogDescribeResult{}, catalogProtocolError(CatalogErrorStaleGeneration, "exact tool reference is not present in the selected snapshot", false)
	}
	if count > 1 {
		return ResolvedCatalogDescribeResult{}, catalogProtocolError(CatalogErrorAmbiguousRef, "exact tool reference resolved more than once", false)
	}
	entry := record.Entry
	allowed, err := s.authorizer.AuthorizeCatalogEntry(ctx, CatalogActionDescribe, binding, entry)
	if err != nil {
		return ResolvedCatalogDescribeResult{}, fmt.Errorf("catalog: authorize describe: %w", err)
	}
	if !allowed {
		return ResolvedCatalogDescribeResult{}, unauthorized("describe access denied")
	}
	return ResolvedCatalogDescribeResult{
		SchemaVersion:   spec.ToolCatalogSchemaVersion,
		SnapshotID:      snapshot.SnapshotID,
		CatalogRevision: snapshot.CatalogRevision,
		Record:          record,
	}, nil
}

// ResolveInvocation resolves an exact reference only against the currently
// live, context-matching, routable generation, then performs the independent
// effect-specific invocation decision. It never falls back to a bare name,
// another revision, or a retained discovery decision.
func (s *LiveService) ResolveInvocation(ctx context.Context, binding QueryBinding, ref spec.ToolReference) (spec.ToolCatalogEntry, error) {
	record, err := s.ResolveInvocationRecord(ctx, binding, ref)
	if err != nil {
		return spec.ToolCatalogEntry{}, err
	}
	return record.Entry, nil
}

// ResolveInvocationRecord performs exact current-generation invocation
// resolution and returns the trusted publication context beside the entry.
func (s *LiveService) ResolveInvocationRecord(ctx context.Context, binding QueryBinding, ref spec.ToolReference) (ResolvedCatalogEntry, error) {
	if err := ref.Validate(); err != nil {
		return ResolvedCatalogEntry{}, invalidRequest(err.Error())
	}
	if err := validateQueryBinding(binding, binding.Context); err != nil {
		return ResolvedCatalogEntry{}, unauthorized(err.Error())
	}
	record, err := s.backend.LoadState(ctx)
	if err != nil {
		return ResolvedCatalogEntry{}, fmt.Errorf("catalog: load live state: %w", err)
	}
	state, err := prepareState(record.State, record.Exists)
	if err != nil {
		return ResolvedCatalogEntry{}, fmt.Errorf("catalog: invalid backend state: %w", err)
	}
	now := s.now().UTC()
	entries := make([]ResolvedCatalogEntry, 0, 1)
	for _, publication := range state.Publications {
		if !publicationLiveAt(publication, now) || !contextMatches(publication.Context, binding.Context) {
			continue
		}
		if s.liveness != nil {
			routable, err := s.liveness.CatalogPublicationRoutable(ctx, publication)
			if err != nil {
				return ResolvedCatalogEntry{}, fmt.Errorf("catalog: check publication liveness: %w", err)
			}
			if !routable {
				continue
			}
		}
		for _, entry := range publication.Entries {
			if refsEqual(entry.Ref, ref) {
				entries = append(entries, ResolvedCatalogEntry{Entry: entry, Context: publication.Context})
			}
		}
	}
	if len(entries) == 0 {
		return ResolvedCatalogEntry{}, catalogProtocolError(CatalogErrorStaleGeneration, "exact tool reference is not currently live", false)
	}
	if len(entries) > 1 {
		return ResolvedCatalogEntry{}, catalogProtocolError(CatalogErrorAmbiguousRef, "exact tool reference resolved more than once", false)
	}
	action, err := InvocationCatalogAction(entries[0].Entry.Effect)
	if err != nil {
		return ResolvedCatalogEntry{}, err
	}
	allowed, err := s.authorizer.AuthorizeCatalogEntry(ctx, action, binding, entries[0].Entry)
	if err != nil {
		return ResolvedCatalogEntry{}, fmt.Errorf("catalog: authorize invocation: %w", err)
	}
	if !allowed {
		return ResolvedCatalogEntry{}, unauthorized("invocation access denied")
	}
	return entries[0], nil
}

// InvocationCatalogAction maps the admitted effect to the canonical Aether
// resource operation. Unknown effects fail closed.
func InvocationCatalogAction(effect spec.ToolEffect) (string, error) {
	switch effect {
	case spec.ToolEffectRead:
		return CatalogActionInvokeRead, nil
	case spec.ToolEffectWrite:
		return CatalogActionInvokeWrite, nil
	case spec.ToolEffectExecute:
		return CatalogActionInvokeExecute, nil
	case spec.ToolEffectExternal:
		return CatalogActionInvokeExternal, nil
	case spec.ToolEffectInteraction:
		return CatalogActionInvokeInteraction, nil
	default:
		return "", catalogProtocolError(CatalogErrorUnauthorized, "unknown tool effect has no invocation operation", false)
	}
}

func (s *LiveService) createQuerySnapshot(ctx context.Context, binding QueryBinding, bindingDigest, queryDigest string, query spec.ToolCatalogQuery) (RetainedSnapshot, error) {
	record, err := s.backend.LoadState(ctx)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: load live state: %w", err)
	}
	state, err := prepareState(record.State, record.Exists)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: invalid backend state: %w", err)
	}
	now := s.now().UTC()
	records := make([]ResolvedCatalogEntry, 0)
	for _, publication := range state.Publications {
		if !publicationLiveAt(publication, now) || !contextMatches(publication.Context, query.Context) {
			continue
		}
		if s.liveness != nil {
			routable, err := s.liveness.CatalogPublicationRoutable(ctx, publication)
			if err != nil {
				return RetainedSnapshot{}, fmt.Errorf("catalog: check publication liveness: %w", err)
			}
			if !routable {
				continue
			}
		}
		for _, entry := range publication.Entries {
			if !entryMatchesQuery(entry, query.Query) {
				continue
			}
			allowed, err := s.authorizer.AuthorizeCatalogEntry(ctx, CatalogActionDiscover, binding, entry)
			if err != nil {
				return RetainedSnapshot{}, fmt.Errorf("catalog: authorize discovery: %w", err)
			}
			if allowed {
				records = append(records, ResolvedCatalogEntry{Entry: entry, Context: publication.Context})
			}
		}
	}
	if err := sortAndCheckRecords(records); err != nil {
		return RetainedSnapshot{}, err
	}
	revision, err := stateRevision(state)
	if err != nil {
		return RetainedSnapshot{}, err
	}
	return s.retainSnapshot(ctx, revision, bindingDigest, queryDigest, query.Limit, records, now)
}

func (s *LiveService) createDescribeSnapshot(ctx context.Context, binding QueryBinding, bindingDigest string, request spec.ToolCatalogDescribeRequest) (RetainedSnapshot, error) {
	record, err := s.backend.LoadState(ctx)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: load live state: %w", err)
	}
	state, err := prepareState(record.State, record.Exists)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: invalid backend state: %w", err)
	}
	now := s.now().UTC()
	records := make([]ResolvedCatalogEntry, 0, 1)
	for _, publication := range state.Publications {
		if !publicationLiveAt(publication, now) || !contextMatches(publication.Context, request.Context) {
			continue
		}
		if s.liveness != nil {
			routable, err := s.liveness.CatalogPublicationRoutable(ctx, publication)
			if err != nil {
				return RetainedSnapshot{}, fmt.Errorf("catalog: check publication liveness: %w", err)
			}
			if !routable {
				continue
			}
		}
		for _, entry := range publication.Entries {
			if refsEqual(entry.Ref, request.Ref) {
				records = append(records, ResolvedCatalogEntry{Entry: entry, Context: publication.Context})
			}
		}
	}
	if err := sortAndCheckRecords(records); err != nil {
		return RetainedSnapshot{}, err
	}
	queryDigest, err := digestJSON(struct {
		Describe spec.ToolReference `json:"describe"`
	}{request.Ref})
	if err != nil {
		return RetainedSnapshot{}, err
	}
	revision, err := stateRevision(state)
	if err != nil {
		return RetainedSnapshot{}, err
	}
	return s.retainSnapshot(ctx, revision, bindingDigest, queryDigest, 1, records, now)
}

func (s *LiveService) retainSnapshot(ctx context.Context, revision, bindingDigest, queryDigest string, limit uint32, records []ResolvedCatalogEntry, now time.Time) (RetainedSnapshot, error) {
	snapshotID, err := retainedSnapshotID(revision, bindingDigest, queryDigest, limit, records)
	if err != nil {
		return RetainedSnapshot{}, err
	}
	snapshot := RetainedSnapshot{
		SchemaVersion:   LiveStateSchemaVersion,
		SnapshotID:      snapshotID,
		CatalogRevision: revision,
		BindingDigest:   bindingDigest,
		QueryDigest:     queryDigest,
		Limit:           limit,
		CreatedAt:       now.Format(time.RFC3339Nano),
		ExpiresAt:       now.Add(s.snapshotRetention).Format(time.RFC3339Nano),
		Records:         records,
	}
	retained, err := s.backend.StoreSnapshot(ctx, snapshot, s.snapshotRetention)
	if err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: retain snapshot: %w", err)
	}
	if err := validateRetainedSnapshot(retained); err != nil {
		return RetainedSnapshot{}, fmt.Errorf("catalog: invalid retained snapshot: %w", err)
	}
	return retained, nil
}

func (s *LiveService) resolvedPage(snapshot RetainedSnapshot, position int) (ResolvedCatalogPage, error) {
	if position < 0 || position > len(snapshot.Records) {
		return ResolvedCatalogPage{}, catalogProtocolError(CatalogErrorStaleCursor, "cursor position is outside the retained snapshot", true)
	}
	end := position + int(snapshot.Limit)
	if end > len(snapshot.Records) {
		end = len(snapshot.Records)
	}
	records := append([]ResolvedCatalogEntry(nil), snapshot.Records[position:end]...)
	page := ResolvedCatalogPage{
		SchemaVersion:   spec.ToolCatalogSchemaVersion,
		SnapshotID:      snapshot.SnapshotID,
		CatalogRevision: snapshot.CatalogRevision,
		Records:         records,
		AmbiguousNames:  ambiguousRecordNames(snapshot.Records),
	}
	if end < len(snapshot.Records) {
		cursor, err := s.encodeCursor(catalogCursor{
			Version:       cursorVersion,
			SnapshotID:    snapshot.SnapshotID,
			BindingDigest: snapshot.BindingDigest,
			QueryDigest:   snapshot.QueryDigest,
			Limit:         snapshot.Limit,
			Position:      end,
			ExpiresAt:     snapshot.ExpiresAt,
		})
		if err != nil {
			return ResolvedCatalogPage{}, err
		}
		page.NextCursor = cursor
	}
	return page, nil
}

func (s *LiveService) mutate(ctx context.Context, now time.Time, change func(*LiveState) error) (LiveState, error) {
	for attempt := 0; attempt < s.maxCASRetries; attempt++ {
		record, err := s.backend.LoadState(ctx)
		if err != nil {
			return LiveState{}, fmt.Errorf("catalog: load live state: %w", err)
		}
		state, err := prepareState(record.State, record.Exists)
		if err != nil {
			return LiveState{}, fmt.Errorf("catalog: invalid backend state: %w", err)
		}
		pruneTombstones(&state, now)
		pruneExpiredPublications(&state, now, s.tombstoneRetention)
		if err := change(&state); err != nil {
			return LiveState{}, err
		}
		normalizeState(&state)
		if err := validateState(state); err != nil {
			return LiveState{}, fmt.Errorf("catalog: invalid state mutation: %w", err)
		}
		applied, err := s.backend.CompareAndSwapState(ctx, record, state)
		if err != nil {
			return LiveState{}, fmt.Errorf("catalog: compare-and-swap live state: %w", err)
		}
		if applied {
			return state, nil
		}
	}
	return LiveState{}, catalogProtocolError(CatalogErrorConcurrentUpdate, "catalog changed too frequently; retry the mutation", false)
}

func pruneExpiredPublications(state *LiveState, now time.Time, retention time.Duration) {
	live := state.Publications[:0]
	for _, publication := range state.Publications {
		if publicationLiveAt(publication, now) {
			live = append(live, publication)
			continue
		}
		appendTombstone(state, publication, now.Add(retention))
	}
	state.Publications = live
}

func prepareState(state LiveState, exists bool) (LiveState, error) {
	if !exists {
		return LiveState{SchemaVersion: LiveStateSchemaVersion, Publications: []spec.ToolCatalogPublication{}}, nil
	}
	if err := validateState(state); err != nil {
		return LiveState{}, err
	}
	normalizeState(&state)
	return state, nil
}

func validateState(state LiveState) error {
	if state.SchemaVersion != LiveStateSchemaVersion {
		return fmt.Errorf("unsupported live state schema version %q", state.SchemaVersion)
	}
	seenRegistrations := make(map[string]struct{}, len(state.Publications))
	seenGenerations := make(map[string]struct{}, len(state.Publications)+len(state.Tombstones))
	for i, publication := range state.Publications {
		if err := publication.Validate(); err != nil {
			return fmt.Errorf("publications[%d]: %w", i, err)
		}
		registration := publication.ProviderID + "\x00" + publication.RegistrationID
		if _, exists := seenRegistrations[registration]; exists {
			return fmt.Errorf("duplicate current provider registration")
		}
		seenRegistrations[registration] = struct{}{}
		seenGenerations[registration+"\x00"+publication.Generation] = struct{}{}
	}
	for i, tombstone := range state.Tombstones {
		request := spec.ToolCatalogRevokeRequest{
			SchemaVersion: spec.ToolCatalogSchemaVersion,
			ProviderID:    tombstone.ProviderID, RegistrationID: tombstone.RegistrationID,
			Generation: tombstone.Generation, Sequence: tombstone.Sequence,
		}
		if err := request.Validate(); err != nil {
			return fmt.Errorf("tombstones[%d]: %w", i, err)
		}
		if _, err := parseCatalogTime(tombstone.RetainUntil); err != nil {
			return fmt.Errorf("tombstones[%d].retain_until: %w", i, err)
		}
		key := tombstone.ProviderID + "\x00" + tombstone.RegistrationID + "\x00" + tombstone.Generation
		if _, exists := seenGenerations[key]; exists {
			return fmt.Errorf("duplicate or current tombstoned provider generation")
		}
		seenGenerations[key] = struct{}{}
	}
	return nil
}

func normalizeState(state *LiveState) {
	state.SchemaVersion = LiveStateSchemaVersion
	if state.Publications == nil {
		state.Publications = []spec.ToolCatalogPublication{}
	}
	sort.Slice(state.Publications, func(i, j int) bool {
		left, right := state.Publications[i], state.Publications[j]
		if left.ProviderID != right.ProviderID {
			return left.ProviderID < right.ProviderID
		}
		return left.RegistrationID < right.RegistrationID
	})
	sort.Slice(state.Tombstones, func(i, j int) bool {
		left, right := state.Tombstones[i], state.Tombstones[j]
		if left.ProviderID != right.ProviderID {
			return left.ProviderID < right.ProviderID
		}
		if left.RegistrationID != right.RegistrationID {
			return left.RegistrationID < right.RegistrationID
		}
		return left.Generation < right.Generation
	})
}

func pruneTombstones(state *LiveState, now time.Time) {
	retained := state.Tombstones[:0]
	for _, tombstone := range state.Tombstones {
		if now.Before(mustCatalogTime(tombstone.RetainUntil)) {
			retained = append(retained, tombstone)
		}
	}
	state.Tombstones = retained
}

func appendTombstone(state *LiveState, publication spec.ToolCatalogPublication, retainUntil time.Time) {
	state.Tombstones = append(state.Tombstones, GenerationTombstone{
		ProviderID: publication.ProviderID, RegistrationID: publication.RegistrationID,
		Generation: publication.Generation, Sequence: publication.Sequence,
		RetainUntil: retainUntil.UTC().Format(time.RFC3339Nano),
	})
}

func validateMutationBinding(binding MutationBinding) error {
	for field, value := range map[string]string{
		"provider_id": binding.ProviderID, "registration_id": binding.RegistrationID, "generation": binding.Generation,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("%s must be a canonical non-empty identifier", field)
		}
	}
	if err := binding.RequiredContext.Validate(); err != nil {
		return fmt.Errorf("required context: %w", err)
	}
	return nil
}

func bindingMatchesPublication(binding MutationBinding, publication spec.ToolCatalogPublication) error {
	if binding.ProviderID != publication.ProviderID || binding.RegistrationID != publication.RegistrationID || binding.Generation != publication.Generation {
		return fmt.Errorf("publication identity does not match authenticated provider binding")
	}
	for _, pair := range contextFieldPairs(binding.RequiredContext, publication.Context) {
		if pair.required != "" && pair.required != pair.actual {
			return fmt.Errorf("publication does not preserve authenticated %s confinement", pair.name)
		}
	}
	return nil
}

func validateQueryBinding(binding QueryBinding, context spec.ToolCatalogContext) error {
	if strings.TrimSpace(binding.SubjectID) == "" || strings.TrimSpace(binding.SubjectID) != binding.SubjectID || strings.ContainsRune(binding.SubjectID, '\x00') {
		return fmt.Errorf("authenticated subject is required")
	}
	if strings.TrimSpace(binding.PolicyEpoch) == "" || strings.TrimSpace(binding.PolicyEpoch) != binding.PolicyEpoch || strings.ContainsRune(binding.PolicyEpoch, '\x00') {
		return fmt.Errorf("authorization policy epoch is required")
	}
	if err := binding.Context.Validate(); err != nil {
		return fmt.Errorf("authenticated context: %w", err)
	}
	if !contextsEqual(binding.Context, context) {
		return fmt.Errorf("query context does not match authenticated context")
	}
	return nil
}

type contextFieldPair struct{ name, required, actual string }

func contextFieldPairs(required, actual spec.ToolCatalogContext) []contextFieldPair {
	return []contextFieldPair{
		{"workspace_id", required.WorkspaceID, actual.WorkspaceID},
		{"thread_id", required.ThreadID, actual.ThreadID},
		{"view_id", required.ViewID, actual.ViewID},
		{"tool_host_id", required.ToolHostID, actual.ToolHostID},
		{"surface_kind", required.SurfaceKind, actual.SurfaceKind},
		{"surface_instance_id", required.SurfaceInstanceID, actual.SurfaceInstanceID},
	}
}

func contextsEqual(left, right spec.ToolCatalogContext) bool {
	for _, pair := range contextFieldPairs(left, right) {
		if pair.required != pair.actual {
			return false
		}
	}
	return true
}

func contextMatches(publication, query spec.ToolCatalogContext) bool {
	for _, pair := range contextFieldPairs(publication, query) {
		if pair.required != "" && pair.required != pair.actual {
			return false
		}
	}
	return true
}

func (s *LiveService) admitLease(value string, now time.Time) (string, error) {
	expires, err := parseCatalogTime(value)
	if err != nil {
		return "", invalidRequest(err.Error())
	}
	if !now.Before(expires) {
		return "", catalogProtocolError(CatalogErrorLeaseExpired, "lease expiry must be in the future", false)
	}
	maximum := now.Add(s.maxLease)
	if expires.After(maximum) {
		expires = maximum
	}
	return expires.UTC().Format(time.RFC3339Nano), nil
}

func validateImmutableRevisions(previous, next []spec.ToolCatalogEntry) error {
	byRef := make(map[string]spec.ToolCatalogEntry, len(previous))
	for _, entry := range previous {
		byRef[refKey(entry.Ref)] = entry
	}
	for _, entry := range next {
		old, exists := byRef[refKey(entry.Ref)]
		if !exists {
			continue
		}
		oldJSON, err := json.Marshal(old)
		if err != nil {
			return err
		}
		newJSON, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if !hmac.Equal(oldJSON, newJSON) {
			return fmt.Errorf("tool ref %q changed content without a new revision", refKey(entry.Ref))
		}
	}
	return nil
}

func entryMatchesQuery(entry spec.ToolCatalogEntry, query string) bool {
	terms := strings.Fields(strings.ToLower(strings.TrimSpace(query)))
	if len(terms) == 0 {
		return true
	}
	haystack := strings.ToLower(strings.Join(append([]string{
		entry.Ref.Name, entry.Descriptor.Title, entry.Descriptor.Description,
		string(entry.Effect),
	}, entry.Descriptor.Toolsets...), "\n"))
	for _, term := range terms {
		if !strings.Contains(haystack, term) {
			return false
		}
	}
	return true
}

func sortAndCheckRecords(records []ResolvedCatalogEntry) error {
	sort.Slice(records, func(i, j int) bool { return refKey(records[i].Entry.Ref) < refKey(records[j].Entry.Ref) })
	for i := range records {
		if i > 0 && refKey(records[i-1].Entry.Ref) == refKey(records[i].Entry.Ref) {
			return catalogProtocolError(CatalogErrorAmbiguousRef, "exact tool reference appears in multiple live publications", false)
		}
	}
	return nil
}

func findExactRecord(records []ResolvedCatalogEntry, ref spec.ToolReference) (ResolvedCatalogEntry, int) {
	var found ResolvedCatalogEntry
	count := 0
	for _, record := range records {
		if refsEqual(record.Entry.Ref, ref) {
			found = record
			count++
		}
	}
	return found, count
}

func ambiguousRecordNames(records []ResolvedCatalogEntry) []string {
	counts := make(map[string]int, len(records))
	for _, record := range records {
		counts[record.Entry.Ref.Name]++
	}
	result := make([]string, 0)
	for name, count := range counts {
		if count > 1 {
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result
}

func refsEqual(left, right spec.ToolReference) bool { return refKey(left) == refKey(right) }

func refKey(ref spec.ToolReference) string {
	return strings.Join([]string{ref.ProviderID, ref.RegistrationID, ref.Generation, ref.Name, ref.Revision}, "\x00")
}

func findPublication(publications []spec.ToolCatalogPublication, providerID, registrationID string) int {
	for i, publication := range publications {
		if publication.ProviderID == providerID && publication.RegistrationID == registrationID {
			return i
		}
	}
	return -1
}

func findTombstone(tombstones []GenerationTombstone, providerID, registrationID, generation string) int {
	for i, tombstone := range tombstones {
		if tombstone.ProviderID == providerID && tombstone.RegistrationID == registrationID && tombstone.Generation == generation {
			return i
		}
	}
	return -1
}

func publicationLiveAt(publication spec.ToolCatalogPublication, now time.Time) bool {
	expires, err := parseCatalogTime(publication.LeaseExpiresAt)
	return err == nil && now.Before(expires)
}

func mutationResult(providerID, registrationID, generation string, sequence uint64, state LiveState) (spec.ToolCatalogMutationResult, error) {
	revision, err := stateRevision(state)
	if err != nil {
		return spec.ToolCatalogMutationResult{}, err
	}
	return spec.ToolCatalogMutationResult{
		SchemaVersion: spec.ToolCatalogSchemaVersion, ProviderID: providerID,
		RegistrationID: registrationID, Generation: generation,
		AcceptedSequence: sequence, CatalogRevision: revision,
	}, nil
}

func stateRevision(state LiveState) (string, error) {
	normalizeState(&state)
	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("catalog: encode state revision: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func digestJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("catalog: canonical digest: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func parseCatalogTime(value string) (time.Time, error) {
	if value == "" || !strings.HasSuffix(value, "Z") {
		return time.Time{}, fmt.Errorf("timestamp must be canonical UTC RFC3339")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.Format(time.RFC3339Nano) != value {
		return time.Time{}, fmt.Errorf("timestamp must be canonical UTC RFC3339")
	}
	return parsed, nil
}

func mustCatalogTime(value string) time.Time {
	parsed, _ := parseCatalogTime(value)
	return parsed
}

func validateRetainedSnapshot(snapshot RetainedSnapshot) error {
	if snapshot.SchemaVersion != LiveStateSchemaVersion {
		return fmt.Errorf("unsupported retained snapshot schema version %q", snapshot.SchemaVersion)
	}
	if snapshot.SnapshotID == "" || snapshot.CatalogRevision == "" || snapshot.BindingDigest == "" || snapshot.QueryDigest == "" {
		return fmt.Errorf("snapshot identifiers and bindings are required")
	}
	if snapshot.Limit == 0 || snapshot.Limit > 100 {
		return fmt.Errorf("snapshot limit must be between 1 and 100")
	}
	created, err := parseCatalogTime(snapshot.CreatedAt)
	if err != nil {
		return fmt.Errorf("created_at: %w", err)
	}
	expires, err := parseCatalogTime(snapshot.ExpiresAt)
	if err != nil {
		return fmt.Errorf("expires_at: %w", err)
	}
	if !created.Before(expires) {
		return fmt.Errorf("snapshot expiry must follow creation")
	}
	previous := ""
	for i, record := range snapshot.Records {
		if err := record.Entry.Validate(); err != nil {
			return fmt.Errorf("records[%d].entry: %w", i, err)
		}
		if err := record.Context.Validate(); err != nil {
			return fmt.Errorf("records[%d].context: %w", i, err)
		}
		key := refKey(record.Entry.Ref)
		if i > 0 && key <= previous {
			return fmt.Errorf("snapshot records must have unique, canonically ordered refs")
		}
		previous = key
	}
	expectedID, err := retainedSnapshotID(
		snapshot.CatalogRevision, snapshot.BindingDigest, snapshot.QueryDigest, snapshot.Limit, snapshot.Records,
	)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(snapshot.SnapshotID), []byte(expectedID)) {
		return fmt.Errorf("snapshot content digest does not match snapshot_id")
	}
	return nil
}

func retainedSnapshotID(revision, bindingDigest, queryDigest string, limit uint32, records []ResolvedCatalogEntry) (string, error) {
	return digestJSON(struct {
		Revision      string                 `json:"revision"`
		BindingDigest string                 `json:"binding_digest"`
		QueryDigest   string                 `json:"query_digest"`
		Limit         uint32                 `json:"limit"`
		Records       []ResolvedCatalogEntry `json:"records"`
	}{revision, bindingDigest, queryDigest, limit, records})
}

type catalogCursor struct {
	Version       string `json:"version"`
	SnapshotID    string `json:"snapshot_id"`
	BindingDigest string `json:"binding_digest"`
	QueryDigest   string `json:"query_digest"`
	Limit         uint32 `json:"limit"`
	Position      int    `json:"position"`
	ExpiresAt     string `json:"expires_at"`
}

func (s *LiveService) encodeCursor(cursor catalogCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("catalog: encode cursor: %w", err)
	}
	mac := hmac.New(sha256.New, s.cursorKey)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (s *LiveService) decodeCursor(encoded string) (catalogCursor, error) {
	parts := strings.Split(encoded, ".")
	if len(parts) != 2 {
		return catalogCursor{}, fmt.Errorf("invalid cursor encoding")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return catalogCursor{}, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return catalogCursor{}, err
	}
	mac := hmac.New(sha256.New, s.cursorKey)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return catalogCursor{}, fmt.Errorf("invalid cursor signature")
	}
	var cursor catalogCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return catalogCursor{}, err
	}
	if cursor.Version != cursorVersion || cursor.SnapshotID == "" || cursor.BindingDigest == "" || cursor.QueryDigest == "" || cursor.Limit == 0 {
		return catalogCursor{}, fmt.Errorf("invalid cursor fields")
	}
	expires, err := parseCatalogTime(cursor.ExpiresAt)
	if err != nil || !s.now().UTC().Before(expires) {
		return catalogCursor{}, fmt.Errorf("cursor has expired")
	}
	return cursor, nil
}

func catalogProtocolError(code, message string, restart bool) error {
	return &CatalogError{Protocol: spec.ToolCatalogError{
		SchemaVersion: spec.ToolCatalogSchemaVersion,
		Code:          code, Message: message, RestartRequired: restart,
	}}
}

func invalidRequest(message string) error {
	return catalogProtocolError(CatalogErrorInvalidRequest, message, false)
}

func unauthorized(message string) error {
	return catalogProtocolError(CatalogErrorUnauthorized, message, false)
}
