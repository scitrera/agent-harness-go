// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package memorylayer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

// WorkspaceViewPublisher stores the durable identity and latest observation of
// local Sahara workspace views in MemoryLayer.
type WorkspaceViewPublisher struct {
	validator *WorkspaceBindingValidator
	store     *Store
}

// WorkspaceBindingValidator reads MemoryLayer's durable workspace-view and
// observer heads. It deliberately applies no principal policy: Aether (or the
// standalone same-window adapter) decides who may bind the exact resource.
type WorkspaceBindingValidator struct {
	client *memorylayersdk.Client
}

// ValidateExecutionBinding verifies the durable view and latest exact
// observer/host mapping. It never substitutes another observer, host, root, or
// revision for the binding supplied by the caller.
func (v *WorkspaceBindingValidator) ValidateExecutionBinding(ctx context.Context, binding spec.ExecutionBinding) error {
	if v == nil || v.client == nil {
		return fmt.Errorf("validate workspace binding: validator is not configured")
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	view, err := v.client.WorkspaceViews.Get(ctx, binding.WorkspaceID, binding.ViewID, nil)
	if err != nil {
		return fmt.Errorf("authorize workspace view: %w", err)
	}
	if view.WorkspaceID != binding.WorkspaceID || view.ViewID != binding.ViewID {
		return fmt.Errorf("authorize workspace view: durable identity mismatch")
	}
	observation, err := v.client.WorkspaceViews.GetObservation(
		ctx, binding.WorkspaceID, binding.ViewID, binding.ToolHostID, nil,
	)
	if err != nil {
		return fmt.Errorf("authorize workspace observation: %w", err)
	}
	if observation.ObserverID != binding.ToolHostID ||
		observation.ToolHostID == nil || *observation.ToolHostID != binding.ToolHostID ||
		observation.ExecutionSite == nil || *observation.ExecutionSite != string(binding.ExecutionSite) ||
		observation.RootRef == nil || *observation.RootRef != binding.RootRef {
		return fmt.Errorf("authorize workspace observation: exact host binding mismatch")
	}
	if observation.ExpiresAt != nil && !observation.ExpiresAt.After(time.Now()) {
		return fmt.Errorf("authorize workspace observation: observation expired")
	}
	if binding.Revision != "" && (observation.VCS == nil || observation.VCS.HeadRevision == nil ||
		*observation.VCS.HeadRevision != binding.Revision) {
		return fmt.Errorf("authorize workspace observation: revision changed")
	}
	return nil
}

// AuthorizeExecutionBinding verifies the durable view and the latest
// observer-specific host mapping. It never accepts a different host/root as a
// fallback, even when another observation for the same logical view exists.
func (p *WorkspaceViewPublisher) AuthorizeExecutionBinding(ctx context.Context, request workspacepkg.ExecutionBindingAuthorizationRequest) error {
	binding := request.Binding
	// OSS is intentionally private to the originating Aether user window. An
	// enterprise composition may replace this authorizer with one that combines
	// a durable sharing policy, workspace membership, and Aether OBO grants.
	if request.SourceTopic == "" || request.SourceTopic != binding.ToolHostID {
		return fmt.Errorf("authorize workspace observation: requesting principal does not own the tool host")
	}
	return p.validator.ValidateExecutionBinding(ctx, binding)
}

// NewWorkspaceBindingValidator constructs the read-only MemoryLayer authority
// adapter used by enterprise compositions. It does not create workspaces or
// publish observations.
func NewWorkspaceBindingValidator(cfg Config) (*WorkspaceBindingValidator, error) {
	client, err := newWorkspaceViewsClient(cfg)
	if err != nil {
		return nil, err
	}
	return &WorkspaceBindingValidator{client: client}, nil
}

// NewWorkspaceViewPublisher constructs the typed workspace-view adapter. store
// is used to ensure a logical workspace exists before its first view is written.
func NewWorkspaceViewPublisher(cfg Config, store *Store) (*WorkspaceViewPublisher, error) {
	if store == nil {
		return nil, fmt.Errorf("memorylayer workspace views: store required")
	}
	validator, err := NewWorkspaceBindingValidator(cfg)
	if err != nil {
		return nil, err
	}
	return &WorkspaceViewPublisher{validator: validator, store: store}, nil
}

func newWorkspaceViewsClient(cfg Config) (*memorylayersdk.Client, error) {
	opts := []memorylayersdk.Option{
		memorylayersdk.WithWorkspaceID(cfg.Workspace),
	}
	if cfg.BaseURL != "" {
		opts = append(opts, memorylayersdk.WithBaseURL(cfg.BaseURL))
	}
	if cfg.APIKey != "" {
		opts = append(opts, memorylayersdk.WithAPIKey(cfg.APIKey))
	}
	if cfg.HTTPClient != nil {
		opts = append(opts, memorylayersdk.WithHTTPClient(cfg.HTTPClient))
	}
	if cfg.Transport != nil {
		opts = append(opts, memorylayersdk.WithTransport(cfg.Transport))
	}
	client, err := memorylayersdk.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("memorylayer workspace views: client: %w", err)
	}
	return client, nil
}

// PublishWorkspaceView upserts the durable view descriptor and then publishes
// the observer's monotonic latest-state cursor.
func (p *WorkspaceViewPublisher) PublishWorkspaceView(
	ctx context.Context,
	view workspacepkg.View,
	observation workspacepkg.ViewObservation,
) error {
	workspaceID := view.Descriptor.WorkspaceID
	if err := p.store.EnsureWorkspace(ctx, workspaceID); err != nil {
		return err
	}
	input := memorylayersdk.WorkspaceViewCreateInput{
		ViewID:        view.Descriptor.ViewID,
		Kind:          string(view.Descriptor.Kind),
		Capabilities:  append([]string(nil), view.Descriptor.Capabilities...),
		Metadata:      map[string]any{},
		SchemaVersion: 1,
	}
	if view.Descriptor.DisplayName != "" {
		input.DisplayName = stringPointer(view.Descriptor.DisplayName)
	}
	if view.Descriptor.MemoryContextID != "" {
		input.MemoryContextID = stringPointer(view.Descriptor.MemoryContextID)
	}
	existing, err := p.validator.client.WorkspaceViews.Get(ctx, workspaceID, view.Descriptor.ViewID, nil)
	if memorylayersdk.IsNotFound(err) {
		_, err = p.validator.client.WorkspaceViews.Create(ctx, input, memorylayersdk.WorkspaceViewMutationOptions{
			WorkspaceID:    workspaceID,
			IdempotencyKey: mutationID("view-create", workspaceID, view.Descriptor.ViewID),
		})
		if err != nil {
			var conflict *memorylayersdk.ConflictError
			if !errors.As(err, &conflict) {
				return fmt.Errorf("create view: %w", err)
			}
			existing, err = p.validator.client.WorkspaceViews.Get(ctx, workspaceID, view.Descriptor.ViewID, nil)
		}
	}
	if err != nil {
		return fmt.Errorf("get view: %w", err)
	}
	if existing != nil && workspaceViewChanged(existing, input) {
		_, err = p.validator.client.WorkspaceViews.Replace(ctx, view.Descriptor.ViewID, memorylayersdk.WorkspaceViewReplaceInput{
			Kind:            input.Kind,
			DisplayName:     input.DisplayName,
			MemoryContextID: input.MemoryContextID,
			Capabilities:    input.Capabilities,
			Metadata:        input.Metadata,
			SchemaVersion:   input.SchemaVersion,
		}, memorylayersdk.WorkspaceViewMutationOptions{
			WorkspaceID:    workspaceID,
			ETag:           existing.ETag,
			IdempotencyKey: mutationID("view-replace", workspaceID, view.Descriptor.ViewID, existing.ETag),
		})
		if err != nil {
			return fmt.Errorf("replace view: %w", err)
		}
	}

	observationInput := memorylayersdk.WorkspaceViewObservationInput{
		Generation:    observation.Generation,
		Sequence:      observation.Sequence,
		ToolHostID:    stringPointer(observation.ToolHostID),
		ExecutionSite: stringPointer(string(observation.ExecutionSite)),
		RootRef:       stringPointer(observation.RootRef),
		Capabilities:  append([]string(nil), observation.Capabilities...),
		SchemaVersion: 1,
	}
	if !observation.ExpiresAt.IsZero() {
		expiresAt := observation.ExpiresAt
		observationInput.ExpiresAt = &expiresAt
	}
	if observation.VCS != nil {
		observationInput.VCS = &memorylayersdk.WorkspaceVCSObservation{
			Kind:         observation.VCS.Kind,
			RepositoryID: optionalStringPointer(observation.VCS.RepositoryID),
			WorktreeID:   optionalStringPointer(observation.VCS.WorktreeID),
			HeadRevision: optionalStringPointer(observation.VCS.HeadRevision),
			Branch:       optionalStringPointer(observation.VCS.Branch),
			Dirty:        boolPointer(observation.VCS.Dirty),
			Detached:     boolPointer(observation.VCS.Detached),
		}
	}
	mutationOpts := memorylayersdk.WorkspaceObservationMutationOptions{
		WorkspaceID:    workspaceID,
		IdempotencyKey: mutationID("observation", workspaceID, view.Descriptor.ViewID, observation.ObserverID, observation.Generation, fmt.Sprint(observation.Sequence)),
	}
	_, err = p.validator.client.WorkspaceViews.CreateObservation(
		ctx, view.Descriptor.ViewID, observation.ObserverID, observationInput, mutationOpts,
	)
	if err == nil {
		return nil
	}
	var conflict *memorylayersdk.ConflictError
	if !errors.As(err, &conflict) {
		return fmt.Errorf("create view observation: %w", err)
	}
	current, err := p.validator.client.WorkspaceViews.GetObservation(
		ctx, workspaceID, view.Descriptor.ViewID, observation.ObserverID, nil,
	)
	if err != nil {
		return fmt.Errorf("get view observation: %w", err)
	}
	mutationOpts.ETag = current.ETag
	_, err = p.validator.client.WorkspaceViews.ReplaceObservation(
		ctx, view.Descriptor.ViewID, observation.ObserverID, observationInput, mutationOpts,
	)
	if err != nil {
		return fmt.Errorf("replace view observation: %w", err)
	}
	return nil
}

func workspaceViewChanged(existing *memorylayersdk.WorkspaceView, input memorylayersdk.WorkspaceViewCreateInput) bool {
	return existing.Kind != input.Kind ||
		!equalOptionalString(existing.DisplayName, input.DisplayName) ||
		!equalOptionalString(existing.MemoryContextID, input.MemoryContextID) ||
		!reflect.DeepEqual(existing.Capabilities, input.Capabilities) ||
		!reflect.DeepEqual(existing.Metadata, input.Metadata) ||
		existing.SchemaVersion != input.SchemaVersion
}

func equalOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func mutationID(parts ...string) string {
	return strings.Join(parts, ":")
}

func stringPointer(value string) *string { return &value }

func optionalStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func boolPointer(value bool) *bool { return &value }

var _ workspacepkg.ViewPublisher = (*WorkspaceViewPublisher)(nil)
