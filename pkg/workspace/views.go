// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package workspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const (
	// ViewObservationLeaseDuration is the validity window published by a live
	// tool host. It is intentionally longer than the shared renewal cadence.
	ViewObservationLeaseDuration = 5 * time.Minute
	// ViewObservationRenewInterval is shared by client and worker hosts so both
	// remain discoverable without transport-specific magic numbers.
	ViewObservationRenewInterval = 2 * time.Minute
)

// WorkspaceToolCapabilities is the deterministic capability set advertised by
// a Sahara client that hosts the built-in workspace tools.
var WorkspaceToolCapabilities = []string{
	"command.run",
	"directory.list",
	"file.inspect",
	"file.read",
	"file.write",
	"python.run",
	spec.ToolCancelCapability,
}

// VCSObservation is the portable Git state published for a local view. IDs are
// hashes of local Git identities; host-local paths never leave this process.
type VCSObservation struct {
	Kind         string
	RepositoryID string
	WorktreeID   string
	HeadRevision string
	Branch       string
	Dirty        bool
	Detached     bool
}

// View binds one local directory root to a durable logical workspace view.
// Root is host-private. RootRef is the opaque value peers may echo back only to
// this exact tool host.
type View struct {
	Descriptor spec.WorkspaceViewDescriptor
	Root       string
	RootRef    string
	VCS        *VCSObservation
}

// ViewObservation is the latest host observation published to the durable
// workspace authority.
type ViewObservation struct {
	ObserverID    string
	Generation    string
	Sequence      int64
	ToolHostID    string
	ExecutionSite spec.ExecutionSite
	RootRef       string
	Capabilities  []string
	VCS           *VCSObservation
	ExpiresAt     time.Time
}

// ViewPublisher stores durable view identity and latest observed state. A nil
// publisher leaves the registry fully useful in standalone OSS mode.
type ViewPublisher interface {
	PublishWorkspaceView(ctx context.Context, view View, observation ViewObservation) error
}

// Principal is a transport-authenticated actor or on-behalf-of subject. The
// empty value means no OBO subject was asserted by the transport.
type Principal struct {
	Type string
	ID   string
}

// ExecutionBindingAuthorizationRequest gives a durable authority enough
// trusted transport context to apply same-window, same-user, named-principal,
// or workspace-member policy. OSS uses the strict same-window policy.
type ExecutionBindingAuthorizationRequest struct {
	Binding     spec.ExecutionBinding
	ViewPolicy  ExecutionViewPolicy
	SourceTopic string
	// RequestUserID is an application-level claim from the message address. It
	// is useful for audit/correlation but MUST NOT be treated as authenticated
	// identity. SourceTopic and OnBehalfOf are transport-authenticated.
	RequestUserID string
	OnBehalfOf    Principal
}

// ViewRegistryConfig configures a client-local workspace/view registry.
type ViewRegistryConfig struct {
	InitialWorkspaceID string
	InitialRoot        string
	StateDir           string
	ToolHostID         string
	ExecutionSite      spec.ExecutionSite
	Publisher          ViewPublisher
}

// ViewRegistry discovers project roots, assigns logical workspaces, and keeps
// the host-private mapping needed to execute a logical binding safely.
type ViewRegistry struct {
	mu        sync.RWMutex
	publishMu sync.Mutex

	initialWorkspaceID string
	initialRoot        string
	initialRepository  string
	toolHostID         string
	executionSite      spec.ExecutionSite
	generation         string
	resolver           *Resolver
	publisher          ViewPublisher
	views              map[string]View
	sequence           map[string]int64
}

// NewViewRegistry creates a registry and publishes its initial view when a
// publisher is configured.
func NewViewRegistry(ctx context.Context, cfg ViewRegistryConfig) (*ViewRegistry, error) {
	if strings.TrimSpace(cfg.InitialWorkspaceID) == "" {
		return nil, fmt.Errorf("workspace view registry: initial workspace id required")
	}
	if strings.TrimSpace(cfg.ToolHostID) == "" {
		return nil, fmt.Errorf("workspace view registry: tool host id required")
	}
	switch cfg.ExecutionSite {
	case spec.ExecutionSiteClient, spec.ExecutionSiteWorker, spec.ExecutionSiteRemote:
	default:
		return nil, fmt.Errorf("workspace view registry: execution site required")
	}
	root, err := canonicalDirectory(cfg.InitialRoot)
	if err != nil {
		return nil, fmt.Errorf("workspace view registry: initial root: %w", err)
	}
	resolver, err := NewResolver(Config{StateDir: cfg.StateDir})
	if err != nil {
		return nil, fmt.Errorf("workspace view registry: resolver: %w", err)
	}
	generation, err := randomOpaqueID("generation-")
	if err != nil {
		return nil, fmt.Errorf("workspace view registry: generation: %w", err)
	}
	r := &ViewRegistry{
		initialWorkspaceID: strings.TrimSpace(cfg.InitialWorkspaceID),
		initialRoot:        root,
		toolHostID:         strings.TrimSpace(cfg.ToolHostID),
		executionSite:      cfg.ExecutionSite,
		generation:         generation,
		resolver:           resolver,
		publisher:          cfg.Publisher,
		views:              map[string]View{},
		sequence:           map[string]int64{},
	}
	if vcs := observeGit(ctx, root); vcs != nil {
		r.initialRepository = vcs.RepositoryID
	}
	if _, err := r.register(ctx, root); err != nil {
		return nil, err
	}
	return r, nil
}

// GrantWorkingDirectory registers the project/view containing dir. It matches
// the TUI DirectoryAccess seam.
func (r *ViewRegistry) GrantWorkingDirectory(dir string) error {
	_, err := r.register(context.Background(), dir)
	return err
}

// ExecutionBindingForDirectory returns an exact-host logical binding for dir.
func (r *ViewRegistry) ExecutionBindingForDirectory(ctx context.Context, dir string) (spec.ExecutionBinding, error) {
	view, err := r.register(ctx, dir)
	if err != nil {
		return spec.ExecutionBinding{}, err
	}
	view, err = r.refreshObservation(ctx, view)
	if err != nil {
		return spec.ExecutionBinding{}, err
	}
	target, err := canonicalDirectory(dir)
	if err != nil {
		return spec.ExecutionBinding{}, err
	}
	relative, err := filepath.Rel(view.Root, target)
	if err != nil {
		return spec.ExecutionBinding{}, fmt.Errorf("workspace view relative directory: %w", err)
	}
	relative = filepath.ToSlash(relative)
	if relative == "." {
		relative = ""
	}
	normalized, err := spec.NormalizeWorkspaceRelativeDirectory(relative)
	if err != nil {
		return spec.ExecutionBinding{}, err
	}
	binding := spec.NewExecutionBinding(
		view.Descriptor.WorkspaceID,
		view.Descriptor.ViewID,
		r.toolHostID,
		r.executionSite,
	)
	binding.RootRef = view.RootRef
	binding.RelativeDirectory = normalized
	binding.Revision = view.Descriptor.Revision
	return binding, binding.Validate()
}

// ResolveWorkspaceForDirectory returns the logical project identity while
// retaining distinct registered views for separate worktrees of that project.
func (r *ViewRegistry) ResolveWorkspaceForDirectory(ctx context.Context, dir string) (string, error) {
	view, err := r.register(ctx, dir)
	if err != nil {
		return "", err
	}
	return view.Descriptor.WorkspaceID, nil
}

// ScheduledExecutionBindingForDirectory pins a background turn to the latest
// observed revision. Mutable directories and dirty worktrees require explicit
// opt-in because they cannot otherwise be reproduced after a delayed fire.
func (r *ViewRegistry) ScheduledExecutionBindingForDirectory(
	ctx context.Context,
	dir string,
	allowMutable bool,
	allowDirty bool,
) (spec.ExecutionBinding, error) {
	binding, err := r.ExecutionBindingForDirectory(ctx, dir)
	if err != nil {
		return spec.ExecutionBinding{}, err
	}
	if err := r.ValidateScheduledBinding(ctx, binding, allowMutable, allowDirty); err != nil {
		return spec.ExecutionBinding{}, err
	}
	return binding, nil
}

// ValidateScheduledBinding rechecks reproducibility policy immediately before
// admission or execution. Unlike interactive views, a scheduled clean Git view
// must stay clean for the full task; dirty or non-VCS views are usable only
// under the explicit policy captured in the versioned schedule envelope.
func (r *ViewRegistry) ValidateScheduledBinding(
	ctx context.Context,
	binding spec.ExecutionBinding,
	allowMutable bool,
	allowDirty bool,
) error {
	view, err := r.ResolveCurrentBinding(ctx, binding)
	if err != nil {
		return err
	}
	vcs := observeGit(ctx, view.Root)
	if vcs == nil && !allowMutable {
		return fmt.Errorf("scheduled workspace view is mutable; set allow_mutable_view explicitly")
	}
	if vcs != nil && vcs.Dirty && !allowDirty {
		return fmt.Errorf("scheduled workspace view has uncommitted changes; set allow_dirty_view explicitly")
	}
	return nil
}

// RenewRegisteredViews republishes current observations without changing view
// identity. Worker processes call it before MemoryLayer's observation lease
// expires so durable selectors remain live.
func (r *ViewRegistry) RenewRegisteredViews(ctx context.Context) error {
	r.mu.RLock()
	views := make([]View, 0, len(r.views))
	for _, view := range r.views {
		views = append(views, view)
	}
	r.mu.RUnlock()
	for _, view := range views {
		if _, err := r.refreshObservation(ctx, view); err != nil {
			return err
		}
	}
	return nil
}

// SetPublisher publishes every currently registered view, then installs the
// publisher for future observations. It supports transports that must connect
// before their remote MemoryLayer path is usable while still allowing worker
// assignment handlers to be registered before that connection starts.
func (r *ViewRegistry) SetPublisher(ctx context.Context, publisher ViewPublisher) error {
	if publisher == nil {
		return fmt.Errorf("workspace view registry: publisher is required")
	}
	r.publishMu.Lock()
	defer r.publishMu.Unlock()
	r.mu.RLock()
	views := make([]View, 0, len(r.views))
	for _, view := range r.views {
		views = append(views, view)
	}
	r.mu.RUnlock()
	sort.Slice(views, func(i, j int) bool { return views[i].Descriptor.ViewID < views[j].Descriptor.ViewID })
	for _, view := range views {
		view.VCS = observeGit(ctx, view.Root)
		if view.VCS != nil {
			view.Descriptor.Revision = view.VCS.HeadRevision
		} else {
			view.Descriptor.Revision = ""
		}
		r.mu.RLock()
		sequence := r.sequence[view.Descriptor.ViewID] + 1
		r.mu.RUnlock()
		if err := publisher.PublishWorkspaceView(ctx, view, ViewObservation{
			ObserverID: r.toolHostID, Generation: r.generation, Sequence: sequence,
			ToolHostID: r.toolHostID, ExecutionSite: r.executionSite, RootRef: view.RootRef,
			Capabilities: append([]string(nil), WorkspaceToolCapabilities...), VCS: view.VCS,
			ExpiresAt: time.Now().Add(ViewObservationLeaseDuration),
		}); err != nil {
			return fmt.Errorf("publish registered workspace view: %w", err)
		}
		r.mu.Lock()
		r.views[view.Descriptor.ViewID] = view
		r.sequence[view.Descriptor.ViewID] = sequence
		r.mu.Unlock()
	}
	r.publisher = publisher
	return nil
}

func (r *ViewRegistry) refreshObservation(ctx context.Context, view View) (View, error) {
	r.publishMu.Lock()
	defer r.publishMu.Unlock()
	view.VCS = observeGit(ctx, view.Root)
	if view.VCS != nil {
		view.Descriptor.Revision = view.VCS.HeadRevision
	} else {
		view.Descriptor.Revision = ""
	}
	r.mu.Lock()
	r.sequence[view.Descriptor.ViewID]++
	sequence := r.sequence[view.Descriptor.ViewID]
	r.mu.Unlock()
	if r.publisher != nil {
		observation := ViewObservation{
			ObserverID:    r.toolHostID,
			Generation:    r.generation,
			Sequence:      sequence,
			ToolHostID:    r.toolHostID,
			ExecutionSite: r.executionSite,
			RootRef:       view.RootRef,
			Capabilities:  append([]string(nil), WorkspaceToolCapabilities...),
			VCS:           view.VCS,
			ExpiresAt:     time.Now().Add(ViewObservationLeaseDuration),
		}
		if err := r.publisher.PublishWorkspaceView(ctx, view, observation); err != nil {
			return View{}, fmt.Errorf("refresh workspace view observation: %w", err)
		}
	}
	r.mu.Lock()
	r.views[view.Descriptor.ViewID] = view
	r.mu.Unlock()
	return view, nil
}

// ResolveBinding returns the host-private view selected by a validated binding.
// Every identity component is checked; callers must not fall back on failure.
func (r *ViewRegistry) ResolveBinding(binding spec.ExecutionBinding) (View, error) {
	if err := binding.Validate(); err != nil {
		return View{}, err
	}
	if binding.ToolHostID != r.toolHostID || binding.ExecutionSite != r.executionSite {
		return View{}, fmt.Errorf("workspace binding targets a different tool host")
	}
	r.mu.RLock()
	view, ok := r.views[binding.ViewID]
	r.mu.RUnlock()
	if !ok {
		return View{}, fmt.Errorf("workspace view %q is not registered on this tool host", binding.ViewID)
	}
	if view.Descriptor.WorkspaceID != binding.WorkspaceID || view.RootRef != binding.RootRef {
		return View{}, fmt.Errorf("workspace binding does not match the registered view")
	}
	return view, nil
}

// ResolveCurrentBinding performs the exact-host identity checks and verifies a
// pinned VCS revision against the execution host immediately before use. This
// closes the admission-to-execution gap if another process switches worktrees.
func (r *ViewRegistry) ResolveCurrentBinding(ctx context.Context, binding spec.ExecutionBinding) (View, error) {
	view, err := r.ResolveBinding(binding)
	if err != nil {
		return View{}, err
	}
	if binding.Revision == "" {
		return view, nil
	}
	vcs := observeGit(ctx, view.Root)
	if vcs == nil || vcs.HeadRevision != binding.Revision {
		return View{}, fmt.Errorf("workspace binding revision is no longer current")
	}
	return view, nil
}

func (r *ViewRegistry) register(ctx context.Context, dir string) (View, error) {
	target, err := canonicalDirectory(dir)
	if err != nil {
		return View{}, fmt.Errorf("register workspace view: %w", err)
	}
	if existing, ok := r.registeredViewContaining(target); ok {
		return existing, nil
	}
	project, err := r.resolver.detector.Detect(ctx, target)
	if err != nil {
		return View{}, fmt.Errorf("register workspace view: detect project: %w", err)
	}
	root, err := canonicalDirectory(project.Root)
	if err != nil {
		return View{}, fmt.Errorf("register workspace view: canonical project: %w", err)
	}
	vcs := observeGit(ctx, root)
	workspaceID := r.initialWorkspaceID
	sameInitialRepository := vcs != nil && r.initialRepository != "" && vcs.RepositoryID == r.initialRepository
	if root != r.initialRoot && !sameInitialRepository {
		resolution, resolveErr := r.resolver.Resolve(ctx, Request{CWD: root})
		if resolveErr != nil {
			return View{}, fmt.Errorf("register workspace view: resolve logical workspace: %w", resolveErr)
		}
		workspaceID = resolution.WorkspaceID
	}
	viewID := opaqueHash("view", root)
	r.publishMu.Lock()
	defer r.publishMu.Unlock()
	r.mu.RLock()
	existing, ok := r.views[viewID]
	r.mu.RUnlock()
	if ok {
		return existing, nil
	}
	kind := spec.WorkspaceViewKindDirectory
	revision := ""
	if vcs != nil {
		kind = spec.WorkspaceViewKindGitWorktree
		revision = vcs.HeadRevision
	}
	view := View{
		Descriptor: spec.WorkspaceViewDescriptor{
			WorkspaceID:  workspaceID,
			ViewID:       viewID,
			Kind:         kind,
			DisplayName:  filepath.Base(root),
			Revision:     revision,
			Capabilities: append([]string(nil), WorkspaceToolCapabilities...),
		},
		Root:    root,
		RootRef: "root:" + viewID,
		VCS:     vcs,
	}
	if err := view.Descriptor.Validate(); err != nil {
		return View{}, err
	}
	r.mu.Lock()
	if existing, ok := r.views[viewID]; ok {
		r.mu.Unlock()
		return existing, nil
	}
	sequence := r.sequence[viewID] + 1
	r.mu.Unlock()
	if r.publisher != nil {
		observation := ViewObservation{
			ObserverID:    r.toolHostID,
			Generation:    r.generation,
			Sequence:      sequence,
			ToolHostID:    r.toolHostID,
			ExecutionSite: r.executionSite,
			RootRef:       view.RootRef,
			Capabilities:  append([]string(nil), WorkspaceToolCapabilities...),
			VCS:           vcs,
			ExpiresAt:     time.Now().Add(ViewObservationLeaseDuration),
		}
		if err := r.publisher.PublishWorkspaceView(ctx, view, observation); err != nil {
			return View{}, fmt.Errorf("publish workspace view: %w", err)
		}
	}
	r.mu.Lock()
	r.views[viewID] = view
	r.sequence[viewID] = sequence
	r.mu.Unlock()
	return view, nil
}

func (r *ViewRegistry) registeredViewContaining(target string) (View, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var selected View
	for _, view := range r.views {
		relative, err := filepath.Rel(view.Root, target)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		if selected.Root == "" || len(view.Root) > len(selected.Root) {
			selected = view
		}
	}
	return selected, selected.Root != ""
}

func observeGit(ctx context.Context, root string) *VCSObservation {
	commonDir, err := gitOutput(ctx, root, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil || commonDir == "" {
		return nil
	}
	gitDir, err := gitOutput(ctx, root, "rev-parse", "--path-format=absolute", "--git-dir")
	if err != nil || gitDir == "" {
		return nil
	}
	head, _ := gitOutput(ctx, root, "rev-parse", "HEAD")
	branch, branchErr := gitOutput(ctx, root, "symbolic-ref", "--short", "-q", "HEAD")
	status, _ := gitOutput(ctx, root, "status", "--porcelain", "--untracked-files=normal")
	return &VCSObservation{
		Kind:         "git",
		RepositoryID: opaqueHash("repository", filepath.Clean(commonDir)),
		WorktreeID:   opaqueHash("worktree", filepath.Clean(gitDir)),
		HeadRevision: head,
		Branch:       branch,
		Dirty:        status != "",
		Detached:     branchErr != nil || branch == "",
	}
}

func gitOutput(ctx context.Context, root string, args ...string) (string, error) {
	commandArgs := append([]string{"-C", root}, args...)
	output, err := exec.CommandContext(ctx, "git", commandArgs...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func opaqueHash(kind, value string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + value))
	return kind + "-" + hex.EncodeToString(sum[:16])
}

func randomOpaqueID(prefix string) (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value), nil
}
