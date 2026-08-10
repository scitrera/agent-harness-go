package aether

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

var workspaceToolNames = map[string]struct{}{
	"read_file":    {},
	"write_file":   {},
	"edit_file":    {},
	"list_dir":     {},
	"inspect_file": {},
	"shell":        {},
	"python":       {},
}

type boundWorkspaceToolHostConfig struct {
	WorkspaceID   string
	WorkspaceRoot string
	StateDir      string
	ToolHostID    string
	ExecutionSite spec.ExecutionSite
	Publisher     workspacepkg.ViewPublisher
	Python        string
	Timeout       time.Duration
	MaxOutput     int
}

// boundWorkspaceToolHost owns the host-private root mapping and local tool
// registries shared by client-attached and worker-authoritative execution.
// Authorization remains the responsibility of the composition-specific host.
type boundWorkspaceToolHost struct {
	views     *workspacepkg.ViewRegistry
	python    string
	timeout   time.Duration
	maxOutput int

	mu         sync.Mutex
	registries map[string]*tools.Registry
}

func newBoundWorkspaceToolHost(ctx context.Context, cfg boundWorkspaceToolHostConfig) (*boundWorkspaceToolHost, error) {
	views, err := workspacepkg.NewViewRegistry(ctx, workspacepkg.ViewRegistryConfig{
		InitialWorkspaceID: cfg.WorkspaceID,
		InitialRoot:        cfg.WorkspaceRoot,
		StateDir:           cfg.StateDir,
		ToolHostID:         cfg.ToolHostID,
		ExecutionSite:      cfg.ExecutionSite,
		Publisher:          cfg.Publisher,
	})
	if err != nil {
		return nil, err
	}
	host := &boundWorkspaceToolHost{
		views: views, python: cfg.Python, timeout: cfg.Timeout, maxOutput: cfg.MaxOutput,
		registries: map[string]*tools.Registry{},
	}
	if host.python == "" {
		host.python = "python3"
	}
	if host.timeout <= 0 {
		host.timeout = 30 * time.Second
	}
	if host.maxOutput <= 0 {
		host.maxOutput = 1 << 20
	}
	return host, nil
}

func (h *boundWorkspaceToolHost) GrantWorkingDirectory(dir string) error {
	return h.views.GrantWorkingDirectory(dir)
}

func (h *boundWorkspaceToolHost) ExecutionBindingForDirectory(ctx context.Context, dir string) (spec.ExecutionBinding, error) {
	return h.views.ExecutionBindingForDirectory(ctx, dir)
}

func (h *boundWorkspaceToolHost) ScheduledExecutionBindingForDirectory(
	ctx context.Context,
	dir string,
	allowMutable bool,
	allowDirty bool,
) (spec.ExecutionBinding, error) {
	return h.views.ScheduledExecutionBindingForDirectory(ctx, dir, allowMutable, allowDirty)
}

func (h *boundWorkspaceToolHost) Renew(ctx context.Context) error {
	return h.views.RenewRegisteredViews(ctx)
}

func (h *boundWorkspaceToolHost) invokeBound(ctx context.Context, binding spec.ExecutionBinding, req tools.Request) (tools.Result, error) {
	if _, ok := workspaceToolNames[req.Name]; !ok {
		return tools.Result{}, fmt.Errorf("tool %q is not hosted by this workspace host", req.Name)
	}
	if req.Addr.WorkspaceID != binding.WorkspaceID {
		return tools.Result{}, fmt.Errorf("tool invocation workspace does not match execution binding")
	}
	view, err := h.views.ResolveCurrentBinding(ctx, binding)
	if err != nil {
		return tools.Result{}, err
	}
	registry, err := h.registryFor(view)
	if err != nil {
		return tools.Result{}, err
	}
	cwd := view.Root
	if binding.RelativeDirectory != "" {
		cwd = filepath.Join(view.Root, filepath.FromSlash(binding.RelativeDirectory))
	}
	return registry.Invoke(tools.WithWorkingDirectory(ctx, cwd), req)
}

func (h *boundWorkspaceToolHost) registryFor(view workspacepkg.View) (*tools.Registry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if registry := h.registries[view.Descriptor.ViewID]; registry != nil {
		return registry, nil
	}
	workspace, err := localtools.NewWorkspace(view.Root)
	if err != nil {
		return nil, err
	}
	registry := tools.NewRegistry()
	if err := tools.RegisterLocal(registry, tools.LocalConfig{
		Workspace: workspace,
		Python:    h.python,
		Timeout:   h.timeout,
		MaxOutput: h.maxOutput,
	}); err != nil {
		return nil, err
	}
	h.registries[view.Descriptor.ViewID] = registry
	return registry, nil
}

// WorkerToolHostConfig configures tools rooted on the durable Aether worker.
type WorkerToolHostConfig struct {
	WorkspaceID   string
	WorkspaceRoot string
	StateDir      string
	ToolHostID    string
	Publisher     workspacepkg.ViewPublisher
	Python        string
	Timeout       time.Duration
	MaxOutput     int
	// AccessAuthorizer is an enterprise resource-level policy seam. Aether ACLs
	// still decide who may create/route the task; this policy decides whether
	// that task principal may use this worker's concrete workspace view.
	AccessAuthorizer WorkerToolAccessAuthorizer
}

type WorkerToolAccessRequest struct {
	Binding   spec.ExecutionBinding
	Address   spec.MessageAddress
	Authority tools.MemoryAuthority
}

type WorkerToolAccessAuthorizer interface {
	AuthorizeWorkerTool(ctx context.Context, request WorkerToolAccessRequest) error
}

// ScheduledViewPolicy is captured with a versioned task envelope. False values
// are the safe default: require a clean Git worktree.
type ScheduledViewPolicy struct {
	AllowMutableView bool `json:"allow_mutable_view"`
	AllowDirtyView   bool `json:"allow_dirty_view"`
}

// WorkerToolHost exposes only exact bindings registered by this worker. The
// caller's Aether assignment and ACL are the authority; the binding is a
// resource selector and never a bearer grant.
type WorkerToolHost struct {
	local            *boundWorkspaceToolHost
	accessAuthorizer WorkerToolAccessAuthorizer
}

func NewWorkerToolHost(ctx context.Context, cfg WorkerToolHostConfig) (*WorkerToolHost, error) {
	local, err := newBoundWorkspaceToolHost(ctx, boundWorkspaceToolHostConfig{
		WorkspaceID: cfg.WorkspaceID, WorkspaceRoot: cfg.WorkspaceRoot, StateDir: cfg.StateDir,
		ToolHostID: cfg.ToolHostID, ExecutionSite: spec.ExecutionSiteWorker,
		Publisher: cfg.Publisher, Python: cfg.Python, Timeout: cfg.Timeout, MaxOutput: cfg.MaxOutput,
	})
	if err != nil {
		return nil, err
	}
	return &WorkerToolHost{local: local, accessAuthorizer: cfg.AccessAuthorizer}, nil
}

func (h *WorkerToolHost) ExecutionBindingForDirectory(ctx context.Context, dir string) (spec.ExecutionBinding, error) {
	return h.local.ExecutionBindingForDirectory(ctx, dir)
}

func (h *WorkerToolHost) ScheduledExecutionBindingForDirectory(
	ctx context.Context,
	dir string,
	allowMutable bool,
	allowDirty bool,
) (spec.ExecutionBinding, error) {
	return h.local.ScheduledExecutionBindingForDirectory(ctx, dir, allowMutable, allowDirty)
}

func (h *WorkerToolHost) Renew(ctx context.Context) error { return h.local.Renew(ctx) }

func (h *WorkerToolHost) validateScheduledBinding(
	ctx context.Context,
	binding spec.ExecutionBinding,
	policy ScheduledViewPolicy,
) error {
	if h == nil || h.local == nil {
		return fmt.Errorf("aether: worker workspace tool host is not configured")
	}
	return h.local.views.ValidateScheduledBinding(
		ctx, binding, policy.AllowMutableView, policy.AllowDirtyView,
	)
}

func (h *WorkerToolHost) invoke(
	ctx context.Context,
	binding spec.ExecutionBinding,
	policy ScheduledViewPolicy,
	req tools.Request,
) (tools.Result, error) {
	if h == nil || h.local == nil {
		return tools.Result{}, fmt.Errorf("aether: worker workspace tool host is not configured")
	}
	if err := h.validateScheduledBinding(ctx, binding, policy); err != nil {
		return tools.Result{}, fmt.Errorf("aether: scheduled workspace view is no longer admissible: %w", err)
	}
	if h.accessAuthorizer != nil {
		authority, _ := tools.MemoryAuthorityFrom(ctx)
		if err := h.accessAuthorizer.AuthorizeWorkerTool(ctx, WorkerToolAccessRequest{
			Binding: binding, Address: req.Addr, Authority: authority,
		}); err != nil {
			return tools.Result{}, fmt.Errorf("worker tool access denied: %w", err)
		}
	}
	return h.local.invokeBound(ctx, binding, req)
}
