package requirements

import (
	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type Requirements struct {
	Approval     *Sourced[ApprovalRequirement]        `json:"approval,omitempty"`
	ToolPolicy   SourcedToolPolicy                    `json:"tool_policy"`
	Hooks        SourcedHooks                         `json:"hooks"`
	MCPServers   map[string]Sourced[mcp.ServerConfig] `json:"mcp_servers,omitempty"`
	Catalogs     map[string]Sourced[CatalogSource]    `json:"catalogs,omitempty"`
	Models       map[string]Sourced[ModelChoice]      `json:"models,omitempty"`
	FileStores   map[string]Sourced[FileStorePath]    `json:"file_stores,omitempty"`
	FeatureGates map[string]Sourced[FeatureGate]      `json:"feature_gates,omitempty"`
}

type SourcedToolPolicy struct {
	DefaultDecision        *Sourced[tools.DecisionCode]    `json:"default_decision,omitempty"`
	ResolveHostExecutables *Sourced[bool]                  `json:"resolve_host_executables,omitempty"`
	Rules                  []Sourced[tools.CommandRule]    `json:"rules,omitempty"`
	HostExecutables        []Sourced[tools.HostExecutable] `json:"host_executables,omitempty"`
}

type SourcedHooks struct {
	Config *Sourced[hooks.Config]       `json:"config,omitempty"`
	Hooks  []Sourced[hooks.CommandHook] `json:"hooks,omitempty"`
}

func (r Requirements) CommandPolicyConfig() tools.CommandPolicyConfig {
	cfg := tools.CommandPolicyConfig{}
	if r.ToolPolicy.DefaultDecision != nil {
		cfg.DefaultDecision = r.ToolPolicy.DefaultDecision.Value
	}
	if r.ToolPolicy.ResolveHostExecutables != nil {
		cfg.ResolveHostExecutables = r.ToolPolicy.ResolveHostExecutables.Value
	}
	cfg.Rules = make([]tools.CommandRule, 0, len(r.ToolPolicy.Rules))
	for _, rule := range r.ToolPolicy.Rules {
		cfg.Rules = append(cfg.Rules, rule.Value)
	}
	cfg.HostExecutables = make([]tools.HostExecutable, 0, len(r.ToolPolicy.HostExecutables))
	for _, executable := range r.ToolPolicy.HostExecutables {
		cfg.HostExecutables = append(cfg.HostExecutables, executable.Value)
	}
	return cfg
}

func (r Requirements) HookRuntimeConfig() (hooks.Config, []hooks.CommandHook) {
	var cfg hooks.Config
	if r.Hooks.Config != nil {
		cfg = r.Hooks.Config.Value
	}
	commandHooks := make([]hooks.CommandHook, 0, len(r.Hooks.Hooks))
	for _, hook := range r.Hooks.Hooks {
		commandHooks = append(commandHooks, hook.Value)
	}
	return cfg, commandHooks
}
