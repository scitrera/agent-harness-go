package requirements

import (
	"time"

	"github.com/scitrera/agent-harness-go/pkg/hooks"
	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type ApprovalMode string

const (
	ApprovalUnspecified ApprovalMode = ""
	ApprovalNever       ApprovalMode = "never"
	ApprovalOnRequest   ApprovalMode = "on_request"
	ApprovalAlways      ApprovalMode = "always"
)

type Layer struct {
	Source       Source
	Approval     *ApprovalRequirement
	ToolPolicy   *ToolPolicyRequirement
	Hooks        *HookRequirement
	MCPServers   []mcp.ServerConfig
	Catalogs     []CatalogSource
	Models       []ModelChoice
	FileStores   []FileStorePath
	FeatureGates []FeatureGate
}

type ApprovalRequirement struct {
	Mode ApprovalMode
}

type ToolPolicyRequirement struct {
	DefaultDecision        tools.DecisionCode
	ResolveHostExecutables *bool
	Rules                  []tools.CommandRule
	HostExecutables        []tools.HostExecutable
}

type HookRequirement struct {
	Config hooks.Config
	Hooks  []hooks.CommandHook
}

type CatalogKind string

const (
	CatalogPlugin CatalogKind = "plugin"
	CatalogAgent  CatalogKind = "agent"
	CatalogSkill  CatalogKind = "skill"
)

type CatalogSource struct {
	Name string
	Kind CatalogKind
	Path string
	URI  string
}

type ModelChoice struct {
	Role  string
	Model string
}

type FileStorePath struct {
	Name string
	Path string
}

type FeatureGate struct {
	Name    string
	Enabled bool
}

func hookTimeoutValue(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(d)
}
