package subagent

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidDefinition     = errors.New("subagent: invalid agent definition")
	ErrInvalidPermissionMode = errors.New("subagent: invalid permission mode")
	ErrToolDenied            = errors.New("subagent: tool denied")
)

type AgentName string

type AgentType string

type PermissionMode string

const (
	PermissionModeInherit  PermissionMode = "inherit"
	PermissionModeAsk      PermissionMode = "ask"
	PermissionModeAllow    PermissionMode = "allow"
	PermissionModeReadOnly PermissionMode = "read_only"
)

type Definition struct {
	Name           AgentName      `json:"name"`
	Type           AgentType      `json:"type"`
	Description    string         `json:"description"`
	Prompt         string         `json:"prompt,omitempty"`
	PromptPath     string         `json:"prompt_path,omitempty"`
	Model          string         `json:"model,omitempty"`
	MaxTurns       int            `json:"max_turns,omitempty"`
	AllowedTools   []string       `json:"tool_allow,omitempty"`
	DeniedTools    []string       `json:"tool_deny,omitempty"`
	Skills         []string       `json:"skills,omitempty"`
	MCPServers     []string       `json:"mcp_servers,omitempty"`
	PermissionMode PermissionMode `json:"permission_mode,omitempty"`
	ExecPolicyHint string         `json:"exec_policy_hint,omitempty"`
	Background     bool           `json:"background,omitempty"`
}

func (d Definition) Validate() error {
	if strings.TrimSpace(string(d.Name)) == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidDefinition)
	}
	if strings.TrimSpace(string(d.Type)) == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidDefinition)
	}
	if strings.TrimSpace(d.Description) == "" {
		return fmt.Errorf("%w: description is required", ErrInvalidDefinition)
	}
	if strings.TrimSpace(d.Prompt) == "" && strings.TrimSpace(d.PromptPath) == "" {
		return fmt.Errorf("%w: prompt or prompt_path is required", ErrInvalidDefinition)
	}
	if d.MaxTurns < 0 {
		return fmt.Errorf("%w: max_turns must be positive", ErrInvalidDefinition)
	}
	if !d.PermissionMode.valid() {
		return fmt.Errorf("%w: %s", ErrInvalidPermissionMode, d.PermissionMode)
	}
	return nil
}

func (d Definition) AllowsTool(tool string) error {
	tool = strings.TrimSpace(tool)
	if tool == "" {
		return nil
	}
	if contains(d.DeniedTools, tool) {
		return fmt.Errorf("%w: %s", ErrToolDenied, tool)
	}
	if len(d.AllowedTools) > 0 && !contains(d.AllowedTools, tool) {
		return fmt.Errorf("%w: %s is not in allow list", ErrToolDenied, tool)
	}
	return nil
}

func (r Request) AllowsTool(tool string) error {
	tool = strings.TrimSpace(tool)
	if tool == "" {
		return nil
	}
	if contains(r.DeniedTools, tool) {
		return fmt.Errorf("%w: %s", ErrToolDenied, tool)
	}
	if len(r.AllowedTools) > 0 && !contains(r.AllowedTools, tool) {
		return fmt.Errorf("%w: %s is not in allow list", ErrToolDenied, tool)
	}
	return nil
}

func (m PermissionMode) valid() bool {
	switch m {
	case "", PermissionModeInherit, PermissionModeAsk, PermissionModeAllow, PermissionModeReadOnly:
		return true
	default:
		return false
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
