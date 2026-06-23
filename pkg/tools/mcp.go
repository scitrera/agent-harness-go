package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/mcp"
)

type MCPCaller interface {
	CallTool(ctx context.Context, server string, tool string, args json.RawMessage) (mcp.CallToolResult, error)
}

type MCPToolConfig struct {
	RegistryName string
	ServerName   string
	ToolName     string
	Caller       MCPCaller
}

func RegisterMCPTool(reg *Registry, cfg MCPToolConfig) error {
	if cfg.RegistryName == "" || cfg.ServerName == "" || cfg.ToolName == "" || cfg.Caller == nil {
		return fmt.Errorf("%w: mcp registry name, server, tool, and caller required", ErrInvalidTool)
	}
	return reg.Register(cfg.RegistryName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		result, err := cfg.Caller.CallTool(ctx, cfg.ServerName, cfg.ToolName, req.Arguments)
		if err != nil {
			return Result{}, fmt.Errorf("invoke mcp tool: %w", err)
		}
		payload, err := json.Marshal(result)
		if err != nil {
			return Result{}, fmt.Errorf("marshal mcp result: %w", err)
		}
		return NewJSONResult(req.CallID, req.Name, payload)
	}))
}
