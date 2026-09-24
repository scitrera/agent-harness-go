// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

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

type MCPResourceConfig struct {
	RegistryName string
	ServerName   string
	Client       mcp.ResourceClient
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
		return resultFromMCP(req, result)
	}))
}

func RegisterMCPListResourcesTool(reg *Registry, cfg MCPResourceConfig) error {
	if err := validateMCPResourceConfig(cfg); err != nil {
		return err
	}
	return reg.Register(cfg.RegistryName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		resourceReq, err := decodeListResourcesRequest(req.Arguments)
		if err != nil {
			return Result{}, err
		}
		result, err := cfg.Client.ListResources(ctx, cfg.ServerName, resourceReq)
		if err != nil {
			return Result{}, fmt.Errorf("list mcp resources: %w", err)
		}
		payload, err := json.Marshal(result)
		if err != nil {
			return Result{}, fmt.Errorf("marshal mcp resources/list result: %w", err)
		}
		return NewJSONResult(req.CallID, req.Name, payload)
	}))
}

func RegisterMCPReadResourceTool(reg *Registry, cfg MCPResourceConfig) error {
	if err := validateMCPResourceConfig(cfg); err != nil {
		return err
	}
	return reg.Register(cfg.RegistryName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		resourceReq, err := decodeReadResourceRequest(req.Arguments)
		if err != nil {
			return Result{}, err
		}
		result, err := cfg.Client.ReadResource(ctx, cfg.ServerName, resourceReq)
		if err != nil {
			return Result{}, fmt.Errorf("read mcp resource: %w", err)
		}
		payload, err := json.Marshal(result)
		if err != nil {
			return Result{}, fmt.Errorf("marshal mcp resources/read result: %w", err)
		}
		return NewJSONResult(req.CallID, req.Name, payload)
	}))
}

func RegisterMCPListResourceTemplatesTool(reg *Registry, cfg MCPResourceConfig) error {
	if err := validateMCPResourceConfig(cfg); err != nil {
		return err
	}
	return reg.Register(cfg.RegistryName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
		templateReq, err := decodeListResourceTemplatesRequest(req.Arguments)
		if err != nil {
			return Result{}, err
		}
		result, err := cfg.Client.ListResourceTemplates(ctx, cfg.ServerName, templateReq)
		if err != nil {
			return Result{}, fmt.Errorf("list mcp resource templates: %w", err)
		}
		payload, err := json.Marshal(result)
		if err != nil {
			return Result{}, fmt.Errorf("marshal mcp resources/templates/list result: %w", err)
		}
		return NewJSONResult(req.CallID, req.Name, payload)
	}))
}

func validateMCPResourceConfig(cfg MCPResourceConfig) error {
	if cfg.RegistryName == "" || cfg.ServerName == "" || cfg.Client == nil {
		return fmt.Errorf("%w: mcp registry name, server, and resource client required", ErrInvalidTool)
	}
	return nil
}

func decodeListResourcesRequest(args json.RawMessage) (mcp.ListResourcesRequest, error) {
	var req mcp.ListResourcesRequest
	if len(args) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(args, &req); err != nil {
		return mcp.ListResourcesRequest{}, fmt.Errorf("%w: decode mcp resources/list arguments: %w", ErrInvalidArgument, err)
	}
	return req, nil
}

func decodeReadResourceRequest(args json.RawMessage) (mcp.ReadResourceRequest, error) {
	var req mcp.ReadResourceRequest
	if len(args) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(args, &req); err != nil {
		return mcp.ReadResourceRequest{}, fmt.Errorf("%w: decode mcp resources/read arguments: %w", ErrInvalidArgument, err)
	}
	return req, nil
}

func decodeListResourceTemplatesRequest(args json.RawMessage) (mcp.ListResourceTemplatesRequest, error) {
	var req mcp.ListResourceTemplatesRequest
	if len(args) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(args, &req); err != nil {
		return mcp.ListResourceTemplatesRequest{}, fmt.Errorf("%w: decode mcp resources/templates/list arguments: %w", ErrInvalidArgument, err)
	}
	return req, nil
}
