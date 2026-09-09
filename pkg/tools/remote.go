// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package tools

import (
	"context"
	"fmt"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

var ErrToolRequiresApproval = fmt.Errorf("tools: approval required")

type RemoteInvoker interface {
	InvokeTool(ctx context.Context, envelope protocol.ToolInvokeEnvelope) (Result, error)
}

type AuditSink interface {
	Record(ctx context.Context, record AuditRecord) error
}

type RemoteConfig struct {
	Invoker RemoteInvoker
	Policy  Policy
	Audit   AuditSink
}

func RegisterRemote(reg *Registry, names []string, cfg RemoteConfig) error {
	if cfg.Invoker == nil {
		return fmt.Errorf("%w: remote invoker required", ErrInvalidTool)
	}
	if cfg.Policy == nil {
		cfg.Policy = StaticPolicy{}
	}
	for _, name := range names {

		toolName := name
		if err := reg.Register(toolName, HandlerFunc(func(ctx context.Context, req Request) (Result, error) {
			return invokeRemote(ctx, cfg, req)
		})); err != nil {
			return err
		}
	}
	return nil
}

func invokeRemote(ctx context.Context, cfg RemoteConfig, req Request) (Result, error) {
	decision := cfg.Policy.Decide(req)
	if cfg.Audit != nil {
		if err := cfg.Audit.Record(ctx, NewAuditRecord(req, decision)); err != nil {
			return Result{}, fmt.Errorf("record tool audit: %w", err)
		}
	}
	if !decision.Allowed() {
		return Result{}, policyError(decision, req.Name)
	}
	result, err := cfg.Invoker.InvokeTool(ctx, protocol.ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion,
		CallID:        req.CallID,
		Name:          req.Name,
		Args:          protocol.RawToArgs(req.Arguments),
		Addr:          req.Addr,
	})
	if err != nil {
		return Result{}, fmt.Errorf("invoke remote tool: %w", err)
	}
	return result, nil
}
