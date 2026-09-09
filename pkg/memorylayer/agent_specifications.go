// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package memorylayer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
)

const (
	agentSpecificationPageSize       = 100
	maxAgentSpecificationPages       = 5
	maxEnabledAgentSpecifications    = 128
	maxAgentSpecificationPromptBytes = 64 << 10
)

// AgentSpecificationProvider maps MemoryLayer's typed current heads to the OSS
// subagent definition contract. MemoryLayer remains authoritative: this adapter
// neither caches nor falls back to a filesystem catalog.
type AgentSpecificationProvider struct {
	client *memorylayersdk.Client
}

type agentSpecificationProviderConfig struct {
	transport memorylayersdk.Transport
}

type AgentSpecificationProviderOption func(*agentSpecificationProviderConfig)

// WithAgentSpecificationTransport routes requests over a MemoryLayer SDK
// transport such as the Aether proxy transport.
func WithAgentSpecificationTransport(transport memorylayersdk.Transport) AgentSpecificationProviderOption {
	return func(config *agentSpecificationProviderConfig) { config.transport = transport }
}

func NewAgentSpecificationProvider(cfg Config, providerOptions ...AgentSpecificationProviderOption) (*AgentSpecificationProvider, error) {
	providerConfig := agentSpecificationProviderConfig{}
	for _, option := range providerOptions {
		if option != nil {
			option(&providerConfig)
		}
	}
	transport := providerConfig.transport
	if transport == nil {
		transport = cfg.Transport
	}
	opts := []memorylayersdk.Option{
		memorylayersdk.WithAPIKey(cfg.APIKey),
		memorylayersdk.WithWorkspaceID(cfg.Workspace),
	}
	if transport != nil {
		opts = append(opts, memorylayersdk.WithTransport(transport))
	} else {
		if cfg.BaseURL == "" {
			return nil, fmt.Errorf("memorylayer: agent specifications base url or transport required")
		}
		opts = append(opts, memorylayersdk.WithBaseURL(cfg.BaseURL))
	}
	if cfg.HTTPClient != nil && transport == nil {
		opts = append(opts, memorylayersdk.WithHTTPClient(cfg.HTTPClient))
	}
	client, err := memorylayersdk.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("memorylayer: agent specifications client: %w", err)
	}
	return &AgentSpecificationProvider{client: client}, nil
}

func (p *AgentSpecificationProvider) LoadWorkspace(ctx context.Context, workspaceID string) ([]subagent.Definition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	authority := promptNoteAuthority(ctx)
	pageToken := ""
	seenTokens := map[string]struct{}{}
	seenTypes := map[subagent.AgentType]struct{}{}
	var definitions []subagent.Definition
	for pageNumber := 0; pageNumber < maxAgentSpecificationPages; pageNumber++ {
		page, err := p.client.AgentSpecifications.List(ctx, memorylayersdk.AgentSpecificationListOptions{
			WorkspaceID: workspaceID,
			Limit:       agentSpecificationPageSize,
			PageToken:   pageToken,
			Authority:   authority,
		})
		if err != nil {
			return nil, fmt.Errorf("memorylayer: list agent specifications: %w", err)
		}
		for _, specification := range page.Specifications {
			if !specification.Enabled {
				continue
			}
			if specification.SchemaVersion != 1 {
				return nil, fmt.Errorf("memorylayer: agent specification %q uses unsupported schema version %d", specification.Key, specification.SchemaVersion)
			}
			if len(specification.Instructions) > maxAgentSpecificationPromptBytes {
				return nil, fmt.Errorf("memorylayer: agent specification %q exceeds %d instruction bytes", specification.Key, maxAgentSpecificationPromptBytes)
			}
			definition := agentSpecificationDefinition(specification)
			if err := definition.Validate(); err != nil {
				return nil, fmt.Errorf("memorylayer: agent specification %q: %w", specification.Key, err)
			}
			if _, duplicate := seenTypes[definition.Type]; duplicate {
				return nil, fmt.Errorf("memorylayer: duplicate enabled agent specification key %q", definition.Type)
			}
			if len(definitions) == maxEnabledAgentSpecifications {
				return nil, fmt.Errorf("memorylayer: more than %d enabled agent specifications", maxEnabledAgentSpecifications)
			}
			seenTypes[definition.Type] = struct{}{}
			definitions = append(definitions, definition)
		}
		if page.NextPageToken == "" {
			sort.Slice(definitions, func(i, j int) bool { return definitions[i].Type < definitions[j].Type })
			return definitions, nil
		}
		if _, repeated := seenTokens[page.NextPageToken]; repeated {
			return nil, fmt.Errorf("memorylayer: agent-specification cursor cycle at %q", page.NextPageToken)
		}
		seenTokens[page.NextPageToken] = struct{}{}
		pageToken = page.NextPageToken
	}
	return nil, fmt.Errorf("memorylayer: agent-specification listing exceeds %d pages (%d heads maximum)", maxAgentSpecificationPages, agentSpecificationPageSize*maxAgentSpecificationPages)
}

func agentSpecificationDefinition(specification memorylayersdk.AgentSpecification) subagent.Definition {
	description := strings.TrimSpace(specification.Description)
	if guidance := strings.TrimSpace(specification.InvocationGuidance); guidance != "" {
		description += "\nWhen to use: " + guidance
	}
	return subagent.Definition{
		Name:           subagent.AgentName(specification.Name),
		Type:           subagent.AgentType(specification.Key),
		Description:    description,
		Prompt:         specification.Instructions,
		Model:          specification.Model,
		MaxTurns:       specification.MaxTurns,
		AllowedTools:   append([]string(nil), specification.AllowedTools...),
		DeniedTools:    append([]string(nil), specification.DeniedTools...),
		Skills:         append([]string(nil), specification.Skills...),
		MCPServers:     append([]string(nil), specification.MCPServers...),
		PermissionMode: subagent.PermissionMode(specification.PermissionMode),
		ExecPolicyHint: specification.ExecPolicyHint,
		Background:     specification.Background,
	}
}

var _ subagent.WorkspaceDefinitionProvider = (*AgentSpecificationProvider)(nil)
