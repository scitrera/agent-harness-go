// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package requirements

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/scitrera/agent-harness-go/pkg/mcp"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func Compose(layers ...Layer) (Requirements, error) {
	state := newMergeState()
	for i, layer := range layers {
		if err := state.addLayer(layer, i); err != nil {
			return Requirements{}, err
		}
	}
	return state.output(), nil
}

type mergeState struct {
	approval               *Sourced[ApprovalRequirement]
	defaultDecision        *Sourced[tools.DecisionCode]
	resolveHostExecutables *Sourced[bool]
	rules                  []Sourced[tools.CommandRule]
	ruleByID               map[string]Sourced[tools.CommandRule]
	hosts                  []Sourced[tools.HostExecutable]
	hostByName             map[string]Sourced[tools.HostExecutable]
	hookConfig             *Sourced[HookRequirement]
	hooks                  []Sourced[hookEntry]
	hookByKey              map[string]Sourced[hookEntry]
	mcpServers             map[string]Sourced[mcp.ServerConfig]
	catalogs               map[string]Sourced[CatalogSource]
	models                 map[string]Sourced[ModelChoice]
	fileStores             map[string]Sourced[FileStorePath]
	features               map[string]Sourced[FeatureGate]
}

func newMergeState() *mergeState {
	return &mergeState{
		ruleByID:   map[string]Sourced[tools.CommandRule]{},
		hostByName: map[string]Sourced[tools.HostExecutable]{},
		hookByKey:  map[string]Sourced[hookEntry]{},
		mcpServers: map[string]Sourced[mcp.ServerConfig]{},
		catalogs:   map[string]Sourced[CatalogSource]{},
		models:     map[string]Sourced[ModelChoice]{},
		fileStores: map[string]Sourced[FileStorePath]{},
		features:   map[string]Sourced[FeatureGate]{},
	}
}

func (s *mergeState) addLayer(layer Layer, index int) error {
	if !layer.Source.valid() {
		return fmt.Errorf("%w: layer %d source label required", ErrInvalidLayer, index)
	}
	if err := s.mergeApproval(layer); err != nil {
		return err
	}
	if err := s.mergeToolPolicy(layer); err != nil {
		return err
	}
	if err := s.mergeHooks(layer); err != nil {
		return err
	}
	if err := mergeNamed(layer.Source, "mcp_servers", layer.MCPServers, serverName, validateMCPServer, &s.mcpServers); err != nil {
		return err
	}
	if err := mergeNamed(layer.Source, "catalogs", layer.Catalogs, catalogName, validateCatalogSource, &s.catalogs); err != nil {
		return err
	}
	if err := mergeNamed(layer.Source, "models", layer.Models, modelRole, validateModelChoice, &s.models); err != nil {
		return err
	}
	if err := mergeNamed(layer.Source, "file_stores", layer.FileStores, fileStoreName, validateFileStorePath, &s.fileStores); err != nil {
		return err
	}
	return mergeNamed(layer.Source, "feature_gates", layer.FeatureGates, featureName, validateFeatureGate, &s.features)
}

func (s *mergeState) output() Requirements {
	sort.SliceStable(s.hosts, func(i, j int) bool { return s.hosts[i].Value.Name < s.hosts[j].Value.Name })
	return Requirements{
		Approval: s.approval,
		ToolPolicy: SourcedToolPolicy{
			DefaultDecision:        s.defaultDecision,
			ResolveHostExecutables: s.resolveHostExecutables,
			Rules:                  sourceRules(s.rules),
			HostExecutables:        s.hosts,
		},
		Hooks: SourcedHooks{
			Config: sourceHookConfig(s.hookConfig),
			Hooks:  sourceHooks(s.hooks),
		},
		MCPServers:   s.mcpServers,
		Catalogs:     s.catalogs,
		Models:       s.models,
		FileStores:   s.fileStores,
		FeatureGates: s.features,
	}
}

func equal[T any](a T, b T) bool {
	return reflect.DeepEqual(a, b)
}
