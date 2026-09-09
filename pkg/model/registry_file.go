// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package model

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ErrInvalidRegistryFile is the sentinel wrapping every malformed-models.yaml
// error, so a distribution can errors.Is against it (and wrap it in its own
// config error) rather than string-matching.
var ErrInvalidRegistryFile = errors.New("model: invalid registry file")

// registryFile is the on-disk schema of a models.yaml. It is a pure parse target:
// no env-var resolution or default-filling happens here (that is the resolver's
// job) so the loaded Registry mirrors the file exactly.
type registryFile struct {
	Default   string `yaml:"default"`
	Providers []struct {
		Name        string `yaml:"name"`
		Kind        string `yaml:"kind"`
		BaseURL     string `yaml:"base_url"`
		APIKey      string `yaml:"api_key"`
		APIKeyEnv   string `yaml:"api_key_env"`
		AuthProfile string `yaml:"auth_profile"`
		Format      string `yaml:"format"`
	} `yaml:"providers"`
	Models []struct {
		Name      string `yaml:"name"`
		Tier      string `yaml:"tier"`
		Provider  string `yaml:"provider"`
		Context   int    `yaml:"context"`
		Reasoning struct {
			DefaultEffort  string   `yaml:"default_effort"`
			AllowedEfforts []string `yaml:"allowed_efforts"`
		} `yaml:"reasoning"`
		Capabilities struct {
			Vision bool `yaml:"vision"`
			Tools  bool `yaml:"tools"`
			Audio  bool `yaml:"audio"`
		} `yaml:"capabilities"`
	} `yaml:"models"`
}

// LoadRegistry reads a models.yaml at path into a *Registry. A missing file
// returns (nil, nil): no registry means the consumer keeps its single configured
// model + provider. A present file that is malformed, or lists no models, is an
// error (wrapping ErrInvalidRegistryFile). Env vars are NOT resolved and defaults
// are NOT filled here — the file is parsed verbatim; resolution happens later in
// the per-model provider resolver.
func LoadRegistry(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: read %s: %w", ErrInvalidRegistryFile, path, err)
	}
	var rf registryFile
	if err := yaml.Unmarshal(data, &rf); err != nil {
		return nil, fmt.Errorf("%w: parse %s: %w", ErrInvalidRegistryFile, path, err)
	}
	if len(rf.Models) == 0 {
		return nil, fmt.Errorf("%w: %s lists no models", ErrInvalidRegistryFile, path)
	}
	models := make([]Model, 0, len(rf.Models))
	for i, m := range rf.Models {
		if m.Name == "" {
			return nil, fmt.Errorf("%w: %s model[%d] has no name", ErrInvalidRegistryFile, path, i)
		}
		reasoning, err := normalizeReasoningConfig(ReasoningConfig{
			DefaultEffort: m.Reasoning.DefaultEffort, AllowedEfforts: m.Reasoning.AllowedEfforts,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: %s model[%d] reasoning: %w", ErrInvalidRegistryFile, path, i, err)
		}
		models = append(models, Model{
			Name:      m.Name,
			Tier:      m.Tier,
			Provider:  m.Provider,
			Context:   m.Context,
			Reasoning: reasoning,
			Capabilities: Capabilities{
				Vision: m.Capabilities.Vision,
				Tools:  m.Capabilities.Tools,
				Audio:  m.Capabilities.Audio,
			},
		})
	}
	providers := make([]ProviderConfig, 0, len(rf.Providers))
	for i, p := range rf.Providers {
		if p.Name == "" {
			return nil, fmt.Errorf("%w: %s provider[%d] has no name", ErrInvalidRegistryFile, path, i)
		}
		providers = append(providers, ProviderConfig{
			Name:        p.Name,
			Kind:        p.Kind,
			BaseURL:     p.BaseURL,
			APIKey:      p.APIKey,
			APIKeyEnv:   p.APIKeyEnv,
			AuthProfile: p.AuthProfile,
			Format:      p.Format,
		})
	}
	return NewRegistryWithProviders(models, rf.Default, providers), nil
}
