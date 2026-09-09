// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package plugin defines the transport-neutral admission contract for Sahara
// extensions. It does not load or execute code; WASM and MCP runtimes consume an
// admitted Manifest only after operator policy has bounded its capabilities.
package plugin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	ManifestSchemaVersion = 1
	MaxManifestBytes      = 256 << 10
	CompatibilityWindow   = 2 // supported minor releases after deprecation
)

// Capability is an operator-grantable execution capability. Values are
// deliberately strings so additive capability names do not require a manifest
// schema bump; an unknown capability remains denied unless policy grants it.
type Capability string

const (
	CapabilityFilesystemRead  Capability = "filesystem.read"
	CapabilityFilesystemWrite Capability = "filesystem.write"
	CapabilityNetwork         Capability = "network"
	CapabilityToolCall        Capability = "tool.call"
)

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

// Manifest is side-effect-free discovery metadata. Unknown JSON fields are
// ignored for additive compatibility. Privileged flags are represented only so
// admission can explicitly reject a plugin attempting to self-elevate.
type Manifest struct {
	SchemaVersion         int          `json:"schema_version"`
	ID                    string       `json:"id"`
	Revision              string       `json:"revision"`
	ArtifactDigest        string       `json:"artifact_digest"`
	RequestedCapabilities []Capability `json:"requested_capabilities,omitempty"`
	Tools                 []Tool       `json:"tools,omitempty"`
	Preauthorized         bool         `json:"preauthorized,omitempty"`
	OverrideCoreTools     bool         `json:"override_core_tools,omitempty"`
}

// Policy is the deployment/Aether ceiling. It is external to the plugin
// artifact, so embedded metadata cannot broaden it.
type Policy struct {
	ArtifactDigest      string
	GrantedCapabilities map[Capability]struct{}
	CoreToolNames       map[string]struct{}
}

// Admission is the immutable result passed to a concrete extension runtime.
type Admission struct {
	Manifest            Manifest
	GrantedCapabilities []Capability
}

// DecodeManifest performs lazy, side-effect-free discovery. Unknown fields are
// intentionally ignored; required fields and security invariants are validated
// by Admit against operator policy.
func DecodeManifest(data []byte) (Manifest, error) {
	if len(data) == 0 || len(data) > MaxManifestBytes {
		return Manifest{}, errors.New("plugin: manifest size is invalid")
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("plugin: decode manifest: %w", err)
	}
	return manifest, nil
}

// Admit verifies artifact identity, namespace and tool collisions, and the
// separation between requested and operator-granted capabilities.
func Admit(manifest Manifest, policy Policy) (Admission, error) {
	if manifest.SchemaVersion != ManifestSchemaVersion {
		return Admission{}, fmt.Errorf("plugin: unsupported manifest schema version %d", manifest.SchemaVersion)
	}
	if err := validateID(manifest.ID); err != nil {
		return Admission{}, err
	}
	if strings.TrimSpace(manifest.Revision) == "" || manifest.Revision != strings.TrimSpace(manifest.Revision) {
		return Admission{}, errors.New("plugin: revision is required and must be trimmed")
	}
	if err := validateDigest(manifest.ArtifactDigest); err != nil {
		return Admission{}, fmt.Errorf("plugin: invalid artifact digest: %w", err)
	}
	if manifest.ArtifactDigest != policy.ArtifactDigest {
		return Admission{}, errors.New("plugin: artifact digest does not match deployment admission")
	}
	if manifest.Preauthorized {
		return Admission{}, errors.New("plugin: manifest cannot self-preauthorize")
	}
	if manifest.OverrideCoreTools {
		return Admission{}, errors.New("plugin: manifest cannot override core tools")
	}

	seenCapabilities := map[Capability]struct{}{}
	granted := make([]Capability, 0, len(manifest.RequestedCapabilities))
	for _, capability := range manifest.RequestedCapabilities {
		if strings.TrimSpace(string(capability)) == "" || capability != Capability(strings.TrimSpace(string(capability))) {
			return Admission{}, errors.New("plugin: requested capability is invalid")
		}
		if _, duplicate := seenCapabilities[capability]; duplicate {
			continue
		}
		seenCapabilities[capability] = struct{}{}
		if _, ok := policy.GrantedCapabilities[capability]; !ok {
			return Admission{}, fmt.Errorf("plugin: capability %q is not granted by operator policy", capability)
		}
		granted = append(granted, capability)
	}

	seenTools := map[string]struct{}{}
	for _, tool := range manifest.Tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" || name != tool.Name {
			return Admission{}, errors.New("plugin: tool name is required and must be trimmed")
		}
		if _, core := policy.CoreToolNames[name]; core {
			return Admission{}, fmt.Errorf("plugin: tool %q collides with a core tool", name)
		}
		if _, duplicate := seenTools[name]; duplicate {
			return Admission{}, fmt.Errorf("plugin: duplicate tool %q", name)
		}
		seenTools[name] = struct{}{}
		if len(tool.InputSchema) > 0 && !json.Valid(tool.InputSchema) {
			return Admission{}, fmt.Errorf("plugin: tool %q has invalid input schema", name)
		}
	}
	return Admission{Manifest: cloneManifest(manifest), GrantedCapabilities: append([]Capability(nil), granted...)}, nil
}

// Registry rejects plugin identity and tool collisions across an activated set.
// It stores only already-admitted manifests and never instantiates code.
type Registry struct {
	plugins map[string]Admission
	tools   map[string]string
}

func NewRegistry() *Registry {
	return &Registry{plugins: map[string]Admission{}, tools: map[string]string{}}
}

func (r *Registry) Register(admission Admission) error {
	if r == nil {
		return errors.New("plugin: registry is nil")
	}
	id := admission.Manifest.ID
	if existing, ok := r.plugins[id]; ok {
		if existing.Manifest.Revision == admission.Manifest.Revision && existing.Manifest.ArtifactDigest == admission.Manifest.ArtifactDigest {
			return nil
		}
		return fmt.Errorf("plugin: identity %q already has an active revision", id)
	}
	for _, tool := range admission.Manifest.Tools {
		if owner, exists := r.tools[tool.Name]; exists {
			return fmt.Errorf("plugin: tool %q already belongs to %q", tool.Name, owner)
		}
	}
	r.plugins[id] = admission
	for _, tool := range admission.Manifest.Tools {
		r.tools[tool.Name] = id
	}
	return nil
}

func validateID(value string) error {
	if value == "" || value != strings.TrimSpace(value) || (!strings.Contains(value, ".") && !strings.Contains(value, "/")) {
		return errors.New("plugin: id must be a trimmed namespace-qualified name")
	}
	if strings.Contains(value, "..") || strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") {
		return errors.New("plugin: id contains an invalid namespace path")
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' || r == '/' {
			continue
		}
		return errors.New("plugin: id contains unsupported characters")
	}
	return nil
}

func validateDigest(value string) error {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return errors.New("expected a sha256 digest")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
		return errors.New("expected a lowercase sha256 digest")
	}
	return nil
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.RequestedCapabilities = append([]Capability(nil), manifest.RequestedCapabilities...)
	manifest.Tools = append([]Tool(nil), manifest.Tools...)
	for index := range manifest.Tools {
		manifest.Tools[index].InputSchema = append(json.RawMessage(nil), manifest.Tools[index].InputSchema...)
	}
	return manifest
}
