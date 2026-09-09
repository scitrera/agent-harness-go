// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Package scheduleconfig owns the strict, credential-free YAML declaration
// shape shared by OSS and embedded Sahara distributions.
package scheduleconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
	"gopkg.in/yaml.v3"
)

const Version = 1

type File struct {
	Version   int           `yaml:"version"`
	Schedules []Declaration `yaml:"schedules"`
}

type Declaration struct {
	ID                              string   `yaml:"id"`
	Name                            string   `yaml:"name"`
	Enabled                         *bool    `yaml:"enabled"`
	Schedule                        Schedule `yaml:"schedule"`
	ThreadID                        string   `yaml:"thread_id"`
	Prompt                          string   `yaml:"prompt"`
	RelativeDirectory               string   `yaml:"relative_directory"`
	AllowMutableView                bool     `yaml:"allow_mutable_view"`
	AllowDirtyView                  bool     `yaml:"allow_dirty_view"`
	OfflinePolicy                   string   `yaml:"offline_policy"`
	RequireTaskAuthority            bool     `yaml:"require_task_authority"`
	RequiredDownstreamAuthorityHops uint32   `yaml:"required_downstream_authority_hops"`
}

type Schedule struct {
	Type       string `yaml:"type"`
	Expression string `yaml:"expression"`
	MissPolicy string `yaml:"miss_policy"`
}

func Load(path string) ([]Declaration, [sha256.Size]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("read scheduled turn config: %w", err)
	}
	digest := sha256.Sum256(data)
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var config File
	if err := decoder.Decode(&config); err != nil {
		return nil, digest, fmt.Errorf("decode scheduled turn config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, digest, errors.New("scheduled turn config must contain one YAML document")
		}
		return nil, digest, fmt.Errorf("decode scheduled turn config trailer: %w", err)
	}
	if config.Version != Version {
		return nil, digest, fmt.Errorf("scheduled turn config version %d is unsupported (want %d)", config.Version, Version)
	}
	return config.Schedules, digest, nil
}

func LoadRegistrations(
	ctx context.Context,
	path string,
	workspaceRoot string,
	host *aetherchan.WorkerToolHost,
) ([]aetherchan.ScheduledTurnRegistration, [sha256.Size]byte, error) {
	declarations, digest, err := Load(path)
	if err != nil {
		return nil, digest, err
	}
	registrations, err := Prepare(ctx, declarations, workspaceRoot, host)
	return registrations, digest, err
}

func Prepare(
	ctx context.Context,
	declarations []Declaration,
	workspaceRoot string,
	host *aetherchan.WorkerToolHost,
) ([]aetherchan.ScheduledTurnRegistration, error) {
	if host == nil {
		return nil, errors.New("scheduled turns require a worker tool host")
	}
	seen := map[string]struct{}{}
	registrations := make([]aetherchan.ScheduledTurnRegistration, 0, len(declarations))
	for _, declaration := range declarations {
		declaration.ID = strings.TrimSpace(declaration.ID)
		if declaration.ID == "" {
			return nil, errors.New("scheduled turn declaration id is required")
		}
		if _, exists := seen[declaration.ID]; exists {
			return nil, fmt.Errorf("duplicate scheduled turn id %q", declaration.ID)
		}
		seen[declaration.ID] = struct{}{}
		enabled := true
		if declaration.Enabled != nil {
			enabled = *declaration.Enabled
		}
		name := strings.TrimSpace(declaration.Name)
		if name == "" {
			name = declaration.ID
		}
		if !enabled {
			registrations = append(registrations, aetherchan.ScheduledTurnRegistration{
				ID: declaration.ID, Name: name, Enabled: false,
			})
			continue
		}
		relative, err := spec.NormalizeWorkspaceRelativeDirectory(declaration.RelativeDirectory)
		if err != nil {
			return nil, fmt.Errorf("scheduled turn %q relative directory: %w", declaration.ID, err)
		}
		target := workspaceRoot
		if relative != "" {
			target = filepath.Join(workspaceRoot, filepath.FromSlash(relative))
		}
		binding, err := host.ScheduledExecutionBindingForDirectory(
			ctx, target, declaration.AllowMutableView, declaration.AllowDirtyView,
		)
		if err != nil {
			return nil, fmt.Errorf("scheduled turn %q workspace view: %w", declaration.ID, err)
		}
		threadID := strings.TrimSpace(declaration.ThreadID)
		if threadID == "" {
			threadID = "scheduled-" + declaration.ID
		}
		missPolicy := strings.ToLower(strings.TrimSpace(declaration.Schedule.MissPolicy))
		if missPolicy == "" {
			missPolicy = aetherchan.ScheduledMissPolicyFireOnce
		}
		offlinePolicy := strings.ToLower(strings.TrimSpace(declaration.OfflinePolicy))
		if offlinePolicy == "" {
			offlinePolicy = "queue"
		}
		writeAccess := workspacepkg.ViewWriteAccessReadOnly
		if declaration.AllowDirtyView {
			writeAccess = workspacepkg.ViewWriteAccessReadWrite
		}
		registration := aetherchan.ScheduledTurnRegistration{
			ID: declaration.ID, Name: name, Enabled: enabled,
			ScheduleType:       strings.ToLower(strings.TrimSpace(declaration.Schedule.Type)),
			ScheduleExpression: strings.TrimSpace(declaration.Schedule.Expression),
			MissPolicy:         missPolicy, ThreadID: threadID,
			TargetOfflinePolicy:             offlinePolicy,
			RequireTaskAuthority:            declaration.RequireTaskAuthority,
			RequiredDownstreamAuthorityHops: declaration.RequiredDownstreamAuthorityHops,
			Prompt:                          strings.TrimSpace(declaration.Prompt), Binding: binding,
			ViewPolicy: aetherchan.ScheduledViewPolicy{
				WriteAccess:      writeAccess,
				AllowMutableView: declaration.AllowMutableView,
				AllowDirtyView:   declaration.AllowDirtyView,
			},
		}
		if err := registration.Validate(); err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	return registrations, nil
}
