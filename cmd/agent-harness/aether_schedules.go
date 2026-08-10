package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
	"gopkg.in/yaml.v3"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	scheduledTurnConfigVersion = 1
	workerViewRenewInterval    = 2 * time.Minute
)

type scheduledTurnConfigFile struct {
	Version   int                        `yaml:"version"`
	Schedules []scheduledTurnDeclaration `yaml:"schedules"`
}

type scheduledTurnDeclaration struct {
	ID                string                `yaml:"id"`
	Name              string                `yaml:"name"`
	Enabled           *bool                 `yaml:"enabled"`
	Schedule          scheduledTurnSchedule `yaml:"schedule"`
	ThreadID          string                `yaml:"thread_id"`
	Prompt            string                `yaml:"prompt"`
	RelativeDirectory string                `yaml:"relative_directory"`
	AllowMutableView  bool                  `yaml:"allow_mutable_view"`
	AllowDirtyView    bool                  `yaml:"allow_dirty_view"`
}

type scheduledTurnSchedule struct {
	Type       string `yaml:"type"`
	Expression string `yaml:"expression"`
	MissPolicy string `yaml:"miss_policy"`
}

func loadScheduledTurnDeclarations(path string) ([]scheduledTurnDeclaration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open scheduled turn config: %w", err)
	}
	defer func() { _ = file.Close() }()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var config scheduledTurnConfigFile
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode scheduled turn config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("scheduled turn config must contain one YAML document")
		}
		return nil, fmt.Errorf("decode scheduled turn config trailer: %w", err)
	}
	if config.Version != scheduledTurnConfigVersion {
		return nil, fmt.Errorf("scheduled turn config version %d is unsupported (want %d)", config.Version, scheduledTurnConfigVersion)
	}
	if len(config.Schedules) == 0 {
		return nil, errors.New("scheduled turn config has no schedules")
	}
	return config.Schedules, nil
}

func prepareScheduledTurnRegistrations(
	ctx context.Context,
	path string,
	workspaceRoot string,
	host *aetherchan.WorkerToolHost,
) ([]aetherchan.ScheduledTurnRegistration, error) {
	declarations, err := loadScheduledTurnDeclarations(path)
	if err != nil {
		return nil, err
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
			missPolicy = "fire_once"
		}
		registration := aetherchan.ScheduledTurnRegistration{
			ID: declaration.ID, Name: name, Enabled: enabled,
			ScheduleType:       strings.ToLower(strings.TrimSpace(declaration.Schedule.Type)),
			ScheduleExpression: strings.TrimSpace(declaration.Schedule.Expression),
			MissPolicy:         missPolicy, ThreadID: threadID,
			Prompt: strings.TrimSpace(declaration.Prompt), Binding: binding,
			ViewPolicy: aetherchan.ScheduledViewPolicy{
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

func configureAetherWorkerViews(
	ctx context.Context,
	ch *aetherchan.Channel,
	cfg appConfig,
	publisher workspacepkg.ViewPublisher,
) (*aetherchan.WorkerToolHost, error) {
	if publisher == nil {
		return nil, nil
	}
	host, err := aetherchan.NewWorkerToolHost(ctx, aetherchan.WorkerToolHostConfig{
		WorkspaceID: effectiveWorkspace("", cfg.workspaceID), WorkspaceRoot: cfg.workspaceRoot,
		StateDir: cfg.workspaceIndexDir, ToolHostID: ch.Topic(), Publisher: publisher,
	})
	if err != nil {
		return nil, fmt.Errorf("worker workspace view: %w", err)
	}
	ch.SetWorkerToolHost(host)
	go renewAetherWorkerViews(ctx, host)
	return host, nil
}

func renewAetherWorkerViews(ctx context.Context, host *aetherchan.WorkerToolHost) {
	ticker := time.NewTicker(workerViewRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := host.Renew(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "worker workspace view renewal failed", slog.Any("err", err))
			}
		}
	}
}

func enableAetherScheduledTurns(
	ctx context.Context,
	ch *aetherchan.Channel,
	host *aetherchan.WorkerToolHost,
	handoff *authhandoff.Store,
	cfg appConfig,
) error {
	if strings.TrimSpace(cfg.scheduleConfig) == "" {
		return nil
	}
	if host == nil {
		return errors.New("scheduled turns require MemoryLayer-backed worker workspace views")
	}
	registrations, err := prepareScheduledTurnRegistrations(ctx, cfg.scheduleConfig, cfg.workspaceRoot, host)
	if err != nil {
		return err
	}
	if err := ch.EnableScheduledTurns(ctx, registrations, handoff, 0); err != nil {
		return fmt.Errorf("enable scheduled turns: %w", err)
	}
	return nil
}
