package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
)

type scheduledTurnConfigFile struct {
	Version   int                        `yaml:"version"`
	Schedules []scheduledTurnDeclaration `yaml:"schedules"`
}

type scheduledTurnDeclaration struct {
	ID                              string                `yaml:"id"`
	Name                            string                `yaml:"name"`
	Enabled                         *bool                 `yaml:"enabled"`
	Schedule                        scheduledTurnSchedule `yaml:"schedule"`
	ThreadID                        string                `yaml:"thread_id"`
	Prompt                          string                `yaml:"prompt"`
	RelativeDirectory               string                `yaml:"relative_directory"`
	AllowMutableView                bool                  `yaml:"allow_mutable_view"`
	AllowDirtyView                  bool                  `yaml:"allow_dirty_view"`
	OfflinePolicy                   string                `yaml:"offline_policy"`
	RequireTaskAuthority            bool                  `yaml:"require_task_authority"`
	RequiredDownstreamAuthorityHops uint32                `yaml:"required_downstream_authority_hops"`
}

type scheduledTurnSchedule struct {
	Type       string `yaml:"type"`
	Expression string `yaml:"expression"`
	MissPolicy string `yaml:"miss_policy"`
}

func loadScheduledTurnDeclarations(path string) ([]scheduledTurnDeclaration, [sha256.Size]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("read scheduled turn config: %w", err)
	}
	digest := sha256.Sum256(data)
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var config scheduledTurnConfigFile
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
	if config.Version != scheduledTurnConfigVersion {
		return nil, digest, fmt.Errorf("scheduled turn config version %d is unsupported (want %d)", config.Version, scheduledTurnConfigVersion)
	}
	return config.Schedules, digest, nil
}

func prepareScheduledTurnRegistrations(
	ctx context.Context,
	path string,
	workspaceRoot string,
	host *aetherchan.WorkerToolHost,
) ([]aetherchan.ScheduledTurnRegistration, error) {
	registrations, _, err := prepareScheduledTurnRegistrationsSnapshot(ctx, path, workspaceRoot, host)
	return registrations, err
}

func prepareScheduledTurnRegistrationsSnapshot(
	ctx context.Context,
	path string,
	workspaceRoot string,
	host *aetherchan.WorkerToolHost,
) ([]aetherchan.ScheduledTurnRegistration, [sha256.Size]byte, error) {
	declarations, digest, err := loadScheduledTurnDeclarations(path)
	if err != nil {
		return nil, digest, err
	}
	registrations, err := prepareScheduledTurnDeclarations(ctx, declarations, workspaceRoot, host)
	return registrations, digest, err
}

func prepareScheduledTurnDeclarations(
	ctx context.Context,
	declarations []scheduledTurnDeclaration,
	workspaceRoot string,
	host *aetherchan.WorkerToolHost,
) ([]aetherchan.ScheduledTurnRegistration, error) {
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
	go renewAetherWorkspaceViews(ctx, "worker", host)
	return host, nil
}

type workspaceViewRenewer interface {
	Renew(context.Context) error
}

func renewAetherWorkspaceViews(ctx context.Context, role string, host workspaceViewRenewer) {
	ticker := time.NewTicker(workspacepkg.ViewObservationRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := host.Renew(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, role+" workspace view renewal failed", slog.Any("err", err))
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
	registrations, digest, err := prepareScheduledTurnRegistrationsSnapshot(ctx, cfg.scheduleConfig, cfg.workspaceRoot, host)
	if err != nil {
		return err
	}
	if err := ch.EnableScheduledTurns(ctx, registrations, handoff, 0); err != nil {
		return fmt.Errorf("enable scheduled turns: %w", err)
	}
	go watchScheduledTurnDeclarations(ctx, ch, host, cfg, digest)
	return nil
}

type scheduledTurnUpdater interface {
	UpdateScheduledTurns(context.Context, []aetherchan.ScheduledTurnRegistration) error
}

func watchScheduledTurnDeclarations(
	ctx context.Context,
	updater scheduledTurnUpdater,
	host *aetherchan.WorkerToolHost,
	cfg appConfig,
	appliedDigest [sha256.Size]byte,
) {
	interval := cfg.scheduleReloadInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastErrorKey := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		declarations, digest, err := loadScheduledTurnDeclarations(cfg.scheduleConfig)
		if err == nil && digest == appliedDigest {
			continue
		}
		if err == nil {
			var registrations []aetherchan.ScheduledTurnRegistration
			registrations, err = prepareScheduledTurnDeclarations(ctx, declarations, cfg.workspaceRoot, host)
			if err == nil {
				err = updater.UpdateScheduledTurns(ctx, registrations)
			}
		}
		if err != nil {
			key := hex.EncodeToString(digest[:]) + ":" + err.Error()
			if key != lastErrorKey && ctx.Err() == nil {
				slog.WarnContext(ctx, "scheduled turn reload failed; reconciliation will retry", slog.Any("err", err))
			}
			lastErrorKey = key
			continue
		}
		appliedDigest = digest
		lastErrorKey = ""
		slog.InfoContext(ctx, "reloaded scheduled turn declarations", slog.Int("schedules", len(declarations)))
	}
}
