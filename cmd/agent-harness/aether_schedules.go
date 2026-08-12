package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/authhandoff"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/channels/aether/scheduleconfig"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type scheduledTurnDeclaration = scheduleconfig.Declaration

func loadScheduledTurnDeclarations(path string) ([]scheduledTurnDeclaration, [sha256.Size]byte, error) {
	return scheduleconfig.Load(path)
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
	return scheduleconfig.Prepare(ctx, declarations, workspaceRoot, host)
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
